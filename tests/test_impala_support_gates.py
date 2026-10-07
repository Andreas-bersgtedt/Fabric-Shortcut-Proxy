"""Opt-in live gates for promoting Apache Impala to supported."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
import base64
import hashlib
import io
import json
import os
import re
import threading
import time
from typing import Iterator
import urllib.error
import urllib.request

import pytest
import httpx
import pyarrow.parquet as pq
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

import fabric_shortcut_proxy.cache.lru_cache as parquet_cache
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.config import ColumnDef, TableDef
from fabric_shortcut_proxy.db import executor
from fabric_shortcut_proxy.db.reflect import build_url
from fabric_shortcut_proxy.db.read_points import BestEffortReadSession
from fabric_shortcut_proxy.iceberg import freshness
import fabric_shortcut_proxy.iceberg.state_store as state_store


_SUPPORT_GATE_FLAG = "FSP_RUN_IMPALA_SUPPORT_GATES"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class ImpalaSupportSettings:
    host: str
    port: int
    database: str
    username: str
    password: str | None
    query: dict[str, str]
    type_table: str
    scale_table: str
    refresh_table: str
    coordinators: tuple[str, ...]


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required when {_SUPPORT_GATE_FLAG}=1")
    return value


def _qualified_table(value: str) -> tuple[str, str]:
    parts = [part.strip() for part in value.split(".")]
    if len(parts) != 2 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
        raise ValueError(f"table must use schema.table form, got {value!r}")
    return parts[0], parts[1]


def _query_options() -> dict[str, str]:
    raw = os.environ.get("IMPALA_QUERY_JSON", "").strip()
    if not raw:
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str)
        for key, item in value.items()
    ):
        raise ValueError("IMPALA_QUERY_JSON must be a JSON object of string values")
    return value


def _support_settings() -> ImpalaSupportSettings:
    if os.environ.get(_SUPPORT_GATE_FLAG) != "1":
        pytest.skip(f"set {_SUPPORT_GATE_FLAG}=1 to run live Impala support gates")

    host = _required_environment("IMPALA_HOST")
    coordinators = tuple(
        item.strip()
        for item in os.environ.get("IMPALA_COORDINATORS", host).split(",")
        if item.strip()
    )
    if not coordinators:
        raise ValueError("IMPALA_COORDINATORS must contain at least one host")

    return ImpalaSupportSettings(
        host=host,
        port=int(os.environ.get("IMPALA_PORT", "21050")),
        database=_required_environment("IMPALA_DATABASE"),
        username=_required_environment("IMPALA_USERNAME"),
        password=os.environ.get("IMPALA_PASSWORD") or None,
        query=_query_options(),
        type_table=os.environ.get(
            "INTEGRATION_IMPALA_TYPE_TABLE",
            "fsp_impala_cert.type_matrix",
        ),
        scale_table=os.environ.get(
            "INTEGRATION_IMPALA_SCALE_TABLE",
            "fsp_impala_cert.large_skew",
        ),
        refresh_table=os.environ.get(
            "INTEGRATION_IMPALA_REFRESH_TABLE",
            "fsp_impala_cert.refresh_rows",
        ),
        coordinators=coordinators,
    )


def _control_api(
    method: str,
    path: str,
    payload: dict | None = None,
) -> dict:
    root = _required_environment("IMPALA_CONTROL_URL").rstrip("/")
    username = _required_environment("IMPALA_CONTROL_USERNAME")
    password = _required_environment("IMPALA_CONTROL_PASSWORD")
    authorization = base64.b64encode(
        f"{username}:{password}".encode()
    ).decode()
    request = urllib.request.Request(
        f"{root}/{path.lstrip('/')}",
        data=None if payload is None else json.dumps(payload).encode(),
        method=method,
        headers={
            "Authorization": f"Basic {authorization}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        body = error.read().decode(errors="replace")
        raise RuntimeError(
            f"control request {method} {path} returned HTTP {error.code}: {body}"
        ) from error
    return json.loads(body) if body else {}


def _wait_for_role(role_name: str, expected: str, timeout: int = 600) -> dict:
    cluster = _required_environment("IMPALA_CONTROL_CLUSTER")
    service = os.environ.get("IMPALA_CONTROL_SERVICE", "impala")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        role = _control_api(
            "GET",
            f"clusters/{cluster}/services/{service}/roles/{role_name}"
            "?view=summary",
        )
        if role.get("roleState") == expected:
            return role
        time.sleep(5)
    raise TimeoutError(f"role did not reach {expected} within {timeout} seconds")


def _coordinator_role() -> str:
    cluster = _required_environment("IMPALA_CONTROL_CLUSTER")
    service = os.environ.get("IMPALA_CONTROL_SERVICE", "impala")
    hostname = _required_environment("IMPALA_CONTROL_COORDINATOR")
    roles = _control_api(
        "GET",
        f"clusters/{cluster}/services/{service}/roles?view=summary",
    )
    matches = [
        role["name"]
        for role in roles.get("items", [])
        if role.get("type") == "IMPALAD"
        and role.get("hostRef", {}).get("hostname") == hostname
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"expected one IMPALAD role for the configured coordinator, got {len(matches)}"
        )
    return matches[0]


def _role_command(command: str, role_name: str) -> None:
    cluster = _required_environment("IMPALA_CONTROL_CLUSTER")
    service = os.environ.get("IMPALA_CONTROL_SERVICE", "impala")
    _control_api(
        "POST",
        f"clusters/{cluster}/services/{service}/roleCommands/{command}",
        {"items": [role_name]},
    )


def _engine(settings: ImpalaSupportSettings, host: str | None = None) -> Engine:
    return create_engine(
        build_url(
            dialect="impala",
            host=host or settings.host,
            port=settings.port,
            database=settings.database,
            username=settings.username,
            password=settings.password,
            query=settings.query,
        ),
        pool_pre_ping=True,
    )


@contextmanager
def _connection(
    settings: ImpalaSupportSettings,
    host: str | None = None,
) -> Iterator[Connection]:
    engine = _engine(settings, host)
    try:
        with engine.connect() as connection:
            yield connection
    finally:
        engine.dispose()


def test_impala_support_gate_configuration_requires_explicit_identity(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_SUPPORT_GATE_FLAG, "1")
    monkeypatch.setenv("IMPALA_HOST", "coordinator.example.test")
    monkeypatch.setenv("IMPALA_DATABASE", "certification")
    monkeypatch.delenv("IMPALA_USERNAME", raising=False)

    with pytest.raises(ValueError, match="IMPALA_USERNAME is required"):
        _support_settings()


def test_impala_support_gate_query_options_are_string_only(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("IMPALA_QUERY_JSON", '{"auth_mechanism":"PLAIN","use_ssl":true}')

    with pytest.raises(ValueError, match="JSON object of string values"):
        _query_options()


@pytest.mark.parametrize(
    "value",
    (
        "missing_schema",
        "schema.table.extra",
        "schema.table;drop_table",
        "schema.table-name",
    ),
)
def test_impala_support_gate_rejects_unsafe_table_names(value: str):
    with pytest.raises(ValueError, match="schema.table"):
        _qualified_table(value)


def test_impala_support_type_matrix_live_gate():
    settings = _support_settings()
    schema, table = _qualified_table(settings.type_table)
    engine = _engine(settings)
    try:
        columns = {
            column["name"].casefold(): str(column["type"]).casefold()
            for column in inspect(engine).get_columns(table, schema=schema)
        }
        assert {
            "id",
            "nullable_split_key",
            "event_ts",
            "amount",
            "ratio",
            "active",
            "label",
        } <= columns.keys()
        assert "bigint" in columns["nullable_split_key"]
        assert "decimal" in columns["amount"]
        assert "timestamp" in columns["event_ts"]

        with engine.connect() as connection:
            rows = connection.execute(
                text(f"SELECT * FROM {schema}.{table} ORDER BY id")
            ).mappings().all()
        assert len(rows) == 6
        assert rows[0]["nullable_split_key"] is None
        assert rows[0]["label"] is None
        assert rows[1]["amount"] == Decimal("-123.4500")
        assert rows[1]["label"] == "Jöhn@example.test"
        assert rows[3]["amount"] is None
        assert rows[3]["ratio"] is None
        assert rows[3]["active"] is None
        assert rows[4]["event_ts"] is None
        assert rows[4]["label"] == ""
    finally:
        engine.dispose()


def test_impala_support_scale_and_skew_live_gate():
    settings = _support_settings()
    schema, table = _qualified_table(settings.scale_table)
    with _connection(settings) as connection:
        row = connection.execute(
            text(
                "SELECT COUNT(*) AS row_count, "
                "COUNT(DISTINCT id) AS distinct_ids, "
                "SUM(CASE WHEN split_key = 1 THEN 1 ELSE 0 END) AS skew_rows, "
                "COUNT(DISTINCT split_key) AS distinct_split_keys "
                f"FROM {schema}.{table}"
            )
        ).mappings().one()

    assert row["row_count"] == 1_000_000
    assert row["distinct_ids"] == 1_000_000
    assert row["skew_rows"] == 800_000
    assert row["distinct_split_keys"] > 1_000


def test_impala_support_all_coordinators_live_gate():
    settings = _support_settings()
    schema, table = _qualified_table(settings.type_table)
    assert len(settings.coordinators) >= 3

    for host in settings.coordinators:
        with _connection(settings, host) as connection:
            row = connection.execute(
                text(
                    "SELECT VERSION() AS version, COUNT(*) AS row_count "
                    f"FROM {schema}.{table}"
                )
            ).mappings().one()
        assert "impalad version" in row["version"]
        assert row["row_count"] == 6


def test_impala_support_database_timeout_and_recovery_live_gate():
    if os.environ.get("FSP_RUN_IMPALA_TIMEOUT_GATE") != "1":
        pytest.skip("set FSP_RUN_IMPALA_TIMEOUT_GATE=1 to run the timeout gate")

    settings = _support_settings()
    schema, table = _qualified_table(settings.scale_table)
    from impala.dbapi import connect
    from impala.error import HiveServer2Error, OperationalError

    connect_kwargs: dict[str, object] = {
        "host": settings.host,
        "port": settings.port,
        "database": settings.database,
        "user": settings.username,
    }
    if settings.password:
        connect_kwargs["password"] = settings.password
    connect_kwargs.update(settings.query)

    started = time.monotonic()
    connection = connect(**connect_kwargs)
    try:
        cursor = connection.cursor()
        cursor.execute("SET EXEC_TIME_LIMIT_S=5")
        with pytest.raises(
            (HiveServer2Error, OperationalError),
            match="(?i)(expired|timeout|time limit|cancel)",
        ):
            cursor.execute(
                f"SELECT COUNT(*) FROM {schema}.{table} a "
                f"CROSS JOIN {schema}.{table} b"
            )
    finally:
        connection.close()
    assert time.monotonic() - started <= 15

    recovery_started = time.monotonic()
    recovered = connect(**connect_kwargs)
    try:
        cursor = recovered.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
    finally:
        recovered.close()
    assert time.monotonic() - recovery_started <= 30


def test_impala_support_source_mutation_live_gate():
    if os.environ.get("FSP_RUN_IMPALA_MUTATION_GATE") != "1":
        pytest.skip("set FSP_RUN_IMPALA_MUTATION_GATE=1 to run the mutation gate")

    settings = _support_settings()
    schema, table = _qualified_table(settings.refresh_table)
    baseline = (
        "(1, CAST('2026-01-01 00:00:00' AS TIMESTAMP), 'baseline')"
    )
    changed = (
        "(1, CAST('2026-01-01 00:00:00' AS TIMESTAMP), 'updated'),"
        "(2, CAST('2026-01-02 00:00:00' AS TIMESTAMP), 'inserted')"
    )

    with _connection(settings) as connection:
        try:
            connection.execute(
                text(f"INSERT OVERWRITE {schema}.{table} VALUES {''.join(changed)}")
            )
            rows = connection.execute(
                text(f"SELECT id, value FROM {schema}.{table} ORDER BY id")
            ).all()
            assert rows == [(1, "updated"), (2, "inserted")]
        finally:
            connection.execute(
                text(f"INSERT OVERWRITE {schema}.{table} VALUES {baseline}")
            )
        restored = connection.execute(
            text(f"SELECT id, value FROM {schema}.{table}")
        ).one()
        assert restored == (1, "baseline")


def test_impala_support_coordinator_restart_live_gate():
    if os.environ.get("FSP_RUN_IMPALA_RESTART_GATE") != "1":
        pytest.skip("set FSP_RUN_IMPALA_RESTART_GATE=1 to run the restart gate")

    settings = _support_settings()
    schema, table = _qualified_table(settings.scale_table)
    role_name = _coordinator_role()
    started = threading.Event()
    result: dict[str, BaseException | None] = {"error": None}

    def run_query() -> None:
        try:
            with _connection(settings) as connection:
                started.set()
                connection.execute(
                    text(
                        f"SELECT COUNT(*) FROM {schema}.{table} a "
                        f"CROSS JOIN {schema}.{table} b"
                    )
                ).scalar_one()
        except BaseException as error:  # noqa: BLE001 - asserted by the gate
            result["error"] = error
        finally:
            started.set()

    query = threading.Thread(target=run_query, name="impala-restart-query")
    query.start()
    assert started.wait(timeout=30)
    time.sleep(2)

    try:
        _role_command("stop", role_name)
        _wait_for_role(role_name, "STOPPED")
        query.join(timeout=30)
        assert not query.is_alive()
        assert result["error"] is not None
    finally:
        role = _control_api(
            "GET",
            "clusters/"
            f"{_required_environment('IMPALA_CONTROL_CLUSTER')}/services/"
            f"{os.environ.get('IMPALA_CONTROL_SERVICE', 'impala')}/roles/"
            f"{role_name}?view=summary",
        )
        if role.get("roleState") != "STARTED":
            _role_command("start", role_name)
        healthy = _wait_for_role(role_name, "STARTED")
        deadline = time.monotonic() + 300
        while healthy.get("healthSummary") != "GOOD" and time.monotonic() < deadline:
            time.sleep(5)
            healthy = _wait_for_role(role_name, "STARTED")
        assert healthy.get("healthSummary") == "GOOD"

    with _connection(settings) as connection:
        assert connection.execute(text("SELECT 1")).scalar_one() == 1


@pytest.mark.asyncio
async def test_impala_support_large_materialization_live_gate(
    monkeypatch: pytest.MonkeyPatch,
):
    if os.environ.get("FSP_RUN_IMPALA_MATERIALIZATION_GATE") != "1":
        pytest.skip(
            "set FSP_RUN_IMPALA_MATERIALIZATION_GATE=1 to run the materialization gate"
        )

    settings = _support_settings()
    source_schema, source_table = _qualified_table(settings.scale_table)
    url = build_url(
        dialect="impala",
        host=settings.host,
        port=settings.port,
        database=settings.database,
        username=settings.username,
        password=settings.password,
        query=settings.query,
    )
    db_url = url.render_as_string(hide_password=False)
    monkeypatch.setattr(config, "DB_URL", db_url)
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "content_hash")
    monkeypatch.setattr(config, "BUCKET_NAME", "impala-support-bucket")
    await executor.dispose_engines()

    engine = _engine(settings)
    try:
        reflected = inspect(engine).get_columns(
            source_table,
            schema=source_schema,
        )
    finally:
        engine.dispose()
    schema = [
        ColumnDef(
            field_id=index + 1,
            name=column["name"],
            iceberg_type=executor.sqlalchemy_type_to_iceberg(column["type"]),
            nullable=column["nullable"],
        )
        for index, column in enumerate(reflected)
    ]
    name = "impala_support_large_materialization"
    table = TableDef(
        name=name,
        source_table=settings.scale_table,
        schema=schema,
        key_column="split_key",
        num_splits=4,
        split_strategy="range",
        split_target_rows=1_000_000,
    )
    expected_rows = await BestEffortReadSession().execute_scalar(
        f"SELECT COUNT(*) FROM {settings.scale_table}"
    )

    import psutil

    process = psutil.Process()
    peak_rss = process.memory_info().rss
    stop_monitor = threading.Event()

    def monitor_memory() -> None:
        nonlocal peak_rss
        while not stop_monitor.wait(0.05):
            peak_rss = max(peak_rss, process.memory_info().rss)

    monitor = threading.Thread(target=monitor_memory, name="impala-memory-monitor")
    monitor.start()
    started = time.monotonic()
    state_store.unregister_snapshot(name)
    try:
        assert await freshness.poll_once(
            table,
            "impala-support-gate",
            "impala-support-gate",
        )
        elapsed = time.monotonic() - started
        snapshot = state_store.get_snapshot(name)
        assert snapshot.total_records == expected_rows == 1_000_000
        assert sum(split.record_count or 0 for split in snapshot.splits) == expected_rows
        assert len(snapshot.splits) == 4
        for split in snapshot.splits:
            data = parquet_cache.peek_parquet(split.object_key)
            assert data is not None
            assert pq.read_metadata(io.BytesIO(data)).num_rows == split.record_count
        from main import app

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://impala-support.test",
        ) as client:
            listing = await client.get(
                f"/{config.BUCKET_NAME}",
                params={"list-type": "2", "prefix": ""},
            )
            assert listing.status_code == 200
            key = snapshot.splits[0].object_key
            assert key in listing.text

            head = await client.head(f"/{config.BUCKET_NAME}/{key}")
            full = await client.get(f"/{config.BUCKET_NAME}/{key}")
            byte_range = await client.get(
                f"/{config.BUCKET_NAME}/{key}",
                headers={"Range": "bytes=0-1023"},
            )
            assert head.status_code == full.status_code == 200
            assert byte_range.status_code == 206
            assert byte_range.content == full.content[:1024]
            assert head.headers["etag"] == full.headers["etag"]
            assert int(head.headers["content-length"]) == len(full.content)
            assert hashlib.sha256(full.content).hexdigest() == hashlib.sha256(
                parquet_cache.peek_parquet(key)
            ).hexdigest()
            assert pq.read_metadata(io.BytesIO(full.content)).num_rows == (
                snapshot.splits[0].record_count
            )
        assert not await freshness.poll_once(
            table,
            "impala-support-gate",
            "impala-support-gate",
        )
        assert elapsed <= int(os.environ.get("IMPALA_MAX_MATERIALIZATION_SECONDS", "600"))
        assert peak_rss <= int(
            os.environ.get("IMPALA_MAX_MATERIALIZATION_RSS_BYTES", str(2 * 1024**3))
        )
    finally:
        stop_monitor.set()
        monitor.join(timeout=5)
        for snapshot in state_store.get_snapshot_history(name):
            for split in snapshot.splits:
                parquet_cache.evict_parquet(split.object_key)
        state_store.unregister_snapshot(name)
        freshness._probe_tokens.pop(name, None)
        freshness._ttl_gen.pop(name, None)
        await executor.dispose_engines()
