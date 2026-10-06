"""Opt-in live gates for promoting Apache Impala to supported."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from decimal import Decimal
import json
import os
import re
import time
from typing import Iterator

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

from db.reflect import build_url


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
