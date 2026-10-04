"""Opt-in live capability gates for Oracle, Databricks, Redshift, Teradata, Impala.

These gates require dedicated, small integration tables and read-only credentials.
Set FSP_RUN_SOURCE_CAPABILITY_GATES=1 to opt in. See
docs/SOURCE_CAPABILITIES.md for the per-source environment-variable contract and
the query costs. No source service is contacted when the opt-in flag is absent.
"""
from __future__ import annotations

import io
import os

import pytest
import pyarrow.parquet as pq

import config
import db.executor as executor
from config import ColumnDef, ColumnTransform, TableDef
from db.capabilities import capabilities_for_db_url
from db.read_points import BestEffortReadSession
from db.reflect import SchemaReflector, build_url
import cache.lru_cache as parquet_cache
import iceberg.state_store as state_store
from iceberg import freshness
from iceberg.state_store import SplitDescriptor
from observability import tokenization as tokenization_metrics
from planner.dialects import get_dialect
from planner.split_planner import (
    build_split_query,
    compute_key_ranges,
    compute_temporal_ranges,
    mins_from_equidepth,
)

_SOURCE_ENV = {
    "oracle": ("ORACLE", ("HOST", "DATABASE", "USERNAME", "PASSWORD")),
    "databricks": ("DATABRICKS", ("HOST", "TOKEN", "HTTP_PATH")),
    "redshift": ("REDSHIFT", ("HOST", "DATABASE", "USERNAME", "PASSWORD")),
    "teradata": ("TERADATA", ("HOST", "DATABASE", "USERNAME", "PASSWORD")),
    # The official Impala quickstart accepts unauthenticated NOSASL connections.
    "impala": ("IMPALA", ("HOST", "DATABASE")),
}


def _connection(prefix: str, dialect: str) -> dict:
    return {
        "dialect": dialect,
        "host": os.environ.get(f"{prefix}_HOST"),
        "port": int(os.environ[f"{prefix}_PORT"]) if os.environ.get(f"{prefix}_PORT") else None,
        "database": os.environ.get(f"{prefix}_DATABASE"),
        "username": (
            "token" if dialect == "databricks"
            else os.environ.get(f"{prefix}_USERNAME")
        ),
        "password": (
            os.environ.get(f"{prefix}_TOKEN") if dialect == "databricks"
            else os.environ.get(f"{prefix}_PASSWORD")
        ),
        "query": {
            **(
                {"http_path": os.environ[f"{prefix}_HTTP_PATH"]}
                if os.environ.get(f"{prefix}_HTTP_PATH")
                else {}
            ),
            **(
                {"catalog": os.environ[f"{prefix}_CATALOG"]}
                if os.environ.get(f"{prefix}_CATALOG")
                else {}
            ),
            **(
                {"schema": os.environ[f"{prefix}_SCHEMA"]}
                if os.environ.get(f"{prefix}_SCHEMA")
                else {}
            ),
        },
    }


def _row_value(row: dict, name: str):
    return next(
        value for key, value in row.items()
        if str(key).casefold() == name.casefold()
    )


def _live_gate_enabled(prefix: str, required: tuple[str, ...]) -> bool:
    return os.environ.get("FSP_RUN_SOURCE_CAPABILITY_GATES") == "1" and all(
        os.environ.get(f"INTEGRATION_{prefix}_{name}")
        for name in ("TABLE", "VIEW", "INTEGER_KEY", "DATE_COLUMN")
    ) and all(os.environ.get(f"{prefix}_{name}") for name in required)


def test_impala_live_gate_supports_unauthenticated_local_quickstart(monkeypatch):
    monkeypatch.setenv("FSP_RUN_SOURCE_CAPABILITY_GATES", "1")
    monkeypatch.setenv("IMPALA_HOST", "127.0.0.1")
    monkeypatch.setenv("IMPALA_DATABASE", "default")
    for name, value in {
        "TABLE": "issue94_gate.issue94_rows",
        "VIEW": "issue94_gate.issue94_rows_view",
        "INTEGER_KEY": "id",
        "DATE_COLUMN": "event_ts",
    }.items():
        monkeypatch.setenv(f"INTEGRATION_IMPALA_{name}", value)
    for name in ("USERNAME", "PASSWORD"):
        monkeypatch.delenv(f"IMPALA_{name}", raising=False)

    assert _live_gate_enabled("IMPALA", _SOURCE_ENV["impala"][1])
    connection = _connection("IMPALA", "impala")
    assert connection["username"] is None
    assert connection["password"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", tuple(_SOURCE_ENV))
async def test_source_capability_live_gate(dialect: str, monkeypatch):
    prefix, required = _SOURCE_ENV[dialect]
    if not _live_gate_enabled(prefix, required):
        pytest.skip(
            f"Set FSP_RUN_SOURCE_CAPABILITY_GATES=1 and the documented "
            f"{prefix} integration variables to run this live gate."
        )
    if capabilities_for_db_url(f"{dialect}://example").supports_primary_key_reflection:
        if not os.environ.get(f"INTEGRATION_{prefix}_PK_COLUMN"):
            pytest.skip(f"INTEGRATION_{prefix}_PK_COLUMN is required for the PK-reflection gate.")

    connection = _connection(prefix, dialect)
    url = build_url(**connection)
    db_url = url.render_as_string(hide_password=False)
    monkeypatch.setattr(config, "DB_URL", db_url)
    await executor.dispose_engines()
    table = os.environ[f"INTEGRATION_{prefix}_TABLE"]
    view = os.environ[f"INTEGRATION_{prefix}_VIEW"]
    integer_key = os.environ[f"INTEGRATION_{prefix}_INTEGER_KEY"]
    date_column = os.environ[f"INTEGRATION_{prefix}_DATE_COLUMN"]
    caps = capabilities_for_db_url(db_url)
    assert caps.supports_streaming_query is False
    assert caps.supports_freshness_probe is False

    async with SchemaReflector(url) as reflector:
        objects = await reflector.list_tables()
        listed = {
            (str(item.get("schema") or "").casefold(), str(item["name"]).casefold())
            for item in objects
        }
        table_schema, _, table_name = table.rpartition(".")
        view_schema, _, view_name = view.rpartition(".")
        assert (table_schema.casefold(), table_name.casefold()) in listed
        assert (view_schema.casefold(), view_name.casefold()) in listed
        columns = await reflector.columns(table)
        reflected_names = {column["name"].casefold() for column in columns}
        assert integer_key.casefold() in reflected_names
        assert date_column.casefold() in reflected_names
        from configbuilder.router import _split_key_metadata

        key_metadata = await _split_key_metadata(
            reflector, db_url, table, columns
        )
        if caps.requires_explicit_split_key:
            assert key_metadata["detected_key"] is None
            assert key_metadata["primary_key"] == []
            assert integer_key.casefold() in {
                column.casefold() for column in key_metadata["integer_keys"]
            }
        elif caps.supports_primary_key_reflection:
            expected_pk = os.environ[f"INTEGRATION_{prefix}_PK_COLUMN"]
            assert expected_pk.casefold() in {
                column.casefold() for column in key_metadata["primary_key"]
            }
        estimate = await reflector.approx_row_count(table)
        assert estimate is None or estimate >= 0

    session = BestEffortReadSession()
    integer_bounds = await session.fetch_key_bounds(table, integer_key)
    date_bounds = await session.fetch_column_bounds(table, date_column)
    quantiles = await session.fetch_key_quantile_bounds(
        table, integer_key, 2, key_is_integer=True
    )
    assert integer_bounds is not None and integer_bounds[0] <= integer_bounds[1]
    assert date_bounds is not None and date_bounds[0] <= date_bounds[1]
    assert quantiles is not None
    assert mins_from_equidepth([1, 5, 9], 2) == [1, 5]
    exact_rows = await session.fetch_table_row_count(table)
    assert exact_rows is not None and exact_rows >= 1

    # Exercise actual range queries and verify complete, non-overlapping key coverage.
    schema = [
        ColumnDef(
            field_id=index + 1,
            name=column["name"],
            iceberg_type=column["type"],
            nullable=column["nullable"],
        )
        for index, column in enumerate(columns)
    ]
    table_def = TableDef(
        name=table_name,
        source_table=table,
        schema=schema,
        key_column=integer_key,
        num_splits=2,
        split_strategy="range",
    )
    bounds = compute_key_ranges(*integer_bounds, 2)
    split_keys: list[int] = []
    for index, (low, high) in enumerate(bounds):
        split = SplitDescriptor(
            split_index=index,
            num_splits=2,
            object_key=f"issue94/{dialect}/split-{index}.parquet",
            watermark_ms=0,
            table=table_def,
            split_key_column=integer_key,
            key_lo=low,
            key_hi=high,
        )
        sql, params = build_split_query(split)
        rows = await executor.execute_split_query(
            sql, params, index, connection="default"
        )
        split_keys.extend(int(_row_value(row, integer_key)) for row in rows)
    assert len(split_keys) == len(set(split_keys))
    assert len(split_keys) == await session.execute_scalar(
        f"SELECT COUNT(*) FROM {get_dialect(db_url).quote_qualified(table)} "
        f"WHERE {get_dialect(db_url).quote(integer_key)} IS NOT NULL"
    )

    # Exercise actual date bounds/ranges and modulo queries against the source.
    date_type = next(
        column.iceberg_type for column in schema
        if column.name.casefold() == date_column.casefold()
    )
    assert date_type in {"date", "timestamp", "timestamptz"}
    date_ranges = compute_temporal_ranges(
        *date_bounds, 2, "date" if date_type == "date" else "timestamp"
    )
    date_table = TableDef(
        name=table_name,
        source_table=table,
        schema=schema,
        key_column=date_column,
        num_splits=2,
        split_strategy="date",
    )
    date_rows = 0
    for index, (low, high) in enumerate(date_ranges):
        split = SplitDescriptor(
            split_index=index,
            num_splits=2,
            object_key=f"issue94/{dialect}/date-split-{index}.parquet",
            watermark_ms=0,
            table=date_table,
            split_key_column=date_column,
            key_lo=low,
            key_hi=high,
        )
        sql, params = build_split_query(split)
        date_rows += len(
            await executor.execute_split_query(sql, params, index, connection="default")
        )
    expected_date_rows = await session.execute_scalar(
        f"SELECT COUNT(*) FROM {get_dialect(db_url).quote_qualified(table)} "
        f"WHERE {get_dialect(db_url).quote(date_column)} IS NOT NULL"
    )
    assert date_rows == expected_date_rows, (
        f"{dialect} temporal range coverage was incomplete: "
        f"actual={date_rows}, expected={expected_date_rows}, "
        f"type={date_type}, bounds={date_bounds!r}, ranges={date_ranges!r}"
    )

    modulo_keys: list[int] = []
    for index in range(2):
        split = SplitDescriptor(
            split_index=index,
            num_splits=2,
            object_key=f"issue94/{dialect}/modulo-split-{index}.parquet",
            watermark_ms=0,
            table=table_def,
            split_key_column=integer_key,
        )
        sql, params = build_split_query(split)
        rows = await executor.execute_split_query(sql, params, index, connection="default")
        modulo_keys.extend(int(_row_value(row, integer_key)) for row in rows)
    assert len(modulo_keys) == len(set(modulo_keys)) == exact_rows

    # Reopen a disposed pool and verify the source remains reachable.
    await executor.dispose_engines()
    smoke_sql = "SELECT 1 FROM dual" if dialect == "oracle" else "SELECT 1"
    assert int(await executor.execute_scalar(smoke_sql)) == 1
    await executor.dispose_engines()


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", tuple(_SOURCE_ENV))
async def test_source_materialization_and_refresh_live_gate(dialect: str, monkeypatch):
    prefix, required = _SOURCE_ENV[dialect]
    if not _live_gate_enabled(prefix, required):
        pytest.skip(
            f"Set FSP_RUN_SOURCE_CAPABILITY_GATES=1 and the documented "
            f"{prefix} integration variables to run this live gate."
        )
    if (
        capabilities_for_db_url(f"{dialect}://example").supports_primary_key_reflection
        and not os.environ.get(f"INTEGRATION_{prefix}_PK_COLUMN")
    ):
        pytest.skip(f"INTEGRATION_{prefix}_PK_COLUMN is required for the PK-reflection gate.")

    url = build_url(**_connection(prefix, dialect))
    db_url = url.render_as_string(hide_password=False)
    monkeypatch.setattr(config, "DB_URL", db_url)
    monkeypatch.setattr(config, "REFRESH_STRATEGY", "content_hash")
    await executor.dispose_engines()

    source_table = os.environ[f"INTEGRATION_{prefix}_TABLE"]
    integer_key = os.environ[f"INTEGRATION_{prefix}_INTEGER_KEY"]
    name = f"issue94_refresh_{dialect}"
    async with SchemaReflector(url) as reflector:
        reflected = await reflector.columns(source_table)
    schema = [
        ColumnDef(
            field_id=index + 1,
            name=column["name"],
            iceberg_type=column["type"],
            nullable=column["nullable"],
        )
        for index, column in enumerate(reflected)
    ]
    table = TableDef(
        name=name,
        source_table=source_table,
        schema=schema,
        key_column=integer_key,
        num_splits=2,
        split_strategy="range",
    )
    expected_rows = await BestEffortReadSession().execute_scalar(
        f"SELECT COUNT(*) FROM {get_dialect(db_url).quote_qualified(source_table)}"
    )

    state_store.unregister_snapshot(name)
    try:
        assert await freshness.poll_once(table, "issue94-live-gate", "issue94-live-gate")
        first = state_store.get_snapshot(name)
        assert first.total_records == expected_rows
        assert sum(split.record_count or 0 for split in first.splits) == expected_rows
        for split in first.splits:
            data = parquet_cache.peek_parquet(split.object_key)
            assert data is not None
            assert pq.read_metadata(io.BytesIO(data)).num_rows == split.record_count

        assert not await freshness.poll_once(
            table, "issue94-live-gate", "issue94-live-gate"
        )
        assert state_store.get_snapshot(name).snapshot_id == first.snapshot_id
        assert len(state_store.get_snapshot_history(name)) == 1
    finally:
        for split in state_store.get_snapshot_history(name):
            for data_split in split.splits:
                parquet_cache.evict_parquet(data_split.object_key)
        state_store.unregister_snapshot(name)
        freshness._probe_tokens.pop(name, None)
        freshness._ttl_gen.pop(name, None)
        await executor.dispose_engines()


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ("redshift", "teradata", "impala"))
async def test_arrow_fallback_live_materialization_gate(
    dialect: str, monkeypatch, capsys
):
    prefix, required = _SOURCE_ENV[dialect]
    fixture_fields = (
        "TABLE", "INTEGER_KEY", "TOKEN_COLUMN", "NULL_ROW_KEY", "UNICODE_ROW_KEY"
    )
    if (
        not _live_gate_enabled(prefix, required)
        or not all(
            os.environ.get(f"INTEGRATION_{prefix}_{field}")
            for field in fixture_fields
        )
    ):
        pytest.skip(
            f"Set the {prefix} live gate variables plus token, NULL-row, and "
            "Unicode-row fixture values to run Arrow fallback materialization."
        )

    url = build_url(**_connection(prefix, dialect))
    db_url = url.render_as_string(hide_password=False)
    monkeypatch.setattr(config, "DB_URL", db_url)
    monkeypatch.setattr(config, "TOKENIZATION_FALLBACK", "arrow")
    await executor.dispose_engines()
    tokenization_metrics.reset()

    source_table = os.environ[f"INTEGRATION_{prefix}_TABLE"]
    key_column = os.environ[f"INTEGRATION_{prefix}_INTEGER_KEY"]
    token_column = os.environ[f"INTEGRATION_{prefix}_TOKEN_COLUMN"]
    null_key = int(os.environ[f"INTEGRATION_{prefix}_NULL_ROW_KEY"])
    unicode_key = int(os.environ[f"INTEGRATION_{prefix}_UNICODE_ROW_KEY"])
    dialect_adapter = get_dialect(db_url)
    raw_unicode_value = await executor.execute_scalar(
        f"SELECT {dialect_adapter.quote(token_column)} "
        f"FROM {dialect_adapter.quote_qualified(source_table)} "
        f"WHERE {dialect_adapter.quote(key_column)} = :unicode_key",
        {"unicode_key": unicode_key},
    )
    assert isinstance(raw_unicode_value, str)
    assert any(ord(character) > 127 for character in raw_unicode_value)
    expected_rows = await executor.execute_scalar(
        f"SELECT COUNT(*) FROM {dialect_adapter.quote_qualified(source_table)}"
    )
    name = f"issue94_arrow_{dialect}"
    table = TableDef(
        name=name,
        source_table=source_table,
        key_column=key_column,
        num_splits=1,
        schema=[
            ColumnDef(field_id=1, name=key_column, iceberg_type="long", nullable=False),
            ColumnDef(
                field_id=2,
                name="token_value",
                iceberg_type="string",
                source=token_column,
                transform=ColumnTransform(kind="random_token"),
            ),
        ],
    )
    split = SplitDescriptor(
        split_index=0,
        num_splits=1,
        object_key=f"issue94-live-gate/{dialect}/arrow.parquet",
        watermark_ms=0,
        table=table,
    )
    try:
        from runtime.materializer import _materialize_split_once

        assert capabilities_for_db_url(db_url).tokenization_backend(
            "random_token", "arrow"
        ) == "arrow"
        assert await _materialize_split_once(split) == expected_rows
        data = parquet_cache.peek_parquet(split.object_key)
        assert data is not None
        rows = pq.read_table(io.BytesIO(data)).to_pylist()
        tokens = {
            int(_row_value(row, key_column)): _row_value(row, "token_value")
            for row in rows
        }
        assert tokens[null_key] is None
        assert tokens[unicode_key]
        assert tokens[unicode_key] != raw_unicode_value
        assert tokenization_metrics.snapshot()[name] == {
            "count": 1,
            "columns": ["token_value"],
            "flavors": [dialect],
            "kinds": ["random_token"],
        }
        logs = capsys.readouterr().out
        assert "arrow_tokenization_fallback" in logs
        assert "plaintext_values_cross_proxy=True" in logs
        assert raw_unicode_value not in logs
    finally:
        parquet_cache.evict_parquet(split.object_key)
        tokenization_metrics.reset()
        await executor.dispose_engines()


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ("oracle", "databricks"))
async def test_native_tokenization_live_null_and_unicode_gate(dialect: str, monkeypatch):
    prefix, required = _SOURCE_ENV[dialect]
    token_key = os.environ.get("INTEGRATION_TOKENIZATION_KEY")
    fixture_fields = (
        "TABLE", "INTEGER_KEY", "TOKEN_COLUMN", "NULL_ROW_KEY", "UNICODE_ROW_KEY"
    )
    if (
        not _live_gate_enabled(prefix, required)
        or not token_key
        or not all(os.environ.get(f"INTEGRATION_{prefix}_{field}") for field in fixture_fields)
    ):
        pytest.skip(
            "Set the source gate variables plus INTEGRATION_TOKENIZATION_KEY and "
            "the null/Unicode tokenization fixture row variables."
        )
    monkeypatch.setenv("FSP_TOKENIZATION_KEY_ISSUE94_LIVE_GATE", token_key)
    connection = _connection(prefix, dialect)
    url = build_url(**connection)
    db_url = url.render_as_string(hide_password=False)
    monkeypatch.setattr(config, "DB_URL", db_url)
    await executor.dispose_engines()
    table = os.environ[f"INTEGRATION_{prefix}_TABLE"]
    key_column = os.environ[f"INTEGRATION_{prefix}_INTEGER_KEY"]
    token_column = os.environ[f"INTEGRATION_{prefix}_TOKEN_COLUMN"]
    null_key = int(os.environ[f"INTEGRATION_{prefix}_NULL_ROW_KEY"])
    unicode_key = int(os.environ[f"INTEGRATION_{prefix}_UNICODE_ROW_KEY"])
    dialect_adapter = get_dialect(db_url)
    raw_value = await executor.execute_scalar(
        f"SELECT {dialect_adapter.quote(token_column)} "
        f"FROM {dialect_adapter.quote_qualified(table)} "
        f"WHERE {dialect_adapter.quote(key_column)} = :unicode_row_key",
        {"unicode_row_key": unicode_key},
    )
    assert isinstance(raw_value, str)
    assert any(ord(character) > 127 for character in raw_value)

    async def read_tokens(kind: str) -> list[dict]:
        transform = ColumnTransform(
            kind=kind,
            key_ref="issue94-live-gate" if kind == "deterministic_hash" else None,
            domain="issue94-native-tokenization",
        )
        column = ColumnDef(
            field_id=1,
            name="token_value",
            iceberg_type="string",
            source=token_column,
            transform=transform,
        )
        projection, _, params = dialect_adapter.render_projection(column, "live_gate")
        sql = (
            f"SELECT {dialect_adapter.quote(key_column)}, {projection} "
            f"FROM {dialect_adapter.quote_qualified(table)} "
            f"WHERE {dialect_adapter.quote(key_column)} IN "
            "(:null_row_key, :unicode_row_key) "
            f"ORDER BY {dialect_adapter.quote(key_column)}"
        )
        return await executor.execute_split_query(
            sql, {**params, "null_row_key": null_key, "unicode_row_key": unicode_key}, 0
        )

    deterministic = await read_tokens("deterministic_hash")
    deterministic_again = await read_tokens("deterministic_hash")
    assert len(deterministic) == 2
    deterministic_by_key = {
        _row_value(row, key_column): _row_value(row, "token_value")
        for row in deterministic
    }
    repeated_by_key = {
        _row_value(row, key_column): _row_value(row, "token_value")
        for row in deterministic_again
    }
    assert deterministic_by_key[null_key] is None
    assert deterministic_by_key[unicode_key]
    assert deterministic_by_key[unicode_key] != raw_value
    assert deterministic_by_key[unicode_key] == repeated_by_key[unicode_key]

    random_values = await read_tokens("random_token")
    random_values_again = await read_tokens("random_token")
    assert len(random_values) == 2
    random_by_key = {
        _row_value(row, key_column): _row_value(row, "token_value")
        for row in random_values
    }
    random_again_by_key = {
        _row_value(row, key_column): _row_value(row, "token_value")
        for row in random_values_again
    }
    assert random_by_key[null_key] is None
    assert random_by_key[unicode_key]
    assert random_by_key[unicode_key] != raw_value
    assert random_by_key[unicode_key] != random_again_by_key[unicode_key]
    await executor.dispose_engines()
