"""Multi-source / multi-dialect connection support.

Verifies the connection registry, per-connection URL/limit resolution, that the
executor routes queries to the correct engine per connection id, and that
canonical object paths are namespaced by each table's own connection.
"""
from __future__ import annotations

import json
import os
import pathlib

import pytest
from sqlalchemy import create_engine, text

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import config
import connection_config
import db.executor as executor
import iceberg.state_store as state_store
from config import ColumnDef, TableDef


def _seed_sqlite(path: pathlib.Path, ids: list[int]) -> None:
    eng = create_engine(f"sqlite:///{path.as_posix()}")
    with eng.begin() as c:
        c.execute(text("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)"))
        for i in ids:
            c.execute(text("INSERT INTO widgets (id, name) VALUES (:i, :n)"), {"i": i, "n": f"w{i}"})
    eng.dispose()


@pytest.fixture
async def two_sources(tmp_path, monkeypatch):
    a = tmp_path / "default.db"
    b = tmp_path / "second.db"
    _seed_sqlite(a, [1, 2, 3])          # default source: 3 rows
    _seed_sqlite(b, [10, 20])           # named source:   2 rows

    # DEFAULT connection -> a (live config.DB_URL + reset module engines).
    monkeypatch.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{a.as_posix()}", raising=False)
    executor._engine = None
    executor._sync_engine = None

    # NAMED connection 'second' -> b.
    named = connection_config.Connection(
        id="second",
        db_url=f"sqlite+aiosqlite:///{b.as_posix()}",
        query_timeout_seconds=config.QUERY_TIMEOUT_SECONDS,
        query_max_rows=1234,
        db_max_retries=config.DB_MAX_RETRIES,
        db_retry_backoff_seconds=config.DB_RETRY_BACKOFF_SECONDS,
        validate_source_schema=config.VALIDATE_SOURCE_SCHEMA,
    )
    registry = dict(connection_config.CONNECTIONS)
    registry["second"] = named
    monkeypatch.setattr(connection_config, "CONNECTIONS", registry, raising=False)
    monkeypatch.setattr(config, "CONNECTIONS", registry, raising=False)
    executor._named_handles.clear()

    yield

    await executor.dispose_engines()


async def test_row_count_routes_per_connection(two_sources):
    # Same query text, different physical source per connection id.
    assert await executor.fetch_table_row_count("widgets") == 3
    assert await executor.fetch_table_row_count("widgets", connection="second") == 2


async def test_split_query_routes_per_connection(two_sources):
    sql = "SELECT id FROM widgets ORDER BY id"
    default_rows = await executor.execute_split_query(sql, {}, split_index=0)
    second_rows = await executor.execute_split_query(sql, {}, split_index=0, connection="second")
    assert [r["id"] for r in default_rows] == [1, 2, 3]
    assert [r["id"] for r in second_rows] == [10, 20]


async def test_named_handle_is_isolated(two_sources):
    # Named connection gets its own handle, distinct from the default engine.
    handle = executor._get_named_handle("second")
    assert handle.db_url.endswith("second.db")
    assert executor._db_url_for("second") == handle.db_url
    assert executor._db_url_for("default") == config.DB_URL


def test_effective_db_url_and_max_rows(two_sources):
    assert config.effective_db_url("default") == config.DB_URL
    assert config.effective_db_url("second").endswith("second.db")
    # Unknown ids fall back to the default.
    assert config.effective_db_url("ghost") == config.DB_URL
    assert config.effective_query_max_rows("second") == 1234
    assert config.effective_query_max_rows("default") == config.QUERY_MAX_ROWS


def test_default_connection_always_present():
    assert "default" in connection_config.CONNECTIONS
    assert connection_config.get_connection(None).id == "default"
    assert connection_config.get_connection("does-not-exist") is None


def test_canonical_path_namespaced_by_connection(monkeypatch):
    monkeypatch.setattr(config, "OBJECT_PATH_LAYOUT", "canonical", raising=False)
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://u:p@pg-a:5432/dba", raising=False)
    named = connection_config.Connection(
        id="warehouse",
        db_url="postgresql+asyncpg://u:p@pg-b:5432/dbb",
        query_timeout_seconds=config.QUERY_TIMEOUT_SECONDS,
        query_max_rows=config.QUERY_MAX_ROWS,
        db_max_retries=config.DB_MAX_RETRIES,
        db_retry_backoff_seconds=config.DB_RETRY_BACKOFF_SECONDS,
        validate_source_schema=config.VALIDATE_SOURCE_SCHEMA,
    )
    registry = dict(connection_config.CONNECTIONS)
    registry["warehouse"] = named
    monkeypatch.setattr(connection_config, "CONNECTIONS", registry, raising=False)
    monkeypatch.setattr(config, "CONNECTIONS", registry, raising=False)

    t_default = TableDef(name="orders", source_table="sales.orders", num_splits=2,
                         schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)])
    t_named = TableDef(name="orders", source_table="sales.orders", num_splits=2,
                       connection_id="warehouse",
                       schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)])

    p_default = state_store.canonical_table_path(t_default, "warehouse/db")
    p_named = state_store.canonical_table_path(t_named, "warehouse/db")
    assert p_default == "warehouse/db/pg-a/dba/sales/orders"
    assert p_named == "warehouse/db/pg-b/dbb/sales/orders"
    assert p_default != p_named


# ---------------------------------------------------------------------------
# Config-builder persistence contract for the connections[] array.
# ---------------------------------------------------------------------------

def test_validate_connections_setting():
    clean, errors = config.validate_setting_updates(
        {"connections": [{"id": "pg", "db_url": "postgresql+asyncpg://h/db"}]}
    )
    assert not errors
    assert clean["connections"][0]["id"] == "pg"

    _, reserved = config.validate_setting_updates(
        {"connections": [{"id": "default", "db_url": "x"}]}
    )
    assert reserved and "reserved" in reserved[0]

    _, missing = config.validate_setting_updates({"connections": [{"id": "pg"}]})
    assert missing

    _, dup = config.validate_setting_updates(
        {"connections": [{"id": "pg", "db_url": "a"}, {"id": "pg", "db_url": "b"}]}
    )
    assert dup

    _, not_list = config.validate_setting_updates({"connections": {"id": "pg"}})
    assert not_list


def test_write_connections_routes_to_connection_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config.write_config_updates({
        "db_url": "sqlite+aiosqlite:///./a.db",
        "connections": [{"id": "pg", "db_url": "postgresql+asyncpg://h/db"}],
    })
    data = json.loads((tmp_path / "config.connection.json").read_text(encoding="utf-8"))
    # db_url stays in the singular section; connections[] lands at the top level.
    assert data["connection"]["db_url"].endswith("a.db")
    assert data["connections"][0]["id"] == "pg"
    assert data["connections"][0]["db_url"] == "postgresql+asyncpg://h/db"


def test_write_tables_with_connection(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config.write_config_updates({
        "tables": [
            {"name": "orders", "source_table": "dbo.orders", "key_column": "id"},
            {"name": "ship", "source_table": "public.ship", "key_column": "id", "connection": "pg"},
        ]
    })
    data = json.loads((tmp_path / "config.tables.json").read_text(encoding="utf-8"))
    tbls = data["tables"]
    assert tbls[1]["connection"] == "pg"
    assert "connection" not in tbls[0]

