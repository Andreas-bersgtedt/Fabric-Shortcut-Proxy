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


def test_validate_rejects_duplicate_table_names():
    # The same table name under two sources would collide (Iceberg table id).
    _, errors = config.validate_setting_updates({
        "tables": [
            {"name": "orders", "source_table": "dbo.orders", "key_column": "id"},
            {"name": "orders", "source_table": "public.orders", "key_column": "id", "connection": "pg"},
        ]
    })
    assert errors and any("duplicate table name" in e.lower() for e in errors)

    clean, errs = config.validate_setting_updates({
        "tables": [
            {"name": "orders", "source_table": "dbo.orders", "key_column": "id"},
            {"name": "shipments", "source_table": "public.ship", "key_column": "id", "connection": "pg"},
        ]
    })
    assert not errs and "tables" in clean


def test_validate_table_column_policy_payloads():
    valid = {
        "name": "customers_safe",
        "source_table": "dbo.customers",
        "key_column": "customer_id",
        "schema": [
            {"field_id": 1, "name": "customer_id", "type": "long", "nullable": False},
            {
                "field_id": 2,
                "name": "email_token",
                "source": "email",
                "type": "string",
                "transform": {
                    "kind": "deterministic_hash",
                    "key_ref": "customer-pii-v1",
                    "domain": "customer-email",
                    "normalization": "trim_lower",
                },
            },
        ],
    }
    clean, errors = config.validate_setting_updates({"tables": [valid]})
    assert not errors and clean["tables"][0]["schema"][1]["source"] == "email"

    invalid = dict(valid)
    invalid["schema"] = [
        {
            "field_id": 1,
            "name": "customer_id_token",
            "source": "customer_id",
            "type": "string",
            "transform": {"kind": "random_token"},
        },
        {"field_id": 1, "name": "customer_id_token", "type": "string"},
    ]
    _, errors = config.validate_setting_updates({"tables": [invalid]})
    assert any("field_id values must be unique" in error for error in errors)
    assert any("output names must be unique" in error for error in errors)
    assert any("split key 'customer_id'" in error for error in errors)


def test_validate_rejects_table_on_undefined_connection_in_same_apply():
    # The config-builder "Apply" rewrites tables + connections together. A table
    # pointing at a source that is NOT in that connections[] would pass here but
    # fail the stricter startup validator, bricking the Manager — reject it early.
    _, errors = config.validate_setting_updates({
        "tables": [
            {"name": "SO_Header", "source_table": "dbo.SO_Header",
             "key_column": "id", "connection": "SyntheticData"},
        ],
        "connections": [{"id": "SalesLT", "db_url": "mssql+aioodbc://h/db"}],
    })
    assert errors and any("SyntheticData" in e and "not defined" in e for e in errors)


def test_validate_open_mirror_targets():
    clean, errors = config.validate_setting_updates({
        "open_mirror_targets": [{
            "id": "fabric-sales",
            "connection": "default",
            "landing_zone_root": "https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone",
            "tables": [{
                "name": "sales",
                "source_table": "dbo.sales",
                "key_column": "id",
                "target_table": "sales",
                "schema": "dbo",
                "mode": "incremental",
            }],
        }]
    })
    assert not errors
    assert clean["open_mirror_targets"][0]["id"] == "fabric-sales"

    _, invalid = config.validate_setting_updates({
        "open_mirror_targets": [{
            "id": "fabric-sales",
            "connection": "missing-conn",
            "landing_zone_root": "https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone",
            "tables": [{
                "name": "sales",
                "source_table": "dbo.sales",
                "key_column": "id",
                "target_table": "sales",
            }],
        }]
    })
    assert invalid and any("missing-conn" in err for err in invalid)


def test_write_open_mirror_target_routes_to_its_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config.write_config_updates({
        "open_mirror_targets": [{
            "id": "fabric-sales",
            "connection": "default",
            "landing_zone_root": "https://onelake.dfs.fabric.microsoft.com/ws/db/Files/LandingZone",
            "tables": [{
                "name": "sales",
                "source_table": "dbo.sales",
                "key_column": "id",
                "target_table": "sales",
            }],
        }]
    })
    data = json.loads((tmp_path / "config.open_mirror.json").read_text(encoding="utf-8"))
    assert data["open_mirror"]["open_mirror_targets"][0]["id"] == "fabric-sales"
    assert data["open_mirror"]["open_mirror_targets"][0]["connection"] == "default"

    # The same table validates when its source IS in the connections[] payload.
    clean, errs = config.validate_setting_updates({
        "tables": [
            {"name": "SO_Header", "source_table": "dbo.SO_Header",
             "key_column": "id", "connection": "SyntheticData"},
        ],
        "connections": [{"id": "SyntheticData", "db_url": "mssql+aioodbc://h/db"}],
    })
    assert not errs and "tables" in clean


def test_named_connection_env_override(monkeypatch):
    # DB_URL_<ID> overrides the file's db_url so secrets can stay out of config.
    monkeypatch.setenv("DB_URL_WAREHOUSE_PG", "postgresql+asyncpg://envhost/db")
    c = connection_config._connection_from_json(
        {"id": "warehouse_pg", "db_url": "postgresql+asyncpg://filehost/db"}
    )
    assert c.db_url == "postgresql+asyncpg://envhost/db"

    # No env override -> the file value wins.
    c2 = connection_config._connection_from_json(
        {"id": "other_src", "db_url": "postgresql+asyncpg://filehost/db"}
    )
    assert c2.db_url == "postgresql+asyncpg://filehost/db"


def test_inline_db_creds_gate_optin(monkeypatch):
    cred = {"db_url": "mssql+aioodbc://u:p@host/db"}    # Secure by default: an inline credential in db_url is rejected.
    monkeypatch.delenv("ALLOW_CONFIG_DB_CREDENTIALS", raising=False)
    with pytest.raises(ValueError):
        connection_config._gate_connection_dict(cred)

    # Explicit opt-in permits an inline db_url (local, gitignored config).
    monkeypatch.setenv("ALLOW_CONFIG_DB_CREDENTIALS", "1")
    connection_config._gate_connection_dict(cred)  # must not raise

    # ...but a secret under a sensitive KEY name is still rejected even opted-in.
    with pytest.raises(ValueError):
        connection_config._gate_connection_dict({"password": "hunter2"})

