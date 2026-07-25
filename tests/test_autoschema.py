"""
Auto-schema derivation tests (usability):
  - SQLAlchemy type -> Iceberg type mapping
  - schema reflected from source metadata (no manual ColumnDef needed)
  - key column: explicit, auto-detected PK, and validation errors
"""
from __future__ import annotations

import os
import pathlib

import pytest
from sqlalchemy import types as satypes, text

_DB = pathlib.Path(__file__).parent / "test_autoschema.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_DB.as_posix()}"

import config
from config import TableDef
import db.executor as executor
from db.executor import (
    sqlalchemy_type_to_iceberg,
    derive_table_schema,
    resolve_tables,
)


# ---------------------------------------------------------------------------
# Type mapping (pure)
# ---------------------------------------------------------------------------

def test_type_mapping():
    assert sqlalchemy_type_to_iceberg(satypes.BigInteger()) == "long"
    assert sqlalchemy_type_to_iceberg(satypes.Integer()) == "int"
    assert sqlalchemy_type_to_iceberg(satypes.SmallInteger()) == "int"
    assert sqlalchemy_type_to_iceberg(satypes.Float()) == "double"
    assert sqlalchemy_type_to_iceberg(satypes.Numeric(12, 2)) == "decimal(12,2)"
    assert sqlalchemy_type_to_iceberg(satypes.String(50)) == "string"
    assert sqlalchemy_type_to_iceberg(satypes.Text()) == "string"
    assert sqlalchemy_type_to_iceberg(satypes.Boolean()) == "boolean"
    assert sqlalchemy_type_to_iceberg(satypes.Date()) == "date"
    # Fabric SQL endpoint rejects TIMESTAMP_NTZ, so naive datetime -> timestamptz
    # by default (TIMESTAMP_ASSUME_UTC).
    assert sqlalchemy_type_to_iceberg(satypes.DateTime()) == "timestamptz"
    assert sqlalchemy_type_to_iceberg(satypes.DateTime(timezone=True)) == "timestamptz"
    assert sqlalchemy_type_to_iceberg(satypes.LargeBinary()) == "binary"


def test_naive_datetime_ntz_opt_out(monkeypatch):
    import config
    monkeypatch.setattr(config, "TIMESTAMP_ASSUME_UTC", False, raising=False)
    assert sqlalchemy_type_to_iceberg(satypes.DateTime()) == "timestamp"
    # An explicit timezone is always timestamptz regardless of the flag.
    assert sqlalchemy_type_to_iceberg(satypes.DateTime(timezone=True)) == "timestamptz"


def test_type_mapping_uuid_and_money():
    # GUIDs -> string (avoids the fixed(16) binary path); SQL Server money -> decimal.
    from sqlalchemy.dialects.mssql import UNIQUEIDENTIFIER, MONEY
    assert sqlalchemy_type_to_iceberg(satypes.Uuid()) == "string"
    assert sqlalchemy_type_to_iceberg(UNIQUEIDENTIFIER()) == "string"
    assert sqlalchemy_type_to_iceberg(MONEY()) == "decimal(19,4)"


def test_pyarrow_schema_handles_uuid_fixed_binary():
    import pyarrow as pa
    from iceberg.schema import _iceberg_to_pa
    # These previously crashed with AttributeError (pa.fixed_size_binary).
    assert _iceberg_to_pa("uuid") == pa.binary(16)
    assert _iceberg_to_pa("fixed(8)") == pa.binary(8)
    assert _iceberg_to_pa("binary") == pa.binary()


# ---------------------------------------------------------------------------
# Reflection + resolution against a real SQLite table
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
async def seeded():
    mp = pytest.MonkeyPatch()
    mp.setattr(config, "DB_URL", f"sqlite+aiosqlite:///{_DB.as_posix()}")
    executor._engine = None
    engine = executor.get_engine()
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS widgets"))
        await conn.execute(text(
            "CREATE TABLE widgets ("
            "widget_id INTEGER PRIMARY KEY, name TEXT, price REAL, in_stock INTEGER)"
        ))
        await conn.execute(text(
            "INSERT INTO widgets VALUES (1,'a',1.5,1),(2,'b',2.5,0)"
        ))
    yield
    await engine.dispose()
    executor._engine = None
    mp.undo()
    if _DB.exists():
        _DB.unlink(missing_ok=True)


async def test_derive_schema_from_source(seeded):
    schema = await derive_table_schema("widgets")
    assert [c.name for c in schema] == ["widget_id", "name", "price", "in_stock"]
    assert [c.field_id for c in schema] == [1, 2, 3, 4]
    by_name = {c.name: c.iceberg_type for c in schema}
    assert by_name["widget_id"] == "int"     # SQLite INTEGER
    assert by_name["name"] == "string"
    assert by_name["price"] == "double"       # SQLite REAL


async def test_resolve_tables_autodetects_pk(seeded):
    t = TableDef(name="widgets", source_table="widgets")  # schema/key auto
    await resolve_tables([t])
    assert t.schema is not None
    assert [c.name for c in t.schema][0] == "widget_id"
    assert t.key_column == "widget_id"        # detected from PRIMARY KEY


async def test_resolve_tables_explicit_key(seeded):
    t = TableDef(name="widgets", source_table="widgets", key_column="widget_id")
    await resolve_tables([t])
    assert t.key_column == "widget_id"


async def test_resolve_tables_rejects_non_integer_key(seeded):
    t = TableDef(name="widgets", source_table="widgets", key_column="name")
    with pytest.raises(RuntimeError):
        await resolve_tables([t])


async def test_resolve_tables_rejects_unknown_key(seeded):
    t = TableDef(name="widgets", source_table="widgets", key_column="nope")
    with pytest.raises(RuntimeError):
        await resolve_tables([t])
