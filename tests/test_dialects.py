"""
F6 — SQL dialect adapter tests.

Verifies build_split_query() emits correct SQL per dialect:
  - SQLite / Postgres: LIMIT suffix, double-quoted identifiers, CAST type
  - SQL Server (T-SQL): TOP prefix (no LIMIT), bracket identifiers, BIGINT CAST
"""
from __future__ import annotations

import config
from config import ColumnDef, TableDef
from iceberg.state_store import SplitDescriptor
from planner.dialects import get_dialect, SQLiteDialect, PostgresDialect, MSSQLDialect
from planner.split_planner import build_split_query


_SCHEMA = [
    ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
    ColumnDef(field_id=2, name="name", iceberg_type="string", nullable=True),
]
_TABLE = TableDef(name="widgets", source_table="widgets", schema=_SCHEMA, num_splits=4)


def _split() -> SplitDescriptor:
    return SplitDescriptor(
        split_index=1,
        num_splits=4,
        object_key="warehouse/db/widgets/data/split-1-x.parquet",
        watermark_ms=0,
        table=_TABLE,
    )


# ---------------------------------------------------------------------------
# get_dialect resolution
# ---------------------------------------------------------------------------

def test_get_dialect_by_scheme():
    assert isinstance(get_dialect("sqlite+aiosqlite:///x.db"), SQLiteDialect)
    assert isinstance(get_dialect("postgresql+asyncpg://h/db"), PostgresDialect)
    assert isinstance(get_dialect("mssql+aioodbc://h/db"), MSSQLDialect)


def test_dialect_quoting():
    assert SQLiteDialect().quote("id") == '"id"'
    assert PostgresDialect().quote("id") == '"id"'
    assert MSSQLDialect().quote("id") == "[id]"


def test_dialect_cast_type():
    assert SQLiteDialect().cast_int("id") == "CAST(id AS INTEGER)"
    assert PostgresDialect().cast_int("id") == "CAST(id AS BIGINT)"
    assert MSSQLDialect().cast_int("id") == "CAST(id AS BIGINT)"


def test_quote_qualified_dotted():
    assert MSSQLDialect().quote_qualified("dbo.sales") == "[dbo].[sales]"
    assert SQLiteDialect().quote_qualified("main.sales") == '"main"."sales"'


# ---------------------------------------------------------------------------
# build_split_query per dialect
# ---------------------------------------------------------------------------

def test_split_query_sqlite(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "sqlite+aiosqlite:///x.db")
    sql, params = build_split_query(_split())
    assert 'SELECT "id", "name"' in sql
    assert 'FROM "widgets"' in sql
    assert "CAST(\"id\" AS INTEGER)" in sql
    assert "LIMIT :max_rows" in sql
    assert "TOP" not in sql
    assert params == {"num_splits": 4, "split_index": 1, "max_rows": config.QUERY_MAX_ROWS}


def test_split_query_postgres(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "postgresql+asyncpg://h/db")
    sql, _ = build_split_query(_split())
    assert 'CAST("id" AS BIGINT)' in sql
    assert "LIMIT :max_rows" in sql


def test_split_query_mssql(monkeypatch):
    monkeypatch.setattr(config, "DB_URL", "mssql+aioodbc://h/db")
    sql, _ = build_split_query(_split())
    assert sql.startswith("SELECT TOP (:max_rows) [id], [name]")
    assert "FROM [widgets]" in sql
    assert "CAST([id] AS BIGINT)" in sql
    assert "LIMIT" not in sql
    assert sql.rstrip().endswith("ORDER BY [id]")
