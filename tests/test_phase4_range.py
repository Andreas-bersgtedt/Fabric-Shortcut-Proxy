"""Phase 4 scale engine: range-based split planning (index-pruned splits)."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest
from sqlalchemy import text as _text
from sqlalchemy.ext.asyncio import create_async_engine

import config
from config import ColumnDef, TableDef
from iceberg.state_store import SplitDescriptor
from planner.split_planner import compute_key_ranges, build_split_query, pk_column


def _table(source="t", n=4):
    return TableDef(
        name=source, source_table=source, num_splits=n, key_column="id",
        schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
                ColumnDef(field_id=2, name="val", iceberg_type="string", nullable=True)],
    )


def _split(idx, n, *, key_lo=None, key_hi=None, source="t"):
    return SplitDescriptor(split_index=idx, num_splits=n, object_key="k",
                           watermark_ms=0, table=_table(source, n),
                           key_lo=key_lo, key_hi=key_hi)


# ---------------------------------------------------------------------------
# compute_key_ranges (pure)
# ---------------------------------------------------------------------------

def test_ranges_cover_fully_no_overlap():
    ranges = compute_key_ranges(1, 100, 8)
    assert len(ranges) == 8
    assert ranges[0][0] == 1
    assert ranges[-1][1] == 101                       # hi + 1 (inclusive of 100)
    for (a, b), (c, d) in zip(ranges, ranges[1:]):
        assert b == c                                 # contiguous, no gap/overlap
    covered = set()
    for lo, hi in ranges:
        covered |= set(range(lo, hi))
    assert covered == set(range(1, 101))              # exact coverage


def test_ranges_more_splits_than_keys():
    ranges = compute_key_ranges(1, 3, 8)              # 3 keys, 8 splits -> some empty
    covered = [k for lo, hi in ranges for k in range(lo, hi)]
    assert sorted(covered) == [1, 2, 3]               # each key once, no dupes
    assert ranges[-1][1] == 4


def test_ranges_single_key():
    ranges = compute_key_ranges(5, 5, 4)
    assert [k for lo, hi in ranges for k in range(lo, hi)] == [5]


def test_ranges_hi_below_lo_is_safe():
    ranges = compute_key_ranges(10, 9, 4)             # empty/degenerate table
    assert len(ranges) == 4


# ---------------------------------------------------------------------------
# build_split_query: range branch vs modulo default
# ---------------------------------------------------------------------------

def test_build_split_query_range_branch():
    sql, params = build_split_query(_split(2, 4, key_lo=25, key_hi=50))
    assert ">=" in sql and "<" in sql and "%" not in sql
    assert params == {"key_lo": 25, "key_hi": 50, "max_rows": config.QUERY_MAX_ROWS}


def test_build_split_query_modulo_default_unchanged():
    sql, params = build_split_query(_split(2, 4))     # no bounds -> modulo
    assert "%" in sql and ">=" not in sql
    assert params["num_splits"] == 4 and params["split_index"] == 2
    assert "key_lo" not in params


def test_pk_column_prefers_key_column():
    assert pk_column(_table()) == "id"


# ---------------------------------------------------------------------------
# End-to-end over SQLite: range splits read only their slice and together
# cover every row exactly once (no gaps, no dupes).
# ---------------------------------------------------------------------------

@pytest.fixture
async def range_db(tmp_path, monkeypatch):
    import db.executor as ex
    url = f"sqlite+aiosqlite:///{(tmp_path / 'range.db').as_posix()}"
    monkeypatch.setattr(config, "DB_URL", url, raising=False)
    old_engine = ex._engine
    ex._engine = None

    eng = create_async_engine(url)
    async with eng.begin() as conn:
        await conn.execute(_text("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)"))
        await conn.execute(_text(
            "INSERT INTO t2 (id, val) VALUES " +
            ",".join(f"({i},'v{i}')" for i in range(1, 251))
        ))
        await conn.execute(_text("CREATE TABLE t_empty (id INTEGER PRIMARY KEY)"))
    await eng.dispose()

    yield url

    if ex._engine is not None:
        await ex._engine.dispose()
    ex._engine = old_engine


async def test_fetch_key_bounds(range_db):
    from db.executor import fetch_key_bounds
    assert await fetch_key_bounds("t2", "id") == (1, 250)
    assert await fetch_key_bounds("t_empty", "id") is None    # empty -> modulo fallback


async def test_range_splits_cover_all_rows_once(range_db):
    from db.executor import fetch_key_bounds, execute_split_query
    bounds = await fetch_key_bounds("t2", "id")
    ranges = compute_key_ranges(*bounds, 8)
    seen: list[int] = []
    for i, (lo, hi) in enumerate(ranges):
        sql, params = build_split_query(_split(i, 8, key_lo=lo, key_hi=hi, source="t2"))
        assert ">=" in sql                                    # range query, index-pruned
        rows = await execute_split_query(sql, params, split_index=i)
        seen += [r["id"] for r in rows]
    assert sorted(seen) == list(range(1, 251))                # every row exactly once
