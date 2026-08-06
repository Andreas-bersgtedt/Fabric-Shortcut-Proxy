"""Equal-count (NTILE) split-balance planning tests.

``split_balance="count"`` sizes range/date splits by row quantiles so skewed
keys yield balanced splits. Default ``span`` behavior is unchanged.
"""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest
from sqlalchemy import text as _text
from sqlalchemy.ext.asyncio import create_async_engine

import config
from config import ColumnDef, TableDef, _tabledef_from_json
from iceberg.state_store import SnapshotState, SplitDescriptor
from planner.split_planner import plan_ranges_for_snapshot


def _table(**overrides) -> TableDef:
    kwargs = dict(
        name="events", source_table="events", key_column="id", num_splits=4,
        schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
                ColumnDef(field_id=2, name="val", iceberg_type="string", nullable=True)],
    )
    kwargs.update(overrides)
    return TableDef(**kwargs)


def _snapshot(table: TableDef, splits: int) -> SnapshotState:
    base = "db/local/default/dbo/events"
    snap = SnapshotState(
        snapshot_id=1, sequence_number=1, watermark_ms=1_700_000_000_000,
        manifest_list_key=f"{base}/metadata/snap.avro",
        manifest_file_key=f"{base}/metadata/m1.avro",
        metadata_key=f"{base}/metadata/v1.metadata.json",
        version_hint_key=f"{base}/metadata/version-hint.text",
        table=table, table_path=base, legacy_table_path=base,
    )
    snap.splits = [
        SplitDescriptor(split_index=i, num_splits=splits,
                        object_key=f"{base}/data/split-{i}.parquet",
                        watermark_ms=snap.watermark_ms, table=table)
        for i in range(splits)
    ]
    return snap


# ---------------------------------------------------------------------------
# Config surface
# ---------------------------------------------------------------------------

def test_effective_split_balance_falls_back_to_global(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_BALANCE", "span", raising=False)
    assert _table().effective_split_balance == "span"


def test_effective_split_balance_uses_override(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_BALANCE", "span", raising=False)
    assert _table(split_balance="count").effective_split_balance == "count"


def test_tabledef_normalizes_balance_case():
    assert _table(split_balance="COUNT").split_balance == "count"


def test_tabledef_from_json_parses_balance():
    t = _tabledef_from_json({"name": "e", "source_table": "e", "split_balance": "count"})
    assert t.split_balance == "count"


def test_tabledef_from_json_absent_balance_is_none():
    t = _tabledef_from_json({"name": "e", "source_table": "e"})
    assert t.split_balance is None


def test_effective_split_sample_rows_falls_back_to_global(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_SAMPLE_ROWS", 0, raising=False)
    assert _table().effective_split_sample_rows == 0
    assert _table(split_sample_rows=500).effective_split_sample_rows == 500


def test_tabledef_from_json_parses_sample_rows():
    t = _tabledef_from_json({"name": "e", "source_table": "e", "split_sample_rows": 500000})
    assert t.split_sample_rows == 500000


# ---------------------------------------------------------------------------
# End-to-end over SQLite: skewed integer key.
# 300 rows in [1..100] (dense) + 6 rows in [1000..6000] (sparse). Equal-span
# would starve the sparse tail splits; equal-count balances the row counts.
# ---------------------------------------------------------------------------

@pytest.fixture
async def skew_db(tmp_path, monkeypatch):
    import db.executor as ex
    url = f"sqlite+aiosqlite:///{(tmp_path / 'skew.db').as_posix()}"
    monkeypatch.setattr(config, "DB_URL", url, raising=False)
    old_engine = ex._engine
    ex._engine = None

    dense = list(range(1, 101)) * 3                       # 300 rows, ids 1..100
    sparse = [1000, 2000, 3000, 4000, 5000, 6000]         # 6 far-out rows
    ids = dense + sparse
    eng = create_async_engine(url)
    async with eng.begin() as conn:
        await conn.execute(_text("CREATE TABLE events (rowid_ INTEGER PRIMARY KEY AUTOINCREMENT, id INTEGER, val TEXT)"))
        await conn.execute(_text(
            "INSERT INTO events (id, val) VALUES " +
            ",".join(f"({i},'v')" for i in ids)
        ))
        await conn.execute(_text("CREATE TABLE empty_t (id INTEGER PRIMARY KEY)"))
    await eng.dispose()

    yield url

    if ex._engine is not None:
        await ex._engine.dispose()
    ex._engine = old_engine


async def test_fetch_key_quantile_bounds_balances_rows(skew_db):
    from db.executor import fetch_key_quantile_bounds
    result = await fetch_key_quantile_bounds("events", "id", 4)
    assert result is not None
    mins, overall_max = result
    assert len(mins) == 4                        # four buckets
    assert mins == sorted(mins)                  # non-decreasing bucket minimums
    assert overall_max == 6000


async def test_fetch_key_quantile_bounds_empty_is_none(skew_db):
    from db.executor import fetch_key_quantile_bounds
    assert await fetch_key_quantile_bounds("empty_t", "id", 4) is None


async def test_balanced_planning_beats_span_on_skew(skew_db):
    from db.executor import execute_split_query, fetch_key_bounds
    from planner.split_planner import build_split_query, compute_key_ranges

    n = 4

    # Equal-count: each split holds ~equal rows despite the skew.
    table = _table(split_strategy="range", split_balance="count")
    snap = _snapshot(table, n)
    assert await plan_ranges_for_snapshot(snap) is True
    counts_count = []
    seen: list[int] = []
    for i, split in enumerate(snap.splits):
        sql, params = build_split_query(split)
        assert ">=" in sql and "<" in sql        # index-pruned range query
        rows = await execute_split_query(sql, params, split_index=i)
        counts_count.append(len(rows))
        seen += [r["id"] for r in rows]
    assert sum(counts_count) == 306              # every row exactly once
    assert max(counts_count) - min(counts_count) <= 80   # well balanced

    # Equal-span over the same data would dump ~all rows into split 0.
    bounds = await fetch_key_bounds("events", "id")
    span_ranges = compute_key_ranges(*bounds, n)
    counts_span = []
    for i, (lo, hi) in enumerate(span_ranges):
        split = SplitDescriptor(split_index=i, num_splits=n, object_key="k",
                                watermark_ms=0, table=table, key_lo=lo, key_hi=hi)
        split.split_key_column = "id"
        sql, params = build_split_query(split)
        rows = await execute_split_query(sql, params, split_index=i)
        counts_span.append(len(rows))
    assert sum(counts_span) == 306
    assert max(counts_span) >= 200               # span dumps the dense block into one split


async def test_balance_span_default_uses_equal_span(skew_db):
    # Default balance="span" must not invoke quantile planning (behavior unchanged).
    table = _table(split_strategy="range")       # split_balance unset -> global "span"
    snap = _snapshot(table, 4)
    assert await plan_ranges_for_snapshot(snap) is True
    # Span slices the key axis evenly; the first split spans ids [1, ~1500).
    assert snap.splits[0].key_lo == 1


async def test_sampled_quantiles_still_cover_all_rows(skew_db):
    from db.executor import fetch_key_quantile_bounds

    # Sample far below the row count so a stride sample is used. The result must
    # still anchor to the true min (1) and max (6000) so no edge rows are lost.
    result = await fetch_key_quantile_bounds("events", "id", 4, sample_rows=40, key_is_integer=True)
    assert result is not None
    mins, overall_max = result
    assert mins[0] == 1                          # anchored to the true minimum
    assert overall_max == 6000                   # anchored to the true maximum


async def test_balanced_planning_with_sampling_covers_all_rows(skew_db):
    from db.executor import execute_split_query
    from planner.split_planner import build_split_query

    n = 4
    table = _table(split_strategy="range", split_balance="count", split_sample_rows=40)
    snap = _snapshot(table, n)
    assert await plan_ranges_for_snapshot(snap) is True
    seen: list[int] = []
    for i, split in enumerate(snap.splits):
        sql, params = build_split_query(split)
        rows = await execute_split_query(sql, params, split_index=i)
        seen += [r["id"] for r in rows]
    assert len(seen) == 306                       # every row served exactly once
    assert min(seen) == 1 and max(seen) == 6000   # edge rows preserved despite sampling

