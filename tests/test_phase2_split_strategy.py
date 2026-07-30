"""Phase 2 split-strategy tests: date/auto planning + non-PK fallback SQL."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

import config
from config import ColumnDef, TableDef
from iceberg.state_store import SnapshotState, SplitDescriptor
from planner.split_planner import (
    build_split_query,
    choose_table_num_splits,
    compute_split_count,
    plan_ranges_for_snapshot,
)


class _Caps:
    supports_range_key_bounds = True


def _snapshot(table: TableDef, splits: int = 4) -> SnapshotState:
    base = "db/local/default/public/test_obj"
    snap = SnapshotState(
        snapshot_id=1,
        sequence_number=1,
        watermark_ms=1_700_000_000_000,
        manifest_list_key=f"{base}/metadata/snap-1-1-x.avro",
        manifest_file_key=f"{base}/metadata/x-m1.avro",
        metadata_key=f"{base}/metadata/v1.metadata.json",
        version_hint_key=f"{base}/metadata/version-hint.text",
        table=table,
        table_path=base,
        legacy_table_path=base,
    )
    snap.splits = [
        SplitDescriptor(
            split_index=i,
            num_splits=splits,
            object_key=f"{base}/data/split-{i}.parquet",
            watermark_ms=snap.watermark_ms,
            table=table,
        )
        for i in range(splits)
    ]
    return snap


def test_build_split_query_non_integer_key_uses_row_number_strategy():
    table = TableDef(
        name="cust",
        source_table="public.customers",
        key_column="customer_name",
        num_splits=4,
        schema=[
            ColumnDef(field_id=1, name="customer_name", iceberg_type="string", nullable=False),
            ColumnDef(field_id=2, name="amount", iceberg_type="long", nullable=True),
        ],
    )
    split = _snapshot(table).splits[0]
    sql, params = build_split_query(split)
    assert "ROW_NUMBER() OVER" in sql
    assert "num_splits" in params and "split_index" in params


@pytest.mark.asyncio
async def test_plan_ranges_date_strategy_assigns_date_bounds(monkeypatch):
    table = TableDef(
        name="orders",
        source_table="public.orders",
        key_column="order_date",
        num_splits=4,
        schema=[
            ColumnDef(field_id=1, name="order_id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="order_date", iceberg_type="date", nullable=False),
        ],
    )
    snap = _snapshot(table)

    monkeypatch.setattr(config, "SPLIT_STRATEGY", "date", raising=False)
    monkeypatch.setattr("planner.split_planner.capabilities_for_db_url", lambda _u: _Caps())

    async def _fake_bounds(_table: str, _col: str, connection: str = "default"):
        return date(2024, 1, 1), date(2024, 1, 13)

    monkeypatch.setattr("db.executor.fetch_column_bounds", _fake_bounds)

    ok = await plan_ranges_for_snapshot(snap)
    assert ok is True
    assert all(s.split_key_column == "order_date" for s in snap.splits)
    assert all(isinstance(s.key_lo, date) and isinstance(s.key_hi, date) for s in snap.splits)


@pytest.mark.asyncio
async def test_plan_ranges_auto_prefers_temporal_when_no_integer(monkeypatch):
    table = TableDef(
        name="events",
        source_table="public.events",
        key_column=None,
        num_splits=3,
        schema=[
            ColumnDef(field_id=1, name="event_ts", iceberg_type="timestamp", nullable=False),
            ColumnDef(field_id=2, name="payload", iceberg_type="string", nullable=True),
        ],
    )
    snap = _snapshot(table, splits=3)

    monkeypatch.setattr(config, "SPLIT_STRATEGY", "auto", raising=False)
    monkeypatch.setattr("planner.split_planner.capabilities_for_db_url", lambda _u: _Caps())

    async def _fake_temporal_bounds(_table: str, _col: str, connection: str = "default"):
        return (
            datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 0, 0, 12, tzinfo=timezone.utc),
        )

    monkeypatch.setattr("db.executor.fetch_column_bounds", _fake_temporal_bounds)

    ok = await plan_ranges_for_snapshot(snap)
    assert ok is True
    assert all(s.split_key_column == "event_ts" for s in snap.splits)
    assert all(isinstance(s.key_lo, datetime) and isinstance(s.key_hi, datetime) for s in snap.splits)


@pytest.mark.asyncio
async def test_plan_ranges_auto_falls_back_for_string_only(monkeypatch):
    table = TableDef(
        name="tags",
        source_table="public.tags",
        key_column=None,
        num_splits=2,
        schema=[ColumnDef(field_id=1, name="tag", iceberg_type="string", nullable=False)],
    )
    snap = _snapshot(table, splits=2)

    monkeypatch.setattr(config, "SPLIT_STRATEGY", "auto", raising=False)
    monkeypatch.setattr("planner.split_planner.capabilities_for_db_url", lambda _u: _Caps())

    ok = await plan_ranges_for_snapshot(snap)
    assert ok is False
    assert all(s.key_lo is None and s.key_hi is None for s in snap.splits)


@pytest.mark.asyncio
async def test_plan_ranges_promotes_modulo_when_row_target_enabled(monkeypatch):
    table = TableDef(
        name="sales",
        source_table="dbo.sales",
        key_column="id",
        num_splits=4,
        schema=[
            ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="amount", iceberg_type="long", nullable=True),
        ],
    )
    snap = _snapshot(table, splits=4)

    monkeypatch.setattr(config, "SPLIT_STRATEGY", "modulo", raising=False)
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    monkeypatch.setattr("planner.split_planner.capabilities_for_db_url", lambda _u: _Caps())

    async def _fake_bounds(_table: str, _col: str, connection: str = "default"):
        return 1, 400_000

    monkeypatch.setattr("db.executor.fetch_key_bounds", _fake_bounds)

    ok = await plan_ranges_for_snapshot(snap)
    assert ok is True
    assert all(s.key_lo is not None and s.key_hi is not None for s in snap.splits)


def test_compute_split_count_guardrails():
    assert compute_split_count(
        estimated_rows=1_000_000,
        target_rows=100_000,
        min_splits=1,
        max_splits=256,
        default_splits=8,
    ) == 10
    assert compute_split_count(
        estimated_rows=50,
        target_rows=100_000,
        min_splits=4,
        max_splits=256,
        default_splits=8,
    ) == 4
    assert compute_split_count(
        estimated_rows=10_000_000,
        target_rows=1,
        min_splits=1,
        max_splits=32,
        default_splits=8,
    ) == 32
    assert compute_split_count(
        estimated_rows=None,
        target_rows=100_000,
        min_splits=1,
        max_splits=16,
        default_splits=8,
    ) == 8


@pytest.mark.asyncio
async def test_choose_table_num_splits_uses_row_target(monkeypatch):
    table = TableDef(
        name="sales",
        source_table="dbo.sales",
        num_splits=8,
        schema=[ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False)],
    )
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    monkeypatch.setattr(config, "SPLIT_COUNT_MIN", 1, raising=False)
    monkeypatch.setattr(config, "SPLIT_COUNT_MAX", 64, raising=False)

    async def _fake_count(_source: str, connection: str = "default"):
        return 1_200_000

    monkeypatch.setattr("db.executor.fetch_table_row_count", _fake_count)
    chosen = await choose_table_num_splits(table)
    assert chosen == 12
