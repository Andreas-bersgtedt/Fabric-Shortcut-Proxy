"""Per-table ``split_target_rows`` override tests.

A table may set ``split_target_rows`` to override the global default. The override
also raises that table's per-split row cap so a larger target is never truncated
by the smaller global ``query_max_rows``.
"""
from __future__ import annotations

import pytest

import config
from config import ColumnDef, TableDef, _tabledef_from_json
from iceberg.state_store import SnapshotState, SplitDescriptor
from planner.split_planner import build_split_query, choose_table_num_splits


class _Caps:
    supports_range_key_bounds = True


def _table(**overrides) -> TableDef:
    kwargs = dict(
        name="events",
        source_table="dbo.events",
        key_column="event_id",
        num_splits=8,
        schema=[ColumnDef(field_id=1, name="event_id", iceberg_type="long", nullable=False)],
    )
    kwargs.update(overrides)
    return TableDef(**kwargs)


def _split(table: TableDef) -> SplitDescriptor:
    base = "db/local/default/dbo/events"
    snap = SnapshotState(
        snapshot_id=1,
        sequence_number=1,
        watermark_ms=1_700_000_000_000,
        manifest_list_key=f"{base}/metadata/snap.avro",
        manifest_file_key=f"{base}/metadata/m1.avro",
        metadata_key=f"{base}/metadata/v1.metadata.json",
        version_hint_key=f"{base}/metadata/version-hint.text",
        table=table,
        table_path=base,
        legacy_table_path=base,
    )
    return SplitDescriptor(
        split_index=0,
        num_splits=table.num_splits,
        object_key=f"{base}/data/split-0.parquet",
        watermark_ms=snap.watermark_ms,
        table=table,
    )


def test_effective_target_falls_back_to_global(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    assert _table().effective_split_target_rows == 100_000


def test_effective_target_uses_override(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    assert _table(split_target_rows=1_000_000).effective_split_target_rows == 1_000_000


def test_effective_target_override_zero_disables(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    assert _table(split_target_rows=0).effective_split_target_rows == 0


def test_effective_max_rows_lifts_cap_for_large_target(monkeypatch):
    monkeypatch.setattr(config, "QUERY_MAX_ROWS", 500_000, raising=False)
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    # Global-default table: cap stays at the connection ceiling.
    assert _table().effective_max_rows == 500_000
    # Override above the cap: cap is raised to the target.
    assert _table(split_target_rows=1_000_000).effective_max_rows == 1_000_000
    # Override below the cap: connection ceiling is preserved (never lowered).
    assert _table(split_target_rows=50_000).effective_max_rows == 500_000


def test_build_split_query_uses_effective_max_rows(monkeypatch):
    monkeypatch.setattr(config, "QUERY_MAX_ROWS", 500_000, raising=False)
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    _, params = build_split_query(_split(_table(split_target_rows=1_000_000)))
    assert params["max_rows"] == 1_000_000


@pytest.mark.asyncio
async def test_choose_table_num_splits_honors_override(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)
    monkeypatch.setattr(config, "SPLIT_COUNT_MIN", 1, raising=False)
    monkeypatch.setattr(config, "SPLIT_COUNT_MAX", 64, raising=False)

    async def _fake_count(_source: str, connection: str = "default"):
        return 4_000_000

    monkeypatch.setattr("db.executor.fetch_table_row_count", _fake_count)

    # Global 100k -> ceil(4M / 100k) = 40 splits.
    assert await choose_table_num_splits(_table()) == 40
    # Per-table 1M -> ceil(4M / 1M) = 4 splits.
    assert await choose_table_num_splits(_table(split_target_rows=1_000_000)) == 4


@pytest.mark.asyncio
async def test_choose_table_num_splits_override_zero_keeps_configured(monkeypatch):
    monkeypatch.setattr(config, "SPLIT_TARGET_ROWS", 100_000, raising=False)

    async def _boom(*_a, **_k):  # must not be called when planning is disabled
        raise AssertionError("row count should not be fetched when target is 0")

    monkeypatch.setattr("db.executor.fetch_table_row_count", _boom)
    assert await choose_table_num_splits(_table(num_splits=8, split_target_rows=0)) == 8


def test_tabledef_from_json_parses_override():
    t = _tabledef_from_json({
        "name": "clickstream",
        "source_table": "dbo.clickstream",
        "key_column": "event_id",
        "split_target_rows": 1_000_000,
    })
    assert t.split_target_rows == 1_000_000


def test_tabledef_from_json_absent_override_is_none():
    t = _tabledef_from_json({"name": "orders", "source_table": "dbo.orders"})
    assert t.split_target_rows is None
