"""
Phase 5 tests:
  - F5  persistent disk Parquet cache (write-through + warm read on restart)
  - F3  Iceberg value encoding + per-column stats collection + manifest stat maps
"""
from __future__ import annotations

import io
import os
import pathlib
import struct
import datetime

import fastavro

_DB = pathlib.Path(__file__).parent / "test_phase5.db"
os.environ["DB_URL"] = f"sqlite+aiosqlite:///{_DB.as_posix()}"

import config
from iceberg.stats import encode_bound, collect_split_stats
from parquet.generator import rows_to_parquet
from iceberg import manifest as mf
from iceberg.state_store import build_snapshot


_ROWS = [
    {"id": 1, "order_date": "2020-01-01", "customer_id": 10, "product": "A",
     "quantity": 2, "unit_price": 5.0, "total": 10.0, "region": "west"},
    {"id": 5, "order_date": "2020-06-01", "customer_id": 20, "product": "Z",
     "quantity": 3, "unit_price": 4.0, "total": 12.0, "region": "east"},
]


# ---------------------------------------------------------------------------
# F5 — persistent disk cache
# ---------------------------------------------------------------------------

def test_disk_cache_roundtrip(tmp_path, monkeypatch):
    import cache.lru_cache as lru

    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", True)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE_DIR", str(tmp_path))

    key = "warehouse/db/sales/data/split-0-abc.parquet"
    data = b"PAR1-pretend-parquet-bytes"
    lru.put_parquet(key, data)

    # A file should exist on disk.
    assert any(tmp_path.iterdir())

    def _clear_mem():
        lru._parquet_cache._store.clear()
        lru._parquet_cache._current_bytes = 0

    # Simulate a restart: memory is empty, disk still has the object.
    _clear_mem()
    assert lru.warm_parquet(key) == data     # promotes to memory
    _clear_mem()
    assert lru.get_parquet(key) == data       # falls through to disk
    _clear_mem()
    assert lru.peek_parquet(key) == data       # sizing lookup, no metrics


def test_disk_cache_disabled_writes_nothing(tmp_path, monkeypatch):
    import cache.lru_cache as lru

    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE_DIR", str(tmp_path))
    lru.put_parquet("k/x.parquet", b"data")
    assert not any(tmp_path.iterdir())
    assert lru.warm_parquet("k/x.parquet") is None


# ---------------------------------------------------------------------------
# F3 — Iceberg single-value binary encoding
# ---------------------------------------------------------------------------

def test_encode_bound_primitives():
    assert encode_bound("int", 5) == struct.pack("<i", 5)
    assert encode_bound("long", 5) == struct.pack("<q", 5)
    assert encode_bound("double", 1.5) == struct.pack("<d", 1.5)
    assert encode_bound("float", 1.5) == struct.pack("<f", 1.5)
    assert encode_bound("string", "abc") == b"abc"
    assert encode_bound("boolean", True) == b"\x01"
    assert encode_bound("boolean", False) == b"\x00"
    assert encode_bound("date", datetime.date(1970, 1, 2)) == struct.pack("<i", 1)
    assert encode_bound("long", None) is None


def test_collect_split_stats():
    pq_bytes = rows_to_parquet(_ROWS, split_index=0)
    stats = collect_split_stats(pq_bytes, config.TABLE_SCHEMA)
    # id is field_id 1
    id_stats = stats[1]
    assert id_stats.value_count == 2
    assert id_stats.null_count == 0
    assert struct.unpack("<q", id_stats.lower)[0] == 1
    assert struct.unpack("<q", id_stats.upper)[0] == 5
    assert id_stats.column_size > 0


def test_collect_split_stats_timestamptz_never_crashes():
    """Reading a tz-aware timestamp stat's Python value needs tzdata, which may
    be absent (Windows). Stats are optional — collection must NOT raise; the
    column's bounds are simply skipped when they can't be read."""
    from config import ColumnDef
    cols = [
        ColumnDef(field_id=1, name="id", iceberg_type="long", nullable=False),
        ColumnDef(field_id=2, name="ts", iceberg_type="timestamptz", nullable=True),
    ]
    rows = [
        {"id": 1, "ts": datetime.datetime(2020, 1, 2, 3, 4, 5)},
        {"id": 2, "ts": datetime.datetime(2021, 6, 7, 8, 9, 10)},
    ]
    pq_bytes = rows_to_parquet(rows, split_index=0, columns=cols)
    stats = collect_split_stats(pq_bytes, cols)  # must not raise
    assert stats[1].value_count == 2
    assert 2 in stats  # ts column still produces a stats entry (bounds may be None)


# ---------------------------------------------------------------------------
# F3 — manifest stat maps round-trip through fastavro
# ---------------------------------------------------------------------------

def test_manifest_with_stats_roundtrip(monkeypatch):
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", True)

    pq_bytes = rows_to_parquet(_ROWS, split_index=0)
    stats = collect_split_stats(pq_bytes, config.TABLE_SCHEMA)

    snap = build_snapshot("sales", 1, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
    snap.manifest_file_bytes = None
    for s in snap.splits:
        s.record_count = len(_ROWS)
        s.file_size_in_bytes = len(pq_bytes)
        s.stats = stats

    data = mf.build_manifest_file(snap)
    records = list(fastavro.reader(io.BytesIO(data)))
    assert records, "expected at least one manifest entry"
    df = records[0]["data_file"]

    value_counts = {kv["key"]: kv["value"] for kv in df["value_counts"]}
    null_counts = {kv["key"]: kv["value"] for kv in df["null_value_counts"]}
    lower = {kv["key"]: kv["value"] for kv in df["lower_bounds"]}
    upper = {kv["key"]: kv["value"] for kv in df["upper_bounds"]}

    assert value_counts[1] == 2          # id column, 2 rows
    assert null_counts[1] == 0
    assert struct.unpack("<q", lower[1])[0] == 1   # min id
    assert struct.unpack("<q", upper[1])[0] == 5   # max id


def test_manifest_without_stats_has_no_stat_maps(monkeypatch):
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", False)
    snap = build_snapshot("sales", 1, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
    snap.manifest_file_bytes = None
    for s in snap.splits:
        s.record_count = 6250
        s.file_size_in_bytes = 1000
    data = mf.build_manifest_file(snap)
    records = list(fastavro.reader(io.BytesIO(data)))
    assert "value_counts" not in records[0]["data_file"]


# ---------------------------------------------------------------------------
# F2 — snapshot history & time-travel
# ---------------------------------------------------------------------------

def test_snapshot_history_and_time_travel(monkeypatch):
    import json
    from iceberg.state_store import (
        build_snapshot as _bs, advance_table_snapshot, get_snapshot_history,
    )
    from iceberg.metadata import build_metadata_json
    import s3.router as router

    monkeypatch.setattr(config, "ICEBERG_SNAPSHOT_HISTORY", True)

    v1 = _bs("sales", 2, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
    v1.total_records = 100
    v2 = advance_table_snapshot("sales", config.BUCKET_NAME, config.WAREHOUSE_PREFIX)

    assert [s.version for s in get_snapshot_history("sales")] == [1, 2]
    assert v2.snapshot_id != v1.snapshot_id
    assert v2.sequence_number == v1.sequence_number + 1
    assert v2.watermark_ms > v1.watermark_ms
    # Data files are shared across versions (deterministic keys).
    assert [s.object_key for s in v2.splits] == [s.object_key for s in v1.splits]

    # v2 metadata: both snapshots, current = v2, one metadata-log entry (-> v1).
    m2 = json.loads(build_metadata_json(v2))
    assert len(m2["snapshots"]) == 2
    assert m2["current-snapshot-id"] == v2.snapshot_id
    assert len(m2["snapshot-log"]) == 2
    assert len(m2["metadata-log"]) == 1
    assert m2["metadata-log"][0]["metadata-file"].endswith("/v1.metadata.json")

    # v1 metadata is point-in-time: only snapshot 1, empty metadata-log.
    m1 = json.loads(build_metadata_json(v1))
    assert len(m1["snapshots"]) == 1
    assert m1["current-snapshot-id"] == v1.snapshot_id
    assert m1["metadata-log"] == []

    # version-hint.text reflects the current (highest) version.
    assert router._version_hint_bytes(v1.version_hint_key) == b"2"

    # Reset global snapshot state so later modules start clean.
    _bs("sales", config.NUM_SPLITS, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
