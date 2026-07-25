"""Tests for pinned (immutable) snapshot Parquet splits — the READ_EXCEPTION /
BlobNotFound size-drift guard."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest

import config
import cache.lru_cache as cache


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False, raising=False)
    cache.unpin_all()
    cache._parquet_cache._store.clear()
    cache._parquet_cache._current_bytes = 0
    yield
    cache.unpin_all()
    cache._parquet_cache._store.clear()
    cache._parquet_cache._current_bytes = 0


def test_pinned_served_verbatim():
    key = "warehouse/db/Product/data/split-0-abc.parquet"
    data = b"PARQUET-AUTHORITATIVE-BYTES"
    cache.pin_parquet(key, data)
    assert cache.get_parquet(key) == data
    assert cache.peek_parquet(key) == data


def test_pinned_survives_lru_pressure():
    key = "warehouse/db/Product/data/split-0-abc.parquet"
    data = b"X" * 1000
    cache.pin_parquet(key, data)
    # Flood the ordinary LRU well past its cap to force eviction of everything.
    for i in range(50):
        cache.put_parquet(f"warehouse/db/Other/data/split-{i}.parquet", b"Y" * (10 * 1024 * 1024))
    # Pinned split is unaffected — still byte-identical.
    assert cache.get_parquet(key) == data
    assert len(cache.peek_parquet(key)) == 1000


def test_evict_parquet_drops_pin():
    key = "warehouse/db/Product/data/split-0-abc.parquet"
    cache.pin_parquet(key, b"data")
    cache.evict_parquet(key)
    assert cache.get_parquet(key) is None


def test_stats_reports_pinned():
    cache.pin_parquet("warehouse/db/T/data/split-0.parquet", b"abcde")
    s = cache.stats()
    assert s["parquet_pinned"]["entries"] == 1
    assert s["parquet_pinned"]["bytes"] == 5
