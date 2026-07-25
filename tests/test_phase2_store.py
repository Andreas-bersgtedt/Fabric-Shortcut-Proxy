"""Phase 2: artifact-store serving tier (durable, restart-safe Parquet)."""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("S3_BUCKET", "test-bucket")

import pytest

import config
import cache.lru_cache as cache
from runtime.artifact_store import MemoryStore, set_default_store, reset_default_store

KEY = "warehouse/db/sales/data/split-0-deadbeef.parquet"
BODY = b"PAR1-phase2-body-" + bytes(range(64)) + b"-PAR1"


@pytest.fixture
def store(monkeypatch):
    """Serving ON, backed by an in-memory store; caches cleared for isolation."""
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", True, raising=False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False, raising=False)
    mem = MemoryStore()
    set_default_store(mem)
    cache._pinned.clear()
    cache._parquet_cache._store.clear()
    cache._parquet_cache._current_bytes = 0
    try:
        yield mem
    finally:
        cache._pinned.clear()
        cache._parquet_cache._store.clear()
        cache._parquet_cache._current_bytes = 0
        reset_default_store()


def test_pin_writes_through_to_store(store):
    cache.pin_parquet(KEY, BODY)
    assert store.get(KEY) == BODY            # durable copy in the store


def test_put_writes_through_to_store(store):
    cache.put_parquet(KEY, BODY)
    assert store.get(KEY) == BODY


def test_get_reads_through_from_store_on_cold_miss(store):
    # Simulate a restart: bytes only in the store, memory empty.
    store.put(KEY, BODY)
    assert cache._pinned.get(KEY) is None
    got = cache.get_parquet(KEY)
    assert got == BODY
    # promoted to memory for subsequent fast hits
    assert cache._parquet_cache.get(KEY) == BODY


def test_warm_parquet_restores_from_store_zero_regen(store):
    # This is what startup materialization uses to skip SQL regeneration.
    store.put(KEY, BODY)
    warm = cache.warm_parquet(KEY)
    assert warm == BODY


def test_peek_reads_store_for_sizing(store):
    store.put(KEY, BODY)
    assert cache.peek_parquet(KEY) == BODY   # size == len(BODY)


def test_evict_removes_from_store(store):
    cache.pin_parquet(KEY, BODY)
    assert store.exists(KEY)
    cache.evict_parquet(KEY)
    assert not store.exists(KEY)


def test_stats_reports_serving_flag(store):
    assert cache.stats()["artifact_store_serving"] is True


def test_serving_off_does_not_touch_store(monkeypatch):
    # Default (flag off): no write-through, no read-through — known-good path.
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", False, raising=False)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False, raising=False)
    mem = MemoryStore()
    set_default_store(mem)
    cache._pinned.clear()
    cache._parquet_cache._store.clear()
    cache._parquet_cache._current_bytes = 0
    try:
        cache.pin_parquet(KEY, BODY)
        assert not mem.exists(KEY)                       # nothing written
        cache._pinned.clear()
        assert cache.get_parquet(KEY) is None            # nothing read back
    finally:
        cache._pinned.clear()
        cache._parquet_cache._store.clear()
        reset_default_store()
