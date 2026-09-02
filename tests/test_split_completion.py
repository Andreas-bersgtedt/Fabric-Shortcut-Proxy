from __future__ import annotations

from types import SimpleNamespace

import pytest

import cache.lru_cache as cache
import config
from iceberg.stats import ColumnStats
from runtime.artifact_store import MemoryStore, reset_default_store, set_default_store
from runtime.split_completion import publish_split_completion, read_split_completion


@pytest.fixture
def shared_store(monkeypatch):
    monkeypatch.setattr(config, "ARTIFACT_STORE_SERVING", True)
    monkeypatch.setattr(config, "PARQUET_DISK_CACHE", False)
    store = MemoryStore()
    set_default_store(store)
    cache._pinned.clear()
    cache._parquet_cache._store.clear()
    cache._parquet_cache._current_bytes = 0
    try:
        yield store
    finally:
        cache._pinned.clear()
        cache._parquet_cache._store.clear()
        cache._parquet_cache._current_bytes = 0
        reset_default_store()


def _split(index: int = 1):
    return SimpleNamespace(
        object_key=f"warehouse/db/sales/data/split-{index}-test.parquet",
        split_index=index,
        table=SimpleNamespace(name="sales"),
        file_size_in_bytes=None,
        record_count=None,
        stats={},
    )


def test_completion_round_trips_split_metadata(shared_store):
    split = _split()
    data = b"owner-parquet-bytes"
    split.file_size_in_bytes = len(data)
    split.record_count = 7
    split.stats = {
        3: ColumnStats(3, 12, 7, 1, b"a", b"z"),
    }

    published = publish_split_completion(split, data)
    loaded = read_split_completion(split)

    assert loaded == published
    assert shared_store.get(split.object_key) == data


@pytest.mark.asyncio
async def test_non_owner_uses_completion_without_loading_or_pinning_parquet(
    shared_store, monkeypatch
):
    import runtime.materializer as materializer

    split = _split(index=1)
    data = b"owner-parquet-bytes"
    split.file_size_in_bytes = len(data)
    split.record_count = 11
    publish_split_completion(split, data)
    split.file_size_in_bytes = None
    split.record_count = None

    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "MATERIALIZE_WAIT_SECONDS", 1)
    monkeypatch.setattr(
        cache,
        "warm_parquet",
        lambda key: pytest.fail(f"non-owner loaded Parquet bytes: {key}"),
    )
    monkeypatch.setattr(
        cache,
        "pin_parquet",
        lambda key, data: pytest.fail(f"non-owner pinned Parquet bytes: {key}"),
    )

    count = await materializer._materialize_split(split)

    assert count == 11
    assert split.file_size_in_bytes == len(data)
    assert cache._pinned == {}


@pytest.mark.asyncio
async def test_non_owner_timeout_never_falls_back_to_sql(shared_store, monkeypatch):
    import runtime.materializer as materializer

    split = _split(index=1)
    sql_called = False

    async def unexpected_sql(*args, **kwargs):
        nonlocal sql_called
        sql_called = True
        return []

    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "MATERIALIZE_WAIT_SECONDS", 0)
    monkeypatch.setattr(materializer, "execute_split_query", unexpected_sql)

    with pytest.raises(TimeoutError, match="waiting for owner completion"):
        await materializer._materialize_split(split)

    assert sql_called is False


@pytest.mark.asyncio
async def test_owner_backfills_completion_for_existing_durable_split(shared_store, monkeypatch):
    import runtime.materializer as materializer

    split = _split(index=0)
    parquet = pytest.importorskip("pyarrow")
    table = parquet.table({"id": [1, 2, 3]})
    sink = parquet.BufferOutputStream()
    parquet.parquet.write_table(table, sink)
    data = sink.getvalue().to_pybytes()
    shared_store.put(split.object_key, data)

    monkeypatch.setattr(config, "AGENT_SHARD_COUNT", 2)
    monkeypatch.setattr(config, "AGENT_SHARD_INDEX", 0)
    monkeypatch.setattr(config, "ICEBERG_MANIFEST_STATS", False)
    monkeypatch.setattr(config, "PIN_MATERIALIZED_SPLITS", False)

    count = await materializer._materialize_split(split)

    assert count == 3
    assert read_split_completion(split) is not None