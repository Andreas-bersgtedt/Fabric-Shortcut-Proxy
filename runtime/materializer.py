"""On-demand table materialization for ``MATERIALIZE_MODE=lazy``.

Extracts the per-table generate + pin logic so a snapshot's split Parquet bytes
(and their declared sizes) are produced on the first metadata read instead of
eagerly at startup.

Correctness: the Iceberg manifest (and the Delta ``add`` action) declares each
split's ``file_size_in_bytes``. A browse (ListObjectsV2/HEAD) before materialization
can build metadata with placeholder sizes and memoize it. After materializing we
clear those memoized bytes so the authoritative metadata is rebuilt with true sizes.

Cluster: with more than one shard a split is owned by exactly one agent
(``split_index % shard_count``). A non-owner waits for the owning shard to publish
the split to the shared artifact store rather than regenerating it, so every agent
serves byte-identical splits (multi-shard lazy requires ``ARTIFACT_STORE_SERVING``).
"""
from __future__ import annotations

import asyncio
import hashlib
import io

import pyarrow.parquet as pq
import pyarrow as pa

import config
import cache.lru_cache as cache
from db.executor import execute_split_query, stream_split_query
from parquet.generator import rows_to_parquet, stream_rows_to_parquet
from planner.split_planner import arrow_fallback_columns, build_split_query
from iceberg.stats import collect_split_stats
from iceberg.state_store import SnapshotState
from observability.logging import get_logger
from runtime.split_completion import (
    apply_split_completion,
    publish_split_completion,
    read_split_completion,
)

log = get_logger(__name__)

_sem = asyncio.Semaphore(config.MAX_CONCURRENT_GENERATIONS)
_locks: dict[str, asyncio.Lock] = {}


def _lock_for(name: str) -> asyncio.Lock:
    lock = _locks.get(name)
    if lock is None:
        lock = asyncio.Lock()
        _locks[name] = lock
    return lock


def _is_materialized(snap: SnapshotState) -> bool:
    return bool(snap.splits) and all(s.file_size_in_bytes is not None for s in snap.splits)


def _owns_split(split) -> bool:
    """Which agent generates a split. Single shard owns everything; otherwise a
    stable modulo assignment so exactly one shard generates each split."""
    n = config.AGENT_SHARD_COUNT
    if n <= 1:
        return True
    return split.split_index % n == config.AGENT_SHARD_INDEX


def _should_pin() -> bool:
    """Virtual mode never persists: splits stay in the evictable LRU and are
    regenerated (byte-identically) on demand, so zero bytes are pinned at rest."""
    return config.PIN_MATERIALIZED_SPLITS and config.MATERIALIZE_MODE != "virtual"


async def _wait_for_completion(split):
    """Poll for the owning shard's small durable completion record."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + config.MATERIALIZE_WAIT_SECONDS
    while loop.time() < deadline:
        await asyncio.sleep(0.25)
        completion = read_split_completion(split)
        if completion is not None:
            return completion
    return None


def _apply_bytes(split, data: bytes) -> int:
    split.file_size_in_bytes = len(data)
    split.record_count = pq.read_metadata(io.BytesIO(data)).num_rows
    if config.ICEBERG_MANIFEST_STATS:
        split.stats = collect_split_stats(data, split.table.schema)
    if config.AGENT_SHARD_COUNT > 1 and config.ARTIFACT_STORE_SERVING:
        publish_split_completion(split, data)
    if _should_pin():
        cache.pin_parquet(split.object_key, data)
    return split.record_count


def _apply_arrow_fallback(rows: list[dict], split) -> list[dict]:
    """Apply only explicitly selected Arrow fallback transforms to SQL rows."""
    if not rows:
        return rows
    fallback = [column for column in arrow_fallback_columns(split) if column.transform]
    if not fallback:
        return rows
    original = split.table.schema
    native_passthrough = [column for column in original if not column.transform]
    columns = [*native_passthrough, *fallback]
    from storage.tokenizer import tokenize_batch
    batch = pa.RecordBatch.from_pylist(rows)
    return tokenize_batch(batch, columns).to_pylist()


async def _materialize_split(split) -> int:
    key = split.object_key
    if not _owns_split(split) and config.ARTIFACT_STORE_SERVING:
        completion = await _wait_for_completion(split)
        if completion is None:
            raise TimeoutError(
                f"timed out waiting for owner completion: table={split.table.name} "
                f"split={split.split_index}"
            )
        return apply_split_completion(split, completion)
    warm = cache.warm_parquet(key)
    if warm is not None:
        return _apply_bytes(split, warm)
    async with _sem:
        warm = cache.warm_parquet(key)   # another waiter may have won the race
        if warm is not None:
            return _apply_bytes(split, warm)
        sql, params = build_split_query(split)
        if config.STREAMING_PARQUET:
            batches = stream_split_query(
                sql, params, split_index=split.split_index,
                batch_rows=config.STREAM_BATCH_ROWS,
                connection=split.table.connection_id,
            )
            async def transformed_batches():
                async for batch in batches:
                    yield _apply_arrow_fallback(batch, split)

            pq_bytes, nrows = await stream_rows_to_parquet(
                transformed_batches(), split_index=split.split_index, columns=split.table.schema
            )
        else:
            rows = await execute_split_query(
                sql, params, split_index=split.split_index,
                connection=split.table.connection_id,
            )
            pq_bytes = rows_to_parquet(
                _apply_arrow_fallback(rows, split), split_index=split.split_index,
                columns=split.table.schema
            )
            nrows = len(rows)
        split.record_count = nrows
        split.file_size_in_bytes = len(pq_bytes)
        if config.ICEBERG_MANIFEST_STATS:
            split.stats = collect_split_stats(pq_bytes, split.table.schema)
        if config.AGENT_SHARD_COUNT > 1 and config.ARTIFACT_STORE_SERVING:
            publish_split_completion(split, pq_bytes)
        if _should_pin():
            cache.pin_parquet(key, pq_bytes)
        else:
            cache.put_parquet(key, pq_bytes)
        return nrows


async def ensure_snapshot_materialized(snap: SnapshotState) -> None:
    """Materialize + pin every split of ``snap`` exactly once. Idempotent.

    Cheap no-op once the snapshot is materialized. Serializes concurrent first
    requests for the same table with a per-table lock.
    """
    if _is_materialized(snap):
        return
    async with _lock_for(snap.table.name):
        if _is_materialized(snap):
            return
        if config.CONCURRENT_STARTUP_MATERIALIZATION:
            counts = await asyncio.gather(*(_materialize_split(s) for s in snap.splits))
        else:
            counts = [await _materialize_split(s) for s in snap.splits]
        snap.total_records = sum(counts)
        # Discard any placeholder-sized metadata a pre-materialization browse may
        # have memoized, so it rebuilds with the true sizes.
        snap.metadata_bytes = None
        snap.manifest_list_bytes = None
        snap.manifest_file_bytes = None
        if config.TABLE_FORMAT == "delta":
            from delta import log as delta_log
            delta_log.invalidate_table(snap.table.name)
        if config.MATERIALIZE_MODE == "virtual" and snap.splits:
            await _verify_determinism(snap.splits[0])
        log.info("deferred_materialized", mode=config.MATERIALIZE_MODE, table=snap.table.name,
                 total_records=snap.total_records, splits=len(snap.splits))


async def _verify_determinism(split) -> None:
    """Virtual mode serves by regenerating on demand, so a split MUST reproduce
    byte-identically. Regenerate one split and compare; fail closed on drift (a
    non-deterministic encoder, or a source that mutated between reads)."""
    first = cache.peek_parquet(split.object_key)
    if first is None:
        return
    sql, params = build_split_query(split)
    if config.STREAMING_PARQUET:
        batches = stream_split_query(
            sql, params, split_index=split.split_index,
            batch_rows=config.STREAM_BATCH_ROWS,
            connection=split.table.connection_id,
        )

        async def transformed_batches():
            async for batch in batches:
                yield _apply_arrow_fallback(batch, split)

        second, _ = await stream_rows_to_parquet(
            transformed_batches(), split_index=split.split_index, columns=split.table.schema
        )
    else:
        rows = await execute_split_query(
            sql, params, split_index=split.split_index,
            connection=split.table.connection_id,
        )
        second = rows_to_parquet(
            _apply_arrow_fallback(rows, split), split_index=split.split_index,
            columns=split.table.schema
        )
    if hashlib.sha256(first).digest() != hashlib.sha256(second).digest():
        raise ValueError(
            f"MATERIALIZE_MODE 'virtual' requires byte-deterministic regeneration, but "
            f"split {split.split_index} of table {split.table.name!r} regenerated "
            "differently. Use a snapshot-isolated / immutable source and a pinned "
            "PyArrow version, or switch to MATERIALIZE_MODE=lazy."
        )
