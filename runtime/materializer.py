"""On-demand table materialization for ``MATERIALIZE_MODE=lazy``.

Extracts the per-table generate + pin logic so a snapshot's split Parquet bytes
(and their declared sizes) are produced on the first metadata read instead of
eagerly at startup. This is the single-agent path: this agent owns every split.

Correctness: the Iceberg manifest declares each split's ``file_size_in_bytes``.
A browse (ListObjectsV2/HEAD) before materialization can build a manifest with
placeholder sizes and memoize it on the snapshot. After materializing we clear
those memoized bytes so the authoritative manifest is rebuilt with true sizes.
"""
from __future__ import annotations

import asyncio
import io

import pyarrow.parquet as pq

import config
import cache.lru_cache as cache
from db.executor import execute_split_query, stream_split_query
from parquet.generator import rows_to_parquet, stream_rows_to_parquet
from planner.split_planner import build_split_query
from iceberg.stats import collect_split_stats
from iceberg.state_store import SnapshotState
from observability.logging import get_logger

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


def _apply_bytes(split, data: bytes) -> int:
    split.file_size_in_bytes = len(data)
    split.record_count = pq.read_metadata(io.BytesIO(data)).num_rows
    if config.ICEBERG_MANIFEST_STATS:
        split.stats = collect_split_stats(data, split.table.schema)
    if config.PIN_MATERIALIZED_SPLITS:
        cache.pin_parquet(split.object_key, data)
    return split.record_count


async def _materialize_split(split) -> int:
    key = split.object_key
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
            pq_bytes, nrows = await stream_rows_to_parquet(
                batches, split_index=split.split_index, columns=split.table.schema
            )
        else:
            rows = await execute_split_query(
                sql, params, split_index=split.split_index,
                connection=split.table.connection_id,
            )
            pq_bytes = rows_to_parquet(
                rows, split_index=split.split_index, columns=split.table.schema
            )
            nrows = len(rows)
        if config.PIN_MATERIALIZED_SPLITS:
            cache.pin_parquet(key, pq_bytes)
        else:
            cache.put_parquet(key, pq_bytes)
        split.record_count = nrows
        split.file_size_in_bytes = len(pq_bytes)
        if config.ICEBERG_MANIFEST_STATS:
            split.stats = collect_split_stats(pq_bytes, split.table.schema)
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
        # Discard any placeholder-sized metadata/manifest bytes a pre-materialization
        # browse may have memoized, so they rebuild with the true sizes.
        snap.metadata_bytes = None
        snap.manifest_list_bytes = None
        snap.manifest_file_bytes = None
        log.info("lazy_materialized", table=snap.table.name,
                 total_records=snap.total_records, splits=len(snap.splits))
