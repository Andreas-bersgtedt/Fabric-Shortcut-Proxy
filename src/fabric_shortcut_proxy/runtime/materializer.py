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

from fabric_shortcut_proxy import config
import fabric_shortcut_proxy.cache.lru_cache as cache
from fabric_shortcut_proxy.db.executor import execute_split_query, stream_split_query
from fabric_shortcut_proxy.parquet.generator import rows_to_parquet, stream_rows_to_parquet
from fabric_shortcut_proxy.planner.split_planner import arrow_fallback_columns, build_split_query
from fabric_shortcut_proxy.iceberg.stats import collect_split_stats
from fabric_shortcut_proxy.iceberg.state_store import SnapshotState
from fabric_shortcut_proxy.observability.logging import get_logger
from fabric_shortcut_proxy.observability.tokenization import record_arrow_fallback
from fabric_shortcut_proxy.runtime.artifact_store import get_default_store
from fabric_shortcut_proxy.runtime.split_completion import (
    apply_split_completion,
    publish_split_completion,
    read_split_completion,
)
from fabric_shortcut_proxy.runtime.generation import GenerationError, join_generation

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
    if (
        config.GENERATION_SOURCE_CONSISTENCY == "snapshot"
        and not bool(getattr(split, "generation_distributed_snapshot", False))
    ):
        return int(
            getattr(split, "generation_owner_shard", 0)
        ) == config.AGENT_SHARD_INDEX
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


def _apply_bytes(split, data: bytes, *, publish_completion: bool = False) -> int:
    split.file_size_in_bytes = len(data)
    split.record_count = pq.read_metadata(io.BytesIO(data)).num_rows
    split.content_hash = hashlib.sha256(data).hexdigest()
    split.s3_etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
    if config.ICEBERG_MANIFEST_STATS:
        split.stats = collect_split_stats(data, split.table.schema)
    if publish_completion or (
        config.AGENT_SHARD_COUNT > 1 and config.ARTIFACT_STORE_SERVING
    ):
        publish_split_completion(split, data)
    if _should_pin():
        cache.pin_parquet(split.object_key, data)
    return split.record_count


def _apply_arrow_fallback(
    rows: list[dict], split, announced: set[tuple[str, str]] | None = None
) -> list[dict]:
    """Apply only explicitly selected Arrow fallback transforms to SQL rows."""
    if not rows:
        return rows
    columns = arrow_fallback_columns(split)
    if not any(column.transform for column in columns):
        return rows
    from fabric_shortcut_proxy.db.capabilities import capabilities_for_db_url

    flavor = capabilities_for_db_url(
        config.effective_db_url(split.table.connection_id)
    ).flavor
    announced = announced if announced is not None else set()
    for column in columns:
        if not column.transform:
            continue
        kind = column.transform.kind
        identity = (column.source_name, kind)
        if identity in announced:
            continue
        announced.add(identity)
        log.warning(
            "arrow_tokenization_fallback",
            table=split.table.name,
            split_index=split.split_index,
            flavor=flavor,
            column=column.source_name,
            token_kind=kind,
            plaintext_values_cross_proxy=True,
        )
        record_arrow_fallback(
            table=split.table.name,
            column=column.source_name,
            flavor=flavor,
            kind=kind,
        )
    from fabric_shortcut_proxy.storage.tokenizer import tokenize_batch
    batch = pa.RecordBatch.from_pylist(rows)
    return tokenize_batch(batch, columns).to_pylist()


async def _materialize_split_once(
    split,
    *,
    enforce_ownership: bool = True,
    publish_completion: bool = False,
) -> int:
    key = split.object_key
    if enforce_ownership and not _owns_split(split) and config.ARTIFACT_STORE_SERVING:
        completion = await _wait_for_completion(split)
        if completion is None:
            raise TimeoutError(
                f"timed out waiting for owner completion: table={split.table.name} "
                f"split={split.split_index}"
            )
        return apply_split_completion(split, completion)
    if publish_completion:
        completion = read_split_completion(split)
        if completion is not None:
            return apply_split_completion(split, completion)
    warm = cache.warm_parquet(key)
    if warm is not None:
        return _apply_bytes(split, warm, publish_completion=publish_completion)
    async with _sem:
        warm = cache.warm_parquet(key)   # another waiter may have won the race
        if warm is not None:
            return _apply_bytes(split, warm, publish_completion=publish_completion)
        sql, params = build_split_query(split)
        read_session = None
        if config.GENERATION_SOURCE_CONSISTENCY == "snapshot":
            from fabric_shortcut_proxy.runtime.snapshot_planning import read_session_for

            read_session = read_session_for(split.table)
            if read_session is None:
                raise RuntimeError(
                    f"snapshot read session is unavailable for {split.table.name!r}"
                )
        if config.STREAMING_PARQUET:
            announced_fallbacks: set[tuple[str, str]] = set()
            batches = (
                read_session.stream_split_query(
                    sql,
                    params,
                    split_index=split.split_index,
                    batch_rows=config.STREAM_BATCH_ROWS,
                )
                if read_session is not None
                else stream_split_query(
                    sql, params, split_index=split.split_index,
                    batch_rows=config.STREAM_BATCH_ROWS,
                    connection=split.table.connection_id,
                )
            )
            async def transformed_batches():
                async for batch in batches:
                    yield _apply_arrow_fallback(batch, split, announced_fallbacks)

            pq_bytes, nrows = await stream_rows_to_parquet(
                transformed_batches(), split_index=split.split_index, columns=split.table.schema
            )
        else:
            rows = (
                await read_session.execute_split_query(
                    sql, params, split_index=split.split_index
                )
                if read_session is not None
                else await execute_split_query(
                    sql, params, split_index=split.split_index,
                    connection=split.table.connection_id,
                )
            )
            pq_bytes = rows_to_parquet(
                _apply_arrow_fallback(rows, split, set()), split_index=split.split_index,
                columns=split.table.schema
            )
            nrows = len(rows)
        split.record_count = nrows
        split.file_size_in_bytes = len(pq_bytes)
        split.content_hash = hashlib.sha256(pq_bytes).hexdigest()
        split.s3_etag = hashlib.md5(pq_bytes, usedforsecurity=False).hexdigest()
        if config.ICEBERG_MANIFEST_STATS:
            split.stats = collect_split_stats(pq_bytes, split.table.schema)
        if publish_completion or (
            config.AGENT_SHARD_COUNT > 1 and config.ARTIFACT_STORE_SERVING
        ):
            publish_split_completion(split, pq_bytes)
        if _should_pin():
            cache.pin_parquet(key, pq_bytes)
        else:
            cache.put_parquet(key, pq_bytes)
        return nrows


async def _materialize_split(split) -> int:
    try:
        return await _materialize_split_once(split)
    except GenerationError:
        if config.AGENT_SHARD_COUNT <= 1 or not config.ARTIFACT_STORE_SERVING:
            raise
        context = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: join_generation(
                get_default_store(),
                config.AGENT_SHARD_COUNT,
                timeout_seconds=config.MATERIALIZE_WAIT_SECONDS,
            ),
        )
        split.generation_id = context.generation_id
        split.generation_fence = context.fence
        split.generation_token = context.lease_token
        split.generation_plan_sha256 = context.plan_sha256
        log.info(
            "worker_generation_rejoined",
            generation_id=context.generation_id,
            shard_index=config.AGENT_SHARD_INDEX,
        )
        return await _materialize_split_once(split)


async def materialize_queued_split(split) -> int:
    """Materialize a Manager-claimed split on the selected Agent."""
    return await _materialize_split_once(
        split,
        enforce_ownership=False,
        publish_completion=True,
    )


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
        try:
            if (
                config.CONCURRENT_STARTUP_MATERIALIZATION
                and config.GENERATION_SOURCE_CONSISTENCY != "snapshot"
            ):
                counts = await asyncio.gather(
                    *(_materialize_split(s) for s in snap.splits)
                )
            else:
                counts = [await _materialize_split(s) for s in snap.splits]
        except Exception:
            if config.GENERATION_SOURCE_CONSISTENCY == "snapshot":
                from fabric_shortcut_proxy.runtime.snapshot_planning import close_active_table

                await close_active_table(
                    snap.table, abort_reason="table materialization failed"
                )
            raise
        snap.total_records = sum(counts)
        # Discard any placeholder-sized metadata a pre-materialization browse may
        # have memoized, so it rebuilds with the true sizes.
        snap.metadata_bytes = None
        snap.manifest_list_bytes = None
        snap.manifest_file_bytes = None
        if config.TABLE_FORMAT == "delta":
            from fabric_shortcut_proxy.delta import log as delta_log
            delta_log.invalidate_table(snap.table.name)
        if config.MATERIALIZE_MODE == "virtual" and snap.splits:
            await _verify_determinism(snap.splits[0])
        if config.GENERATION_SOURCE_CONSISTENCY == "snapshot":
            from fabric_shortcut_proxy.runtime.snapshot_planning import close_active_table

            await close_active_table(snap.table)
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
        announced_fallbacks: set[tuple[str, str]] = set()
        batches = stream_split_query(
            sql, params, split_index=split.split_index,
            batch_rows=config.STREAM_BATCH_ROWS,
            connection=split.table.connection_id,
        )

        async def transformed_batches():
            async for batch in batches:
                yield _apply_arrow_fallback(batch, split, announced_fallbacks)

        second, _ = await stream_rows_to_parquet(
            transformed_batches(), split_index=split.split_index, columns=split.table.schema
        )
    else:
        rows = await execute_split_query(
            sql, params, split_index=split.split_index,
            connection=split.table.connection_id,
        )
        second = rows_to_parquet(
            _apply_arrow_fallback(rows, split, set()), split_index=split.split_index,
            columns=split.table.schema
        )
    if hashlib.sha256(first).digest() != hashlib.sha256(second).digest():
        raise ValueError(
            f"MATERIALIZE_MODE 'virtual' requires byte-deterministic regeneration, but "
            f"split {split.split_index} of table {split.table.name!r} regenerated "
            "differently. Use a snapshot-isolated / immutable source and a pinned "
            "PyArrow version, or switch to MATERIALIZE_MODE=lazy."
        )
