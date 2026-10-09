"""Manager-side on-demand materialization for lazy mode (stateless / C++ Agents).

A stateless serving Agent (e.g. the zero-dependency C++ Agent) that hits a store
MISS under ``MATERIALIZE_MODE=lazy`` asks the Manager to materialize the object's
table (``POST /control/materialize``). The Manager builds that table's splits and
writes the complete set of objects — data splits **and** metadata / ``_delta_log`` —
into the shared artifact store, so the Agent can then serve every object straight
from the store with no SQL of its own.

The Manager already has DB access (``enterprise.manager`` hydrates credentials before
config import), so it can run the same :mod:`runtime.materializer` pipeline the Python
Agents use. Materialization is idempotent and per-table locked in the materializer.
"""
from __future__ import annotations

import asyncio
import hashlib
import time

from fabric_shortcut_proxy import config
from enterprise.control.contract import (
    Column,
    KeyRange,
    MaterializeTask,
    SnapshotManifest,
    SplitRef,
)
from enterprise.control.work_queue import DurableWorkQueue
from enterprise.control.placement import PlacementConfig, ResidencyPolicyViolation
from fabric_shortcut_proxy.observability.logging import get_logger

log = get_logger(__name__)

_snapshots_ready = False
_build_lock = asyncio.Lock()
_queue: DurableWorkQueue | None = None
_placement = PlacementConfig()
_publication_locks: dict[str, asyncio.Lock] = {}


def configure(queue: DurableWorkQueue, placement: PlacementConfig | None = None) -> None:
    global _queue, _placement
    _queue = queue
    _placement = placement or PlacementConfig()


def _publication_lock(request_id: str) -> asyncio.Lock:
    lock = _publication_locks.get(request_id)
    if lock is None:
        lock = asyncio.Lock()
        _publication_locks[request_id] = lock
    return lock


def _prune_generation_staging(
    generation_id: str, task_ids: list[str]
) -> int:
    if _queue is None:
        raise RuntimeError("materialization work queue is not configured")
    output_keys: list[str] = []
    keep_keys: set[str] = set()
    for task_id in task_ids:
        record = _queue.get_task(task_id)
        if record is None:
            continue
        task = record.get("task") or {}
        output_key = str(task.get("output_key", ""))
        if output_key:
            output_keys.append(output_key)
        result = record.get("result") or {}
        staged_key = str(result.get("output_key", ""))
        if staged_key:
            keep_keys.add(staged_key)
    return _queue.prune_staging(
        generation_id, output_keys, keep_keys=keep_keys
    )


async def _ensure_snapshots() -> None:
    """Build the table snapshot registry once (deterministic keys) so an object
    key can be resolved to its table. Cheap after the first call."""
    global _snapshots_ready
    if _snapshots_ready:
        return
    async with _build_lock:
        if _snapshots_ready:
            return
        from fabric_shortcut_proxy.db.executor import resolve_tables
        from fabric_shortcut_proxy.iceberg.state_store import build_all_snapshots, get_all_snapshots

        await resolve_tables(config.TABLES)
        build_all_snapshots(config.TABLES, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
        if any(t.effective_split_strategy in ("range", "date", "auto") for t in config.TABLES) or any(
            t.effective_split_target_rows > 0 for t in config.TABLES
        ):
            from fabric_shortcut_proxy.planner.split_planner import plan_ranges_for_snapshot
            for snap in get_all_snapshots():
                await plan_ranges_for_snapshot(snap)
        _snapshots_ready = True


def _snapshot_for_key(key: str):
    from fabric_shortcut_proxy.iceberg.state_store import get_all_snapshots, get_split_by_key

    for snap in get_all_snapshots():
        if key in (snap.metadata_key, snap.version_hint_key,
                   snap.manifest_list_key, snap.manifest_file_key):
            return snap
        if "/_delta_log/" in key and key.startswith(snap.table_path + "/_delta_log/"):
            return snap
    split = get_split_by_key(key)
    if split is not None:
        for snap in get_all_snapshots():
            if snap.table.name == split.table.name:
                return snap
    return None


def _publish_snapshot_objects(snap) -> tuple[int, list[str]]:
    """Write the snapshot's data splits and metadata objects to the shared store
    so a stateless Agent can serve them. Idempotent (overwrites with identical
    bytes). Data splits are pinned in memory by the materializer; here we persist
    every object explicitly rather than relying on write-through settings."""
    import fabric_shortcut_proxy.cache.lru_cache as cache
    from fabric_shortcut_proxy.runtime.artifact_store import build_store

    store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
    written = 0
    metadata_keys = []
    for s in snap.splits:
        data = cache.peek_parquet(s.object_key)
        if data is not None:
            store.put(s.object_key, data)
            written += 1

    if config.TABLE_FORMAT == "delta":
        from fabric_shortcut_proxy.delta import log as delta_log
        for k, meta in delta_log.delta_log_objects().items():
            if k.startswith(snap.table_path + "/") and meta.get("data") is not None:
                store.put(k, meta["data"])
                metadata_keys.append(k)
                written += 1
    else:
        from fabric_shortcut_proxy.iceberg.metadata import build_metadata_json
        from fabric_shortcut_proxy.iceberg.manifest import build_manifest_file, build_manifest_list

        store.put(snap.metadata_key, build_metadata_json(snap))
        store.put(snap.manifest_list_key, build_manifest_list(snap))
        store.put(snap.manifest_file_key, build_manifest_file(snap))
        store.put(snap.version_hint_key, str(snap.version).encode())
        metadata_keys.extend([
            snap.metadata_key,
            snap.manifest_list_key,
            snap.manifest_file_key,
            snap.version_hint_key,
        ])
        written += 4
    return written, metadata_keys


async def _materialize_direct(key: str) -> dict:
    """Materialize the table owning ``key`` into the shared store. Idempotent.

    Returns ``{"ok": bool, "materialized": bool, ...}``.
    """
    await _ensure_snapshots()
    snap = _snapshot_for_key(key)
    if snap is None:
        return {"ok": False, "materialized": False, "reason": "unknown_key"}
    from fabric_shortcut_proxy.runtime.materializer import ensure_snapshot_materialized

    await ensure_snapshot_materialized(snap)
    published, _ = _publish_snapshot_objects(snap)
    log.info("manager_materialized_for_agent", key=key, table=snap.table.name,
             objects_published=published)
    return {"ok": True, "materialized": True, "table": snap.table.name}


def _task_columns(table) -> list[Column]:
    columns = []
    for column in table.schema:
        transform = None
        if column.transform is not None:
            transform = {
                "kind": column.transform.kind,
                "key_ref": column.transform.key_ref,
                "domain": column.transform.domain,
                "normalization": column.transform.normalization,
            }
        columns.append(
            Column(
                field_id=column.field_id,
                name=column.name,
                iceberg_type=column.iceberg_type,
                nullable=column.nullable,
                source=column.source or "",
                transform=transform,
                policy_id=column.policy_id or "",
            )
        )
    return columns


def _task_range(split) -> KeyRange | None:
    lo, hi = split.key_lo, split.key_hi
    if lo is None and hi is None:
        return None
    if not isinstance(lo, int) or not isinstance(hi, int):
        raise ValueError(
            "distributed materialization requires integer split bounds"
        )
    return KeyRange(lo=lo, hi=hi)


async def materialize_for_key(key: str) -> dict:
    """Enqueue the key's table and wait for verified durable publication."""
    if _queue is None:
        return await _materialize_direct(key)
    await _ensure_snapshots()
    snap = _snapshot_for_key(key)
    if snap is None:
        return {"ok": False, "materialized": False, "reason": "unknown_key"}

    try:
        _placement.validate_dispatch(
            _placement.policy_for(snap.table.connection_id, snap.table.source_table),
            connection_id=snap.table.connection_id,
        )
    except ResidencyPolicyViolation as exc:
        log.error("residency_policy_violation", table=snap.table.name, detail=str(exc))
        return {"ok": False, "materialized": False, "reason": exc.code}

    from fabric_shortcut_proxy.runtime.artifact_store import get_default_store
    from fabric_shortcut_proxy.runtime.generation import current_generation

    generation = await asyncio.to_thread(
        current_generation, get_default_store()
    )
    if generation is None:
        return {
            "ok": False,
            "materialized": False,
            "reason": "generation_unavailable",
        }
    tasks = [
        MaterializeTask(
            table=snap.table.name,
            epoch=snap.version,
            split_index=split.split_index,
            output_key=split.object_key,
            deadline_ms=int(time.time() * 1000) + 300_000,
            schema=_task_columns(snap.table),
            range=_task_range(split),
            connection_id=snap.table.connection_id,
            source_table=snap.table.source_table,
            connection_fingerprint=hashlib.sha256(
                config.redact_db_url(
                    config.effective_db_url(snap.table.connection_id)
                ).encode("utf-8")
            ).hexdigest(),
            generation_id=generation.generation_id,
            generation_fence=generation.fence,
            plan_sha256=generation.plan_sha256,
            num_splits=len(snap.splits),
            key_column=snap.table.key_column or "",
            split_strategy=snap.table.effective_split_strategy,
        )
        for split in snap.splits
    ]
    timeout_seconds = config.WORK_QUEUE_REQUEST_TIMEOUT_SECONDS
    deadline_ms = int(time.time() * 1000) + timeout_seconds * 1000
    request = await asyncio.to_thread(
        _queue.create_request,
        requested_key=key,
        table=snap.table.name,
        epoch=snap.version,
        table_format=config.TABLE_FORMAT,
        generation_id=generation.generation_id,
        generation_fence=generation.fence,
        plan_sha256=generation.plan_sha256,
        tasks=tasks,
        deadline_ms=deadline_ms,
    )
    terminal = await _queue.wait_request(
        request["request_id"], timeout_seconds=float(timeout_seconds)
    )
    if terminal["state"] != "SUCCEEDED":
        return {
            "ok": False,
            "materialized": False,
            "reason": terminal.get("error") or terminal["state"].lower(),
            "request_id": terminal["request_id"],
        }
    async with _publication_lock(terminal["request_id"]):
        existing = await asyncio.to_thread(
            _queue.get_snapshot, snap.table.name, snap.version
        )
        if existing is not None and existing.request_id == terminal["request_id"]:
            await asyncio.to_thread(
                _prune_generation_staging,
                generation.generation_id,
                terminal["task_ids"],
            )
            return {
                "ok": True,
                "materialized": True,
                "table": snap.table.name,
                "request_id": terminal["request_id"],
            }
        split_refs = []
        output_keys = []
        for task_id in terminal["task_ids"]:
            record = await asyncio.to_thread(_queue.get_task, task_id)
            if record is None or not record.get("result"):
                raise RuntimeError(f"completed queue task has no result: {task_id}")
            task_payload = MaterializeTask.from_dict(record["task"])
            result = record["result"]
            snap_split = next(
                (
                    item
                    for item in snap.splits
                    if item.split_index == task_payload.split_index
                    and item.object_key == task_payload.output_key
                ),
                None,
            )
            if snap_split is None:
                raise RuntimeError(
                    f"queue result does not match snapshot split: {task_id}"
                )
            output_key = str(result.get("output_key", ""))
            original_parent = task_payload.output_key.rpartition("/")[0]
            staging_root = (
                f"{original_parent + '/' if original_parent else ''}.fsp/staging/"
                f"{hashlib.sha256(generation.generation_id.encode()).hexdigest()}/"
                f"{task_id}/"
            )
            if not output_key.startswith(staging_root):
                raise RuntimeError(
                    f"queue result has invalid staged output key: {task_id}"
                )
            snap_split.object_key = output_key
            output_keys.append(task_payload.output_key)
            snap_split.file_size_in_bytes = int(result["size_bytes"])
            snap_split.record_count = int(result["record_count"])
            snap_split.content_hash = str(result["content_hash"])
            snap_split.s3_etag = str(result["s3_etag"])
            split_refs.append(
                SplitRef(
                    object_key=output_key,
                    size_bytes=int(result["size_bytes"]),
                    record_count=int(result["record_count"]),
                    content_hash=str(result["content_hash"]),
                    range=task_payload.range,
                )
            )
        if config.TABLE_FORMAT == "delta":
            from fabric_shortcut_proxy.delta import log as delta_log

            delta_log.invalidate_table(snap.table.name)
        published, metadata_keys = await asyncio.to_thread(
            _publish_snapshot_objects, snap
        )
        manifest = SnapshotManifest(
            table=snap.table.name,
            epoch=snap.version,
            table_format=config.TABLE_FORMAT,
            splits=split_refs,
            metadata_keys=metadata_keys,
            generation_id=generation.generation_id,
            generation_fence=generation.fence,
            plan_sha256=generation.plan_sha256,
            request_id=terminal["request_id"],
            published_at_ms=int(time.time() * 1000),
        )
        await asyncio.to_thread(
            _queue.publish_snapshot, terminal["request_id"], manifest
        )
        await asyncio.to_thread(
            _queue.prune_staging,
            generation.generation_id,
            output_keys,
            keep_keys={split.object_key for split in split_refs},
        )
    log.info(
        "manager_queue_materialized_for_agent",
        key=key,
        table=snap.table.name,
        request_id=terminal["request_id"],
        objects_published=published,
    )
    return {
        "ok": True,
        "materialized": True,
        "table": snap.table.name,
        "request_id": terminal["request_id"],
    }


def reset() -> None:
    """Test hook: forget the built-snapshots flag so a fresh registry is built."""
    global _snapshots_ready
    _snapshots_ready = False
    global _queue
    _queue = None
    _publication_locks.clear()
