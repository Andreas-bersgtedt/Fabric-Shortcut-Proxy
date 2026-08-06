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

import config
from observability.logging import get_logger

log = get_logger(__name__)

_snapshots_ready = False
_build_lock = asyncio.Lock()


async def _ensure_snapshots() -> None:
    """Build the table snapshot registry once (deterministic keys) so an object
    key can be resolved to its table. Cheap after the first call."""
    global _snapshots_ready
    if _snapshots_ready:
        return
    async with _build_lock:
        if _snapshots_ready:
            return
        from db.executor import resolve_tables
        from iceberg.state_store import build_all_snapshots, get_all_snapshots

        await resolve_tables(config.TABLES)
        build_all_snapshots(config.TABLES, config.BUCKET_NAME, config.WAREHOUSE_PREFIX)
        if any(t.effective_split_strategy in ("range", "date", "auto") for t in config.TABLES) or any(
            t.effective_split_target_rows > 0 for t in config.TABLES
        ):
            from planner.split_planner import plan_ranges_for_snapshot
            for snap in get_all_snapshots():
                await plan_ranges_for_snapshot(snap)
        _snapshots_ready = True


def _snapshot_for_key(key: str):
    from iceberg.state_store import get_all_snapshots, get_split_by_key

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


def _publish_snapshot_objects(snap) -> int:
    """Write the snapshot's data splits and metadata objects to the shared store
    so a stateless Agent can serve them. Idempotent (overwrites with identical
    bytes). Data splits are pinned in memory by the materializer; here we persist
    every object explicitly rather than relying on write-through settings."""
    import cache.lru_cache as cache
    from runtime.artifact_store import build_store

    store = build_store(config.ARTIFACT_STORE_BACKEND, local_dir=config.ARTIFACT_STORE_DIR)
    written = 0
    for s in snap.splits:
        data = cache.peek_parquet(s.object_key)
        if data is not None:
            store.put(s.object_key, data)
            written += 1

    if config.TABLE_FORMAT == "delta":
        from delta import log as delta_log
        for k, meta in delta_log.delta_log_objects().items():
            if k.startswith(snap.table_path + "/") and meta.get("data") is not None:
                store.put(k, meta["data"])
                written += 1
    else:
        from iceberg.metadata import build_metadata_json
        from iceberg.manifest import build_manifest_file, build_manifest_list

        store.put(snap.metadata_key, build_metadata_json(snap))
        store.put(snap.manifest_list_key, build_manifest_list(snap))
        store.put(snap.manifest_file_key, build_manifest_file(snap))
        store.put(snap.version_hint_key, str(snap.version).encode())
        written += 4
    return written


async def materialize_for_key(key: str) -> dict:
    """Materialize the table owning ``key`` into the shared store. Idempotent.

    Returns ``{"ok": bool, "materialized": bool, ...}``.
    """
    await _ensure_snapshots()
    snap = _snapshot_for_key(key)
    if snap is None:
        return {"ok": False, "materialized": False, "reason": "unknown_key"}
    from runtime.materializer import ensure_snapshot_materialized

    await ensure_snapshot_materialized(snap)
    published = _publish_snapshot_objects(snap)
    log.info("manager_materialized_for_agent", key=key, table=snap.table.name,
             objects_published=published)
    return {"ok": True, "materialized": True, "table": snap.table.name}


def reset() -> None:
    """Test hook: forget the built-snapshots flag so a fresh registry is built."""
    global _snapshots_ready
    _snapshots_ready = False
