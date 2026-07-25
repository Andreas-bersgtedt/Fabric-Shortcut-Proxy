"""
Retention GC — prune orphaned Parquet splits from the shared artifact store
(SCALE_ARCHITECTURE_PLAN.md §14 Phase 5).

When AUTO_REFRESH publishes a new snapshot version, older versions eventually age
out of the in-memory history (``SNAPSHOT_HISTORY_LIMIT``). Their data-split objects
stay in the durable store forever unless collected. This sweep deletes store
objects that no **retained** snapshot references, bounding storage.

Conservative by design: it only touches ``.../data/*.parquet`` keys (the dominant
storage). Metadata/manifests are tiny, deterministic, and cheap to rebuild, so they
are left in place (metadata GC is a future enhancement). Deletes are idempotent, so
it is safe to run the sweep from a single Agent (shard 0) on a timer.
"""
from __future__ import annotations

import config
from observability.logging import get_logger

log = get_logger(__name__)


def live_object_keys() -> set[str]:
    """Every artifact key referenced by any RETAINED snapshot (current + history)."""
    from iceberg.state_store import get_all_snapshots

    keys: set[str] = set()
    for snap in get_all_snapshots():
        for k in (snap.metadata_key, snap.manifest_list_key,
                  snap.manifest_file_key, snap.version_hint_key):
            if k:
                keys.add(k)
        for s in snap.splits:
            keys.add(s.object_key)
    return keys


def gc_orphaned_data(store, *, warehouse_prefix: str | None = None,
                     dry_run: bool = False) -> list[str]:
    """Delete data-split objects in ``store`` not referenced by any retained
    snapshot. Returns the (sorted) list of orphan keys deleted (or that *would*
    be deleted when ``dry_run``)."""
    live = live_object_keys()
    prefix = warehouse_prefix if warehouse_prefix is not None else config.WAREHOUSE_PREFIX

    orphans: list[str] = []
    try:
        objects = store.list(prefix)
    except Exception as exc:  # noqa: BLE001 - GC must never break serving
        log.warning("retention_gc_list_error", error=str(exc))
        return []

    for stat in objects:
        k = stat.key
        if "/data/" not in k or not k.endswith(".parquet"):
            continue          # only collect data splits; leave metadata alone
        if k in live:
            continue          # still referenced by a retained snapshot
        orphans.append(k)
        if not dry_run:
            try:
                store.delete(k)
            except Exception as exc:  # noqa: BLE001
                log.warning("retention_gc_delete_error", key=k, error=str(exc))

    orphans.sort()
    if orphans:
        log.info("retention_gc", orphans=len(orphans), dry_run=dry_run,
                 live=len(live), prefix=prefix)
    return orphans
