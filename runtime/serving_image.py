"""
Serving image publisher — Phase 6 (SCALE_ARCHITECTURE_PLAN.md §14, C++ Agent).

Writes a **complete, self-contained table image** to the artifact store: every S3
object the Agent would serve — the data splits AND the Iceberg
``metadata.json`` / manifests / ``version-hint.text`` (or the Delta ``_delta_log``)
— keyed by its exact S3 object key. Once published, the store directory is a valid,
servable warehouse: a stateless Agent (e.g. the C++ Agent) can serve any request by
returning the object's bytes straight from the store, with no SQL, Parquet, or
Iceberg/Delta logic of its own.

Data splits are already written through to the store (Phase 2); this fills in the
metadata objects (which are generated on demand today) so the image is complete.
"""
from __future__ import annotations

from observability.logging import get_logger

log = get_logger(__name__)


def publish_serving_image(store) -> dict:
    """Publish every current S3 object (data + metadata) to ``store``.

    Returns ``{"written": n, "skipped": m}``. Best‑effort: a per‑object failure is
    logged and skipped rather than aborting the publish.
    """
    from s3.router import _snapshot_objects
    import cache.lru_cache as cache

    objects = _snapshot_objects()
    written = 0
    skipped = 0
    for key, meta in objects.items():
        data = meta.get("data")
        if data is None:
            # Data splits carry no inline bytes here — pull the materialized bytes.
            data = cache.peek_parquet(key)
        if data is None:
            skipped += 1
            continue
        try:
            store.put(key, data)
            written += 1
        except Exception as exc:  # noqa: BLE001 - one bad object must not abort the image
            log.warning("serving_image_write_failed", key=key, error=str(exc))
            skipped += 1

    log.info("serving_image_published", written=written, skipped=skipped, objects=len(objects))
    return {"written": written, "skipped": skipped, "objects": len(objects)}
