"""
Serving image publisher — Phase 6 (docs/SCALE_ARCHITECTURE_PLAN.md §14, C++ Agent).

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

import hashlib
import json
import uuid

from observability.logging import get_logger

log = get_logger(__name__)


def publish_serving_image(store, *, generation_id: str | None = None) -> dict:
    """Publish every current S3 object as one atomically activated generation.

    Objects are first written below ``generations/<id>``.  ``CURRENT`` is changed
    only after the generation's ``READY.json`` is written, so serving agents never
    select an incomplete image. Returns ``{"written": n, "skipped": m}``.
    """
    from s3.router import _snapshot_objects
    import cache.lru_cache as cache

    objects = _snapshot_objects()
    generation_id = generation_id or uuid.uuid4().hex
    if not generation_id.replace("-", "").isalnum():
        raise ValueError("generation_id must contain only letters, digits, and hyphens")
    prefix = f"generations/{generation_id}/"
    written = 0
    skipped = 0
    published: list[tuple[str, bytes]] = []
    for key, meta in objects.items():
        data = meta.get("data")
        if data is None:
            # Data splits carry no inline bytes here — pull the materialized bytes.
            data = cache.peek_parquet(key)
        if data is None:
            skipped += 1
            continue
        try:
            data = bytes(data)
            store.put(prefix + key, data)
            published.append((key, data))
            written += 1
        except Exception as exc:  # noqa: BLE001 - one bad object must not abort the image
            log.warning("serving_image_write_failed", key=key, error=str(exc))
            skipped += 1

    # A partial image is retained for diagnosis but must never be activated.
    if skipped:
        log.warning("serving_image_incomplete", generation_id=generation_id,
                    written=written, skipped=skipped, objects=len(objects))
        return {"written": written, "skipped": skipped, "objects": len(objects),
                "generation_id": generation_id, "activated": False}

    checksum = hashlib.sha256()
    for key, data in sorted(published):
        checksum.update(key.encode("utf-8"))
        checksum.update(b"\0")
        checksum.update(hashlib.sha256(data).digest())
    ready = json.dumps({
        "generation_id": generation_id,
        "object_count": written,
        "metadata_checksum": checksum.hexdigest(),
    }, sort_keys=True).encode("utf-8")
    store.put(prefix + "READY.json", ready)
    # ArtifactStore.put is an atomic replacement for the filesystem backend.
    store.put("CURRENT", generation_id.encode("utf-8"))
    log.info("serving_image_published", generation_id=generation_id,
             written=written, skipped=skipped, objects=len(objects))
    return {"written": written, "skipped": skipped, "objects": len(objects),
            "generation_id": generation_id, "activated": True}
