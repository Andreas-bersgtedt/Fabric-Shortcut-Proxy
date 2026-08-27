"""Fail-closed, generation-aware serving-image publication."""
from __future__ import annotations

import hashlib
import json
import time

from observability.logging import get_logger
from runtime.generation import (
    BUILD_KEY,
    CURRENT_KEY,
    GenerationContext,
    GenerationError,
    acquire_generation,
    assert_generation_lease,
    renew_generation,
)

log = get_logger(__name__)


def _json_bytes(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _generation_key(generation_id: str, key: str) -> str:
    return f"generations/{generation_id}/{key}"


def publish_serving_image(
    store,
    context: GenerationContext | None = None,
    *,
    timeout_seconds: float = 900,
) -> dict:
    """Stage, verify, and atomically activate one immutable serving generation."""
    from s3.router import _snapshot_objects
    import cache.lru_cache as cache

    if context is None:
        context = acquire_generation(store, 1)
    assert_generation_lease(store, context)
    deadline = time.monotonic() + timeout_seconds
    objects = _snapshot_objects()
    entries: list[tuple[str, int, str]] = []
    try:
        for key in sorted(objects):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"generation publication timed out after {timeout_seconds}s")
            context = renew_generation(store, context)
            data = objects[key].get("data")
            if data is None:
                data = cache.peek_parquet(key)
            if data is None:
                raise GenerationError(f"generation object has no bytes: {key}")
            digest = hashlib.sha256(data).hexdigest()
            staged_key = _generation_key(context.generation_id, key)
            store.put(staged_key, data)
            verified = store.get(staged_key)
            if len(verified) != len(data) or hashlib.sha256(verified).hexdigest() != digest:
                raise GenerationError(f"generation object verification failed: {key}")
            entries.append((key, len(data), digest))

        index_lines = ["fsp-generation-index-v1"]
        index_lines.extend(f"{key}\t{size}\t{digest}" for key, size, digest in entries)
        index_bytes = ("\n".join(index_lines) + "\n").encode("utf-8")
        index_key = _generation_key(context.generation_id, "OBJECTS.index")
        store.put(index_key, index_bytes)
        if store.get(index_key) != index_bytes:
            raise GenerationError("generation index verification failed")

        ready = {
            "version": 1,
            "state": "READY",
            "generation_id": context.generation_id,
            "fence": context.fence,
            "lease_token": context.lease_token,
            "source_consistency": context.source_consistency,
            "object_count": len(entries),
            "index_sha256": hashlib.sha256(index_bytes).hexdigest(),
        }
        ready_bytes = _json_bytes(ready)
        ready_key = _generation_key(context.generation_id, "READY.json")
        store.put(ready_key, ready_bytes)
        if store.get(ready_key) != ready_bytes:
            raise GenerationError("READY.json verification failed")

        assert_generation_lease(store, context)
        current = {
            "version": 1,
            "generation_id": context.generation_id,
            "fence": context.fence,
            "lease_token": context.lease_token,
            "source_consistency": context.source_consistency,
            "ready_sha256": hashlib.sha256(ready_bytes).hexdigest(),
        }
        store.put(CURRENT_KEY, _json_bytes(current))
        store.put(BUILD_KEY, _json_bytes({**ready, "state": "ACTIVE"}))
    except Exception as exc:
        failed = {
            "version": 1,
            "state": "FAILED",
            "generation_id": context.generation_id,
            "fence": context.fence,
            "lease_token": context.lease_token,
            "error": str(exc)[:500],
        }
        try:
            store.put(_generation_key(context.generation_id, "FAILED.json"), _json_bytes(failed))
        except Exception:
            pass
        raise

    log.info("serving_generation_activated", generation_id=context.generation_id,
             fence=context.fence, objects=len(entries))
    return {
        "generation_id": context.generation_id,
        "fence": context.fence,
        "written": len(entries),
        "objects": len(entries),
        "state": "ACTIVE",
    }
