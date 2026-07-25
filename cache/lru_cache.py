"""
Simple TTL + size-bounded LRU cache for metadata and generated Parquet objects.

Backed by an in-process dict; no external dependencies.
Keys are object paths (strings). Values are raw bytes.
"""
from __future__ import annotations

import hashlib
import os
import time
from collections import OrderedDict

import config
from observability.logging import get_logger
from observability import metrics

log = get_logger(__name__)


class BytesLRUCache:
    """Thread-safe TTL LRU cache for bytes values."""

    def __init__(self, max_bytes: int, ttl_seconds: int) -> None:
        self._max_bytes = max_bytes
        self._ttl = ttl_seconds
        self._store: OrderedDict[str, tuple[bytes, float]] = OrderedDict()
        self._current_bytes = 0

    def get(self, key: str) -> bytes | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        data, expiry = entry
        if time.monotonic() > expiry:
            self._evict(key)
            return None
        # Move to end (LRU touch)
        self._store.move_to_end(key)
        return data

    def put(self, key: str, data: bytes) -> None:
        if len(data) > self._max_bytes:
            # Object too large to cache
            return
        if key in self._store:
            self._evict(key)
        while self._current_bytes + len(data) > self._max_bytes and self._store:
            oldest_key = next(iter(self._store))
            self._evict(oldest_key)
        expiry = time.monotonic() + self._ttl
        self._store[key] = (data, expiry)
        self._current_bytes += len(data)
        log.debug("cache_put", key=key, size=len(data), total_bytes=self._current_bytes)

    def _evict(self, key: str) -> None:
        entry = self._store.pop(key, None)
        if entry:
            self._current_bytes -= len(entry[0])


# Singletons
_metadata_cache = BytesLRUCache(
    max_bytes=16 * 1024 * 1024,  # 16 MB — metadata is tiny
    ttl_seconds=config.METADATA_CACHE_TTL_SECONDS,
)
_parquet_cache = BytesLRUCache(
    max_bytes=config.PARQUET_CACHE_MAX_BYTES,
    ttl_seconds=config.PARQUET_CACHE_TTL_SECONDS,
)

# Authoritative, immutable snapshot data files. Startup materialization pins each
# split's Parquet bytes here so they are served BYTE-IDENTICAL for the life of the
# snapshot — never expired (TTL) or evicted (LRU). This prevents the split from
# being regenerated on demand with a DIFFERENT size than the manifest declared,
# which otherwise causes Fabric's XTable conversion to fail with READ_EXCEPTION /
# 404 BlobNotFound (especially for large tables under multi-table cache pressure).
_pinned: dict[str, bytes] = {}


# ---------------------------------------------------------------------------
# Persistent (disk) Parquet cache helpers (F5)
# ---------------------------------------------------------------------------

def _disk_path(key: str) -> str:
    digest = hashlib.sha256(key.encode()).hexdigest()
    return os.path.join(config.PARQUET_DISK_CACHE_DIR, digest + ".parquet")


def _disk_read(key: str) -> bytes | None:
    if not config.PARQUET_DISK_CACHE:
        return None
    try:
        with open(_disk_path(key), "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None
    except OSError as exc:  # pragma: no cover - defensive
        log.warning("disk_cache_read_failed", key=key, error=str(exc))
        return None


def _disk_write(key: str, data: bytes) -> None:
    if not config.PARQUET_DISK_CACHE:
        return
    try:
        os.makedirs(config.PARQUET_DISK_CACHE_DIR, exist_ok=True)
        path = _disk_path(key)
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)  # atomic publish
    except OSError as exc:  # pragma: no cover - defensive
        log.warning("disk_cache_write_failed", key=key, error=str(exc))


# ---------------------------------------------------------------------------
# Artifact-store serving tier (Phase 2). When ARTIFACT_STORE_SERVING is on, the
# durable, key-addressed artifact store (local dir by default; Blob/ADLS/MinIO in
# the backlog) backs Parquet serving: materialized splits are written to it and
# read back on a cold miss so a restarted/stateless Agent serves byte-identically
# with ZERO regeneration. All calls are best-effort — a store error never breaks
# serving (fall through to the in-memory/disk path or SQL regeneration).
# ---------------------------------------------------------------------------

def _store():
    if not config.ARTIFACT_STORE_SERVING:
        return None
    try:
        from runtime.artifact_store import get_default_store
        return get_default_store()
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("artifact_store_unavailable", error=str(exc))
        return None


def _store_read(key: str) -> bytes | None:
    store = _store()
    if store is None:
        return None
    try:
        from runtime.artifact_store import ObjectNotFound
        try:
            return store.get(key)
        except ObjectNotFound:
            return None
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("artifact_store_read_failed", key=key, error=str(exc))
        return None


def _store_write(key: str, data: bytes) -> None:
    store = _store()
    if store is None:
        return
    try:
        store.put(key, data)
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("artifact_store_write_failed", key=key, error=str(exc))


def _store_delete(key: str) -> None:
    store = _store()
    if store is None:
        return
    try:
        store.delete(key)
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("artifact_store_delete_failed", key=key, error=str(exc))


def get_metadata(key: str) -> bytes | None:
    data = _metadata_cache.get(key)
    metrics.record_cache("metadata", data is not None)
    return data


def put_metadata(key: str, data: bytes) -> None:
    _metadata_cache.put(key, data)


def get_parquet(key: str) -> bytes | None:
    pinned = _pinned.get(key)
    if pinned is not None:
        metrics.record_cache("parquet", True)
        return pinned
    data = _parquet_cache.get(key)
    if data is not None:
        metrics.record_cache("parquet", True)
        return data
    # Fall back to the persistent disk cache (F5). A disk hit still avoids
    # regeneration; it's a memory miss but a separate disk hit for observability.
    disk = _disk_read(key)
    metrics.record_cache("parquet", False)
    if disk is not None:
        metrics.record_cache("parquet_disk", True)
        _parquet_cache.put(key, disk)  # promote to memory
        return disk
    metrics.record_cache("parquet_disk", False)
    # Phase 2: durable artifact store (shared across Agents / restart-safe).
    stored = _store_read(key)
    if stored is not None:
        _parquet_cache.put(key, stored)  # promote to memory
        return stored
    return None


def peek_parquet(key: str) -> bytes | None:
    """Cache lookup that does NOT record a metrics event.

    Used for size resolution during HEAD/ListObjectsV2 so that sizing probes
    don't distort the data-serving cache hit ratio. Falls through to the disk
    cache so sizes are accurate on a warm restart before memory is populated.
    """
    pinned = _pinned.get(key)
    if pinned is not None:
        return pinned
    data = _parquet_cache.get(key)
    if data is not None:
        return data
    disk = _disk_read(key)
    if disk is not None:
        return disk
    return _store_read(key)


def warm_parquet(key: str) -> bytes | None:
    """Load a Parquet object from the persistent disk cache or artifact store into
    memory. Used at startup so a warm restart skips SQL + Parquet regeneration
    (keys are deterministic / content-addressed). Returns the bytes if present
    (and promotes them to the in-memory cache), else None. No metrics recorded.
    """
    data = _disk_read(key)
    if data is None:
        data = _store_read(key)   # Phase 2: durable artifact store
    if data is not None:
        _parquet_cache.put(key, data)
    return data


def put_parquet(key: str, data: bytes) -> None:
    _parquet_cache.put(key, data)
    _disk_write(key, data)
    _store_write(key, data)


def pin_parquet(key: str, data: bytes) -> None:
    """Pin an authoritative snapshot Parquet split so it is served verbatim and
    never expired/evicted (see ``_pinned``). Also write-through to the disk cache
    and the artifact store for warm-restart / cross-Agent durability."""
    _pinned[key] = data
    _disk_write(key, data)
    _store_write(key, data)


def evict_parquet(key: str) -> None:
    """Remove a Parquet object from memory and (if enabled) the disk cache and
    artifact store.

    Used when a content-addressed chunk is superseded (freshness refresh).
    """
    _pinned.pop(key, None)
    _parquet_cache._evict(key)
    if config.PARQUET_DISK_CACHE:
        try:
            os.remove(_disk_path(key))
        except OSError:
            pass
    _store_delete(key)


def unpin_all() -> None:
    """Drop all pinned splits (test helper / full snapshot rebuild)."""
    _pinned.clear()


def stats() -> dict:
    """Return current cache occupancy for diagnostics (/_admin/stats)."""
    return {
        "metadata": {
            "entries": len(_metadata_cache._store),
            "bytes": _metadata_cache._current_bytes,
            "max_bytes": _metadata_cache._max_bytes,
        },
        "parquet": {
            "entries": len(_parquet_cache._store),
            "bytes": _parquet_cache._current_bytes,
            "max_bytes": _parquet_cache._max_bytes,
        },
        "parquet_pinned": {
            "entries": len(_pinned),
            "bytes": sum(len(v) for v in _pinned.values()),
        },
        "artifact_store_serving": bool(config.ARTIFACT_STORE_SERVING),
    }
