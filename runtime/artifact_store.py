"""
Artifact store — the durable object layer for the cluster (SCALE_ARCHITECTURE_PLAN.md §4.3).

A single, small interface behind which materialized **Parquet splits** and table
**metadata** (Iceberg ``metadata.json``/manifests or the Delta ``_delta_log``) are
read and written. Introducing it in Phase 0 lets serving be decoupled from
generation and later shared across a fleet of stateless Agents — without changing
the S3 wire protocol.

Backends (Phase 0):
  - :class:`LocalDirStore` — a filesystem directory (single box, or an NFS/SMB
    share for multi‑node). Atomic writes via temp‑file + ``os.replace``.
  - :class:`MemoryStore` — in‑process dict, for tests and ephemeral use.

Future backends (S3/MinIO, Azure Blob/ADLS) implement the same interface with no
change to serving code.

**Keys** are POSIX‑style and identical to the S3 object keys the runtime serves,
e.g. ``warehouse/db/<table>/data/split-0-<hash>.parquet`` or
``warehouse/db/<table>/_delta_log/00000000000000000000.json``. A key maps 1:1 to a
stored object regardless of backend.

Design notes:
  - **Idempotent, content‑addressable friendly.** ``put`` overwrites atomically;
    re‑writing the same content‑addressed key is a safe no‑op‑equivalent.
  - **Ranged reads.** ``get(key, offset=, length=)`` supports the partial reads
    Parquet footer scans need (the caller derives a suffix range from ``head``).
  - **Path‑traversal safe.** Keys are validated; ``..`` / absolute paths are
    rejected so a malicious key can never escape the store root (OWASP A01/A03).
  - **Thread‑safe.** Backends guard mutation; safe under the async server's
    threadpool and the Manager's workers.
"""
from __future__ import annotations

import abc
import os
import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class ObjectStat:
    """Metadata for a stored object."""
    key: str
    size: int


class ObjectNotFound(KeyError):
    """Raised by :meth:`ArtifactStore.get` when a key does not exist."""


def _normalize_key(key: str) -> str:
    """Validate + normalize a POSIX object key.

    Rejects absolute paths and any ``..`` traversal so a key can never resolve
    outside the store root. Returns the cleaned key using ``/`` separators.
    """
    if not key or not isinstance(key, str):
        raise ValueError("artifact key must be a non-empty string")
    k = key.replace("\\", "/").strip("/")
    if not k:
        raise ValueError("artifact key must not be empty after normalization")
    parts = []
    for seg in k.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            raise ValueError(f"artifact key must not contain '..': {key!r}")
        parts.append(seg)
    if not parts:
        raise ValueError(f"invalid artifact key: {key!r}")
    return "/".join(parts)


def _slice(data: bytes, offset: int, length: int | None) -> bytes:
    if offset < 0:
        raise ValueError("offset must be >= 0 (use head() to derive suffix ranges)")
    if offset == 0 and length is None:
        return data
    end = len(data) if length is None else offset + length
    return data[offset:end]


class ArtifactStore(abc.ABC):
    """Durable object store for split + metadata bytes."""

    @abc.abstractmethod
    def put(self, key: str, data: bytes) -> ObjectStat:
        """Store ``data`` at ``key`` (atomic overwrite). Returns its stat."""

    @abc.abstractmethod
    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        """Return the object bytes (optionally a ``[offset, offset+length)`` slice).

        Raises :class:`ObjectNotFound` if the key is absent.
        """

    @abc.abstractmethod
    def head(self, key: str) -> ObjectStat | None:
        """Return the object's stat, or ``None`` if absent."""

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """True iff ``key`` is present."""

    @abc.abstractmethod
    def list(self, prefix: str = "") -> list[ObjectStat]:
        """Return stats for every object whose key starts with ``prefix`` (sorted)."""

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        """Delete ``key``. Returns True if it existed, False otherwise."""


class MemoryStore(ArtifactStore):
    """In‑process, dict‑backed store. Ephemeral — for tests and single‑box dev."""

    def __init__(self) -> None:
        self._data: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def put(self, key: str, data: bytes) -> ObjectStat:
        k = _normalize_key(key)
        b = bytes(data)
        with self._lock:
            self._data[k] = b
        return ObjectStat(k, len(b))

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        k = _normalize_key(key)
        with self._lock:
            try:
                b = self._data[k]
            except KeyError:
                raise ObjectNotFound(k) from None
        return _slice(b, offset, length)

    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        with self._lock:
            b = self._data.get(k)
        return None if b is None else ObjectStat(k, len(b))

    def exists(self, key: str) -> bool:
        k = _normalize_key(key)
        with self._lock:
            return k in self._data

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = prefix.replace("\\", "/").lstrip("/")
        with self._lock:
            items = [ObjectStat(k, len(v)) for k, v in self._data.items() if k.startswith(pfx)]
        items.sort(key=lambda s: s.key)
        return items

    def delete(self, key: str) -> bool:
        k = _normalize_key(key)
        with self._lock:
            return self._data.pop(k, None) is not None


class LocalDirStore(ArtifactStore):
    """Filesystem‑backed store rooted at ``root`` (a dir, NFS/SMB mount, etc.).

    Writes are atomic (temp file in the destination dir + ``os.replace``) so a
    concurrent reader never sees a partial object. The root is created lazily on
    first write.
    """

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        self._lock = threading.Lock()

    def _path(self, key: str) -> str:
        k = _normalize_key(key)
        p = os.path.join(self.root, *k.split("/"))
        # Defense in depth: the resolved path must stay under root.
        rp = os.path.abspath(p)
        root = self.root + os.sep
        if rp != self.root and not rp.startswith(root):
            raise ValueError(f"artifact key escapes store root: {key!r}")
        return rp

    def put(self, key: str, data: bytes) -> ObjectStat:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)  # atomic on Windows + POSIX
        return ObjectStat(_normalize_key(key), len(data))

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        if offset < 0:
            raise ValueError("offset must be >= 0 (use head() to derive suffix ranges)")
        path = self._path(key)
        try:
            with open(path, "rb") as fh:
                if offset:
                    fh.seek(offset)
                return fh.read(-1 if length is None else length)
        except FileNotFoundError:
            raise ObjectNotFound(_normalize_key(key)) from None

    def head(self, key: str) -> ObjectStat | None:
        path = self._path(key)
        try:
            return ObjectStat(_normalize_key(key), os.path.getsize(path))
        except FileNotFoundError:
            return None

    def exists(self, key: str) -> bool:
        return os.path.isfile(self._path(key))

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = prefix.replace("\\", "/").lstrip("/")
        out: list[ObjectStat] = []
        if not os.path.isdir(self.root):
            return out
        for dirpath, _dirs, files in os.walk(self.root):
            for name in files:
                if name.endswith(".tmp"):
                    continue  # in-flight write
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, self.root).replace(os.sep, "/")
                if rel.startswith(pfx):
                    try:
                        out.append(ObjectStat(rel, os.path.getsize(full)))
                    except FileNotFoundError:
                        continue
        out.sort(key=lambda s: s.key)
        return out

    def delete(self, key: str) -> bool:
        path = self._path(key)
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return False


# ---------------------------------------------------------------------------
# Factory + process-wide default
# ---------------------------------------------------------------------------

def build_store(backend: str, *, local_dir: str = "./.artifacts") -> ArtifactStore:
    """Construct an :class:`ArtifactStore` for the given backend name."""
    b = (backend or "").strip().lower()
    if b == "local":
        return LocalDirStore(local_dir)
    if b == "memory":
        return MemoryStore()
    raise ValueError(f"unknown artifact store backend: {backend!r} (expected 'local' or 'memory')")


_default_store: ArtifactStore | None = None
_default_lock = threading.Lock()


def get_default_store() -> ArtifactStore:
    """Return the process‑wide default store built from ``config`` (lazy singleton).

    Reads ``config.ARTIFACT_STORE_BACKEND`` / ``config.ARTIFACT_STORE_DIR``.
    """
    global _default_store
    if _default_store is None:
        with _default_lock:
            if _default_store is None:
                import config
                _default_store = build_store(
                    getattr(config, "ARTIFACT_STORE_BACKEND", "local"),
                    local_dir=getattr(config, "ARTIFACT_STORE_DIR", "./.artifacts"),
                )
    return _default_store


def set_default_store(store: ArtifactStore | None) -> None:
    """Override the process default (tests / explicit wiring)."""
    global _default_store
    with _default_lock:
        _default_store = store


def reset_default_store() -> None:
    """Clear the cached default so the next call rebuilds from config (tests)."""
    set_default_store(None)
