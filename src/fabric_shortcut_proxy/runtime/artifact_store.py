"""
Artifact store — the durable object layer for the cluster (docs/SCALE_ARCHITECTURE_PLAN.md §4.3).

A single, small interface behind which materialized **Parquet splits** and table
**metadata** (Iceberg ``metadata.json``/manifests or the Delta ``_delta_log``) are
read and written. Introducing it in Phase 0 lets serving be decoupled from
generation and later shared across a fleet of stateless Agents — without changing
the S3 wire protocol.

Backends:
  - :class:`LocalDirStore` — a filesystem directory (single box, or an NFS/SMB
    share for multi‑node). Atomic writes via temp‑file + ``os.replace``.
  - :class:`MemoryStore` — in‑process dict, for tests and ephemeral use.
  - :class:`AzureBlobArtifactStore` — Azure Blob Storage, loaded lazily when the
    Azure backend is configured.

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
import contextlib
import hashlib
import hmac
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import BinaryIO, Iterable, Iterator, cast


@dataclass(frozen=True)
class ObjectStat:
    """Metadata for a stored object."""
    key: str
    size: int
    mtime_ms: int | None = None      # last-modified epoch ms, when the backend knows it
    etag: str | None = None


class ObjectNotFound(KeyError):
    """Raised by :meth:`ArtifactStore.get` when a key does not exist."""


class ObjectConflict(RuntimeError):
    """Raised when an immutable artifact key already contains other content."""


# Default streaming chunk size (1 MiB) for get_stream / passthrough serving.
_STREAM_CHUNK = 1 << 20


def _input_chunks(data: BinaryIO | Iterable[bytes]) -> Iterator[bytes]:
    reader = getattr(data, "read", None)
    if callable(reader):
        while True:
            chunk = reader(_STREAM_CHUNK)
            if not chunk:
                return
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise TypeError("artifact stream must yield bytes")
            yield bytes(chunk)
    else:
        for chunk in cast(Iterable[object], data):
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise TypeError("artifact stream must yield bytes")
            yield bytes(chunk)


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

    def put_stream(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        """Store a byte stream; streaming-capable backends should override this."""
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        value = bytearray()
        for chunk in _input_chunks(data):
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                raise TypeError("artifact stream must yield bytes")
            value.extend(chunk)
        if length is not None and len(value) != length:
            raise ValueError(
                f"artifact stream length mismatch: expected {length}, received {len(value)}"
            )
        return self.put(key, bytes(value))

    def put_stream_if_absent(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        """Create an immutable object, accepting an identical prior upload."""
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        value = bytearray()
        digest = hashlib.sha256()
        for chunk in _input_chunks(data):
            value.extend(chunk)
            digest.update(chunk)
        if length is not None and len(value) != length:
            raise ValueError(
                f"artifact stream length mismatch: expected {length}, received {len(value)}"
            )
        if self.compare_and_swap(key, None, bytes(value)):
            stat = self.head(key)
            if stat is None:
                raise ObjectNotFound(key)
            return stat
        stat = self.head(key)
        if stat is not None and stat.size == len(value) and self.verify(
            key,
            size=len(value),
            content_hash=digest.hexdigest(),
        ):
            return stat
        raise ObjectConflict(f"immutable artifact key already contains other data: {key}")

    @abc.abstractmethod
    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        """Return the object bytes (optionally a ``[offset, offset+length)`` slice).

        Raises :class:`ObjectNotFound` if the key is absent.
        """

    @abc.abstractmethod
    def head(self, key: str) -> ObjectStat | None:
        """Return the object's stat, or ``None`` if absent."""

    def verify(self, key: str, *, size: int, content_hash: str) -> bool:
        """Verify size and SHA-256 incrementally without buffering the object."""
        if size < 0:
            return False
        stat = self.head(key)
        if stat is None or stat.size != size:
            return False
        digest = hashlib.sha256()
        try:
            for chunk in self.get_stream(key):
                digest.update(chunk)
        except ObjectNotFound:
            return False
        return hmac.compare_digest(digest.hexdigest(), content_hash)

    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        """True iff ``key`` is present."""

    @abc.abstractmethod
    def list(self, prefix: str = "") -> list[ObjectStat]:
        """Return stats for every object whose key starts with ``prefix`` (sorted)."""

    @abc.abstractmethod
    def delete(self, key: str) -> bool:
        """Delete ``key``. Returns True if it existed, False otherwise."""

    def compare_and_swap(
        self,
        key: str,
        expected: bytes | None,
        data: bytes,
    ) -> bool:
        """Atomically replace ``key`` when its complete value matches ``expected``.

        ``expected=None`` means the key must not exist. Writable shared stores
        used for Manager HA must override this method.
        """
        raise NotImplementedError("artifact store does not support compare-and-swap")

    def fenced_put(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
        data: bytes,
    ) -> bool:
        """Write only while ``fence_key`` names the live owner and fence."""
        raise NotImplementedError("artifact store does not support fenced writes")

    def fenced_delete(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
    ) -> bool | None:
        """Delete only while ``fence_key`` names the live owner and fence."""
        raise NotImplementedError("artifact store does not support fenced deletes")

    def guarded_put_batch(
        self,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        """Write all values only while the guard object remains byte-identical."""
        raise NotImplementedError("artifact store does not support guarded batches")

    def fenced_guarded_put_batch(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        """Write a guarded batch only under the live Manager fence."""
        raise NotImplementedError(
            "artifact store does not support fenced guarded batches"
        )

    def get_stream(self, key: str, *, offset: int = 0, length: int | None = None,
                   chunk_size: int = _STREAM_CHUNK):
        """Yield the object's bytes in chunks (optionally a ``[offset, +length)`` slice).

        Default: a single ``get()`` blob. Backends that can read incrementally
        (e.g. a filesystem) override this so large objects stream without being
        fully buffered. Raises :class:`ObjectNotFound` if the key is absent.
        """
        yield self.get(key, offset=offset, length=length)

    def list_dir(self, prefix: str = "") -> list[tuple]:
        """One directory level under ``prefix`` (which must be empty or end in ``/``).

        Returns ``(name, is_dir, size, mtime_ms)`` for the immediate children only —
        the cheap, S3-``delimiter=/`` folder-browse path. Default derives it from the
        recursive ``list()``; filesystem backends override with an O(one-level) scan.
        """
        pfx = prefix.replace("\\", "/").lstrip("/")
        if pfx and not pfx.endswith("/"):
            pfx += "/"
        dirs: set[str] = set()
        files: list[tuple] = []
        for st in self.list(pfx):
            rest = st.key[len(pfx):]
            slash = rest.find("/")
            if slash == -1:
                files.append((rest, False, st.size, st.mtime_ms))
            else:
                dirs.add(rest[:slash])
        out = [(d, True, 0, None) for d in sorted(dirs)]
        out.extend(sorted(files))
        return out


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

    def put_stream(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        return super().put_stream(key, data, length=length)

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

    def verify(self, key: str, *, size: int, content_hash: str) -> bool:
        k = _normalize_key(key)
        with self._lock:
            data = self._data.get(k)
            return (
                data is not None
                and len(data) == size
                and hmac.compare_digest(
                    hashlib.sha256(data).hexdigest(), content_hash
                )
            )

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

    def compare_and_swap(
        self,
        key: str,
        expected: bytes | None,
        data: bytes,
    ) -> bool:
        k = _normalize_key(key)
        value = bytes(data)
        with self._lock:
            current = self._data.get(k)
            if current != expected:
                return False
            self._data[k] = value
            return True

    def _fence_matches(self, fence_key: str, owner_id: str, fence: int) -> bool:
        raw = self._data.get(_normalize_key(fence_key))
        if raw is None:
            return False
        try:
            record = json.loads(raw.decode("utf-8"))
            now_ms = int(time.time() * 1000)
            renew_ms = int(record.get("renew_ms", 0))
            ttl_ms = int(record.get("ttl_ms", 0))
            expires_at_ms = int(record.get("expires_at_ms", 0))
            live = (
                renew_ms > 0 and now_ms - renew_ms <= ttl_ms
                if renew_ms > 0
                else expires_at_ms > now_ms
            )
            return (
                record.get("owner_id", record.get("generation_id")) == owner_id
                and int(record.get("fence", -1)) == int(fence)
                and live
            )
        except (UnicodeDecodeError, ValueError, TypeError):
            return False

    def fenced_put(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
        data: bytes,
    ) -> bool:
        with self._lock:
            if not self._fence_matches(fence_key, owner_id, fence):
                return False
            self._data[_normalize_key(key)] = bytes(data)
            return True

    def fenced_delete(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
    ) -> bool | None:
        with self._lock:
            if not self._fence_matches(fence_key, owner_id, fence):
                return None
            return self._data.pop(_normalize_key(key), None) is not None

    def guarded_put_batch(
        self,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        with self._lock:
            if self._data.get(_normalize_key(guard_key)) != expected_guard:
                return False
            for key, data in values.items():
                self._data[_normalize_key(key)] = bytes(data)
            return True

    def fenced_guarded_put_batch(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        with self._lock:
            if (
                not self._fence_matches(fence_key, owner_id, fence)
                or self._data.get(_normalize_key(guard_key)) != expected_guard
            ):
                return False
            for key, data in values.items():
                self._data[_normalize_key(key)] = bytes(data)
            return True


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

    def put_stream(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        normalized = _normalize_key(key)
        path = self._path(normalized)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        size = 0
        try:
            with open(tmp, "wb") as handle:
                for chunk in _input_chunks(data):
                    if not isinstance(chunk, (bytes, bytearray, memoryview)):
                        raise TypeError("artifact stream must yield bytes")
                    raw = bytes(chunk)
                    handle.write(raw)
                    size += len(raw)
                if length is not None and size != length:
                    raise ValueError(
                        f"artifact stream length mismatch: expected {length}, received {size}"
                    )
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        finally:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
        return ObjectStat(normalized, size)

    @contextlib.contextmanager
    def _cas_lock(self):
        os.makedirs(self.root, exist_ok=True)
        lock_path = os.path.join(self.root, ".fsp-cas.lock")
        handle = open(lock_path, "a+b")
        if os.path.getsize(lock_path) == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + 5.0
        locked = False
        try:
            while not locked:
                try:
                    if os.name == "nt":
                        import msvcrt
                        handle.seek(0)
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    locked = True
                except (BlockingIOError, OSError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("timed out acquiring artifact store lock")
                    time.sleep(0.01)
            yield
        finally:
            if locked:
                if os.name == "nt":
                    import msvcrt
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()

    def compare_and_swap(
        self,
        key: str,
        expected: bytes | None,
        data: bytes,
    ) -> bool:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with self._cas_lock():
            try:
                with open(path, "rb") as fh:
                    current = fh.read()
            except FileNotFoundError:
                current = None
            if current != expected:
                return False
            self.put(key, data)
            return True

    def _fence_matches(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
    ) -> bool:
        try:
            with open(self._path(fence_key), "rb") as handle:
                record = json.loads(handle.read().decode("utf-8"))
                now_ms = int(time.time() * 1000)
                renew_ms = int(record.get("renew_ms", 0))
                ttl_ms = int(record.get("ttl_ms", 0))
                expires_at_ms = int(record.get("expires_at_ms", 0))
                live = (
                    renew_ms > 0 and now_ms - renew_ms <= ttl_ms
                    if renew_ms > 0
                    else expires_at_ms > now_ms
                )
                return (
                    record.get("owner_id", record.get("generation_id")) == owner_id
                    and int(record.get("fence", -1)) == int(fence)
                    and live
                )
        except (FileNotFoundError, UnicodeDecodeError, ValueError, TypeError):
            return False

    def fenced_put(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
        data: bytes,
    ) -> bool:
        with self._cas_lock():
            if not self._fence_matches(fence_key, owner_id, fence):
                return False
            self.put(key, data)
            return True

    def fenced_delete(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
    ) -> bool | None:
        with self._cas_lock():
            if not self._fence_matches(fence_key, owner_id, fence):
                return None
            return self.delete(key)

    def _read_complete(self, key: str) -> bytes | None:
        try:
            with open(self._path(key), "rb") as handle:
                return handle.read()
        except FileNotFoundError:
            return None

    def guarded_put_batch(
        self,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        with self._cas_lock():
            if self._read_complete(guard_key) != expected_guard:
                return False
            for key, data in values.items():
                self.put(key, data)
            return True

    def fenced_guarded_put_batch(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        with self._cas_lock():
            if (
                not self._fence_matches(fence_key, owner_id, fence)
                or self._read_complete(guard_key) != expected_guard
            ):
                return False
            for key, data in values.items():
                self.put(key, data)
            return True

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
            stat = os.stat(path)
        except FileNotFoundError:
            return None
        return ObjectStat(_normalize_key(key), stat.st_size, int(stat.st_mtime * 1000))

    def exists(self, key: str) -> bool:
        return os.path.isfile(self._path(key))

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = prefix.replace("\\", "/").lstrip("/")
        out: list[ObjectStat] = []
        if not os.path.isdir(self.root):
            return out
        for dirpath, _dirs, files in os.walk(self.root):
            for name in files:
                if name.endswith(".tmp") or name == ".fsp-cas.lock":
                    continue  # in-flight write
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, self.root).replace(os.sep, "/")
                if rel.startswith(pfx):
                    try:
                        st = os.stat(full)
                    except FileNotFoundError:
                        continue
                    out.append(ObjectStat(rel, st.st_size, int(st.st_mtime * 1000)))
        out.sort(key=lambda s: s.key)
        return out

    def list_dir(self, prefix: str = "") -> list[tuple]:
        """One directory level via ``os.scandir`` — no recursive walk (fast browse)."""
        pfx = prefix.replace("\\", "/").strip("/")
        base = os.path.join(self.root, *pfx.split("/")) if pfx else self.root
        rp = os.path.abspath(base)
        if rp != self.root and not rp.startswith(self.root + os.sep):
            raise ValueError(f"list prefix escapes store root: {prefix!r}")
        dirs: list[tuple] = []
        files: list[tuple] = []
        try:
            with os.scandir(base) as it:
                for e in it:
                    try:
                        if e.is_dir():
                            dirs.append((e.name, True, 0, None))
                        elif e.is_file():
                            if e.name.endswith(".tmp") or e.name == ".fsp-cas.lock":
                                continue
                            st = e.stat()
                            files.append((e.name, False, st.st_size, int(st.st_mtime * 1000)))
                    except OSError:
                        continue
        except (FileNotFoundError, NotADirectoryError):
            return []
        dirs.sort(); files.sort()
        return dirs + files

    def delete(self, key: str) -> bool:
        path = self._path(key)
        try:
            os.remove(path)
            return True
        except FileNotFoundError:
            return False

    def get_stream(self, key: str, *, offset: int = 0, length: int | None = None,
                   chunk_size: int = _STREAM_CHUNK):
        """Stream the file in ``chunk_size`` blocks so large objects aren't buffered."""
        if offset < 0:
            raise ValueError("offset must be >= 0")
        path = self._path(key)
        try:
            fh = open(path, "rb")
        except FileNotFoundError:
            raise ObjectNotFound(_normalize_key(key)) from None
        with fh:
            if offset:
                fh.seek(offset)
            remaining = length
            while remaining is None or remaining > 0:
                to_read = chunk_size if remaining is None else min(chunk_size, remaining)
                chunk = fh.read(to_read)
                if not chunk:
                    break
                yield chunk
                if remaining is not None:
                    remaining -= len(chunk)


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
    if b == "azure":
        from fabric_shortcut_proxy.runtime.azure_artifact_store import (
            build_azure_artifact_store,
        )

        return build_azure_artifact_store()
    raise ValueError(
        f"unknown artifact store backend: {backend!r} "
        "(expected 'local', 'memory', or 'azure')"
    )


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
                from fabric_shortcut_proxy import config
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
