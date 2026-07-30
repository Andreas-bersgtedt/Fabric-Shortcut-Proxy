"""
Azure Blob / ADLS Gen2 mount backend (devplan/StorageProxy.md, Phase 3).

An :class:`~runtime.artifact_store.ArtifactStore` that reads blobs straight from an
upstream Azure container, so an ``azure`` mount serves those blobs as byte
passthrough with ranged GET and one-level folder browsing. Works against flat Blob
storage and ADLS Gen2 (hierarchical namespace) alike, since both expose the Blob
endpoint. Read-only: ``put``/``delete`` raise, and every key is normalized to
reject ``..`` so a request can never escape the mount's confinement prefix (which
:func:`storage.passthrough._backend_key` has already applied).

The client machinery lives in :mod:`storage.azure_auth`; this module only maps the
store interface onto Blob API calls. azure-storage-blob is imported lazily via the
client factory, so importing this module is safe without the ``azureblob`` extra.
"""
from __future__ import annotations

from datetime import datetime

from runtime.artifact_store import (
    ArtifactStore,
    ObjectNotFound,
    ObjectStat,
    _STREAM_CHUNK,
    _normalize_key,
)


def _mtime_ms(value) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def _is_not_found(exc: Exception) -> bool:
    """True when an Azure error signals a missing blob/container (vs a real error)."""
    if getattr(exc, "status_code", None) == 404:
        return True
    return any(t.__name__ == "ResourceNotFoundError" for t in type(exc).__mro__)


def _reject_traversal(prefix: str) -> str:
    """Normalize a listing prefix (empty allowed) and reject ``..`` escapes."""
    p = (prefix or "").replace("\\", "/").lstrip("/")
    if any(seg == ".." for seg in p.split("/")):
        raise ValueError(f"listing prefix must not contain '..': {prefix!r}")
    return p


class AzureBlobStore(ArtifactStore):
    """Read-only object store backed by an upstream Azure container client."""

    def __init__(self, *, container: str, client) -> None:
        self._container = container
        self._client = client               # azure ContainerClient (or a fake)

    # -- read -------------------------------------------------------------
    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        try:
            props = self._client.get_blob_client(k).get_blob_properties()
        except Exception as exc:  # noqa: BLE001 - map not-found to None
            if _is_not_found(exc):
                return None
            raise
        return ObjectStat(k, int(getattr(props, "size", 0) or 0), _mtime_ms(getattr(props, "last_modified", None)))

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(self, key: str, *, offset: int = 0, length: int | None = None,
                   chunk_size: int = _STREAM_CHUNK):
        if offset < 0:
            raise ValueError("offset must be >= 0")
        k = _normalize_key(key)
        kwargs = {}
        if offset or length is not None:
            kwargs["offset"] = offset
            if length is not None:
                kwargs["length"] = length
        try:
            downloader = self._client.download_blob(k, **kwargs)
        except Exception as exc:  # noqa: BLE001
            if _is_not_found(exc):
                raise ObjectNotFound(k) from None
            raise
        for chunk in downloader.chunks():
            if chunk:
                yield chunk

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = _reject_traversal(prefix)
        out: list[ObjectStat] = []
        for b in self._client.list_blobs(name_starts_with=pfx):
            out.append(ObjectStat(b.name, int(getattr(b, "size", 0) or 0), _mtime_ms(getattr(b, "last_modified", None))))
        out.sort(key=lambda s: s.key)
        return out

    def list_dir(self, prefix: str = "") -> list[tuple]:
        """One directory level (``walk_blobs`` with delimiter='/') — folder browse."""
        pfx = _reject_traversal(prefix)
        if pfx and not pfx.endswith("/"):
            pfx += "/"
        plen = len(pfx)
        dirs: list[tuple] = []
        files: list[tuple] = []
        for item in self._client.walk_blobs(name_starts_with=pfx, delimiter="/"):
            name = item.name
            if name == pfx:
                continue                       # the folder placeholder blob
            child = name[plen:]
            if child.endswith("/"):
                child = child[:-1]
                if child:
                    dirs.append((child, True, 0, None))
            elif child and "/" not in child:
                files.append((child, False, int(getattr(item, "size", 0) or 0),
                              _mtime_ms(getattr(item, "last_modified", None))))
        dirs.sort()
        files.sort()
        return dirs + files

    # -- write (read-only mount) -----------------------------------------
    def put(self, key: str, data: bytes) -> ObjectStat:
        raise NotImplementedError("azure mount is read-only")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("azure mount is read-only")


def build_azure_store(mount) -> AzureBlobStore:
    """Construct an :class:`AzureBlobStore` for an ``azure`` mount (container = ``root``)."""
    from storage.azure_auth import build_container_client, options_from_mount, resolve_azure_auth

    auth = resolve_azure_auth(mount)
    opts = options_from_mount(mount)
    client = build_container_client(auth, opts, mount.root)
    return AzureBlobStore(container=mount.root, client=client)
