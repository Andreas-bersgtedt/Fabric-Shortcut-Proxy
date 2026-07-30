"""
S3 / MinIO mount backend (devplan/StorageProxy.md, Phase 2).

An :class:`~runtime.artifact_store.ArtifactStore` that reads objects straight from
an upstream S3 (or S3-compatible) bucket, so an ``s3`` mount serves those objects
as byte passthrough with ranged GET and one-level folder browsing. Read-only:
``put``/``delete`` raise, and every key is normalized to reject ``..`` so a request
can never escape the mount's confinement prefix (which
:func:`storage.passthrough._backend_key` has already applied).

The heavy client machinery lives in :mod:`storage.s3_auth`; this module only maps
the store interface onto S3 API calls and follows continuation tokens so nothing
is silently truncated at 1000 keys.

boto3 is imported lazily via the client factory, so importing this module is safe
without the ``s3proxy`` extra installed.
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

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}


def _mtime_ms(value) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def _reject_traversal(prefix: str) -> str:
    """Normalize a listing prefix (empty allowed) and reject ``..`` escapes."""
    p = (prefix or "").replace("\\", "/").lstrip("/")
    if any(seg == ".." for seg in p.split("/")):
        raise ValueError(f"listing prefix must not contain '..': {prefix!r}")
    return p


class S3Store(ArtifactStore):
    """Read-only object store backed by an upstream S3 bucket + client."""

    def __init__(self, *, bucket: str, client) -> None:
        self._bucket = bucket
        self._client = client

    # -- read -------------------------------------------------------------
    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        try:
            resp = self._client.head_object(Bucket=self._bucket, Key=k)
        except Exception as exc:  # noqa: BLE001 - map S3 not-found to None
            if _is_not_found(exc):
                return None
            raise
        return ObjectStat(k, int(resp.get("ContentLength", 0)), _mtime_ms(resp.get("LastModified")))

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(self, key: str, *, offset: int = 0, length: int | None = None,
                   chunk_size: int = _STREAM_CHUNK):
        if offset < 0:
            raise ValueError("offset must be >= 0")
        k = _normalize_key(key)
        kwargs = {"Bucket": self._bucket, "Key": k}
        if offset or length is not None:
            end = "" if length is None else offset + length - 1
            kwargs["Range"] = f"bytes={offset}-{end}"
        try:
            resp = self._client.get_object(**kwargs)
        except Exception as exc:  # noqa: BLE001
            if _is_not_found(exc):
                raise ObjectNotFound(k) from None
            raise
        body = resp["Body"]
        for chunk in body.iter_chunks(chunk_size):
            if chunk:
                yield chunk

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = _reject_traversal(prefix)
        out: list[ObjectStat] = []
        for page in self._paginate(prefix=pfx):
            for obj in page.get("Contents", []) or []:
                out.append(ObjectStat(obj["Key"], int(obj.get("Size", 0)), _mtime_ms(obj.get("LastModified"))))
        out.sort(key=lambda s: s.key)
        return out

    def list_dir(self, prefix: str = "") -> list[tuple]:
        """One directory level (``Delimiter='/'``) — the folder-browse path."""
        pfx = _reject_traversal(prefix)
        if pfx and not pfx.endswith("/"):
            pfx += "/"
        dirs: list[tuple] = []
        files: list[tuple] = []
        plen = len(pfx)
        for page in self._paginate(prefix=pfx, delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                name = cp["Prefix"][plen:].rstrip("/")
                if name:
                    dirs.append((name, True, 0, None))
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                if key == pfx:
                    continue                       # the folder placeholder object
                name = key[plen:]
                if not name or "/" in name:
                    continue
                files.append((name, False, int(obj.get("Size", 0)), _mtime_ms(obj.get("LastModified"))))
        dirs.sort()
        files.sort()
        return dirs + files

    def _paginate(self, *, prefix: str, delimiter: str = ""):
        """Yield ListObjectsV2 pages, following continuation tokens to the end."""
        token = None
        while True:
            kwargs = {"Bucket": self._bucket, "Prefix": prefix}
            if delimiter:
                kwargs["Delimiter"] = delimiter
            if token:
                kwargs["ContinuationToken"] = token
            resp = self._client.list_objects_v2(**kwargs)
            yield resp
            if resp.get("IsTruncated") and resp.get("NextContinuationToken"):
                token = resp["NextContinuationToken"]
            else:
                return

    # -- write (read-only mount) -----------------------------------------
    def put(self, key: str, data: bytes) -> ObjectStat:
        raise NotImplementedError("s3 mount is read-only")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("s3 mount is read-only")


def _is_not_found(exc: Exception) -> bool:
    """True when a boto3 ClientError signals a missing key/bucket (vs a real error)."""
    resp = getattr(exc, "response", None)
    if isinstance(resp, dict):
        code = str(resp.get("Error", {}).get("Code", ""))
        status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in _NOT_FOUND_CODES or status == 404:
            return True
    return False


def build_s3_store(mount) -> S3Store:
    """Construct an :class:`S3Store` for an ``s3`` mount (upstream bucket = ``root``)."""
    from storage.s3_auth import build_s3_client, options_from_mount, resolve_s3_auth

    auth = resolve_s3_auth(mount)
    opts = options_from_mount(mount)
    client = build_s3_client(auth, opts)
    return S3Store(bucket=mount.root, client=client)
