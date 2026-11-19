"""Read-only Google Cloud Storage mount backend."""
from __future__ import annotations

from datetime import datetime
import inspect

from fabric_shortcut_proxy.runtime.artifact_store import (
    ArtifactStore,
    ObjectNotFound,
    ObjectStat,
    _normalize_key,
)


def _mtime_ms(value) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    return None


def _gcs_missing(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    code = getattr(exc, "code", None)
    return status == 404 or code == 404 or exc.__class__.__name__ in {
        "NotFound",
        "GoogleNotFound",
    }


def _reject_traversal(prefix: str) -> str:
    p = (prefix or "").replace("\\", "/").lstrip("/")
    if any(seg == ".." for seg in p.split("/")):
        raise ValueError(f"listing prefix must not contain '..': {prefix!r}")
    return p


def _gcs_retry_kwargs(method) -> dict:
    try:
        parameters = inspect.signature(method).parameters
        supports_retry = "retry" in parameters
        supports_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        )
        if not supports_retry and not supports_kwargs:
            return {}
        from google.api_core.retry import Retry
        return {
            "retry": Retry(
                initial=1.0,
                maximum=30.0,
                multiplier=2.0,
                deadline=120.0,
            )
        }
    except (ImportError, TypeError, ValueError):
        return {}


class GCSStore(ArtifactStore):
    """Read-only object store backed by a GCS bucket."""

    def __init__(self, *, bucket: str, client) -> None:
        self._bucket = bucket
        self._client = client

    def _bucket_client(self):
        if hasattr(self._client, "bucket"):
            return self._client.bucket(self._bucket)
        return self._client

    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        blob = self._bucket_client().blob(k)
        try:
            blob.reload(**_gcs_retry_kwargs(blob.reload))
        except Exception as exc:
            if _gcs_missing(exc):
                return None
            raise
        return ObjectStat(
            k,
            int(getattr(blob, "size", 0) or 0),
            _mtime_ms(getattr(blob, "updated", None)),
            str(getattr(blob, "generation", "") or ""),
        )

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def get(self, key: str, *, offset: int = 0, length: int | None = None) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(
        self,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
        chunk_size: int = 1024 * 1024,
    ):
        if offset < 0:
            raise ValueError("offset must be >= 0")
        k = _normalize_key(key)
        blob = self._bucket_client().blob(k)
        try:
            blob.reload(**_gcs_retry_kwargs(blob.reload))
            remaining = max(0, int(getattr(blob, "size", 0) or 0) - offset)
            if length is not None:
                remaining = min(remaining, length)
            while remaining:
                end = offset + min(remaining, chunk_size) - 1
                download_options = {
                    "start": offset,
                    "end": end,
                    "raw_download": True,
                }
                download_options.update(_gcs_retry_kwargs(blob.download_as_bytes))
                chunk = blob.download_as_bytes(**download_options)
                if not chunk:
                    raise IOError("GCS mount stream ended before the object size")
                yield chunk
                amount = len(chunk)
                offset += amount
                remaining -= amount
        except Exception as exc:
            if _gcs_missing(exc):
                raise ObjectNotFound(k) from None
            raise

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = _reject_traversal(prefix)
        bucket = self._bucket_client()
        out: list[ObjectStat] = []
        for blob in bucket.list_blobs(prefix=pfx):
            out.append(ObjectStat(
                blob.name,
                int(getattr(blob, "size", 0) or 0),
                _mtime_ms(getattr(blob, "updated", None)),
                str(getattr(blob, "generation", "") or ""),
            ))
        out.sort(key=lambda item: item.key)
        return out

    def list_dir(self, prefix: str = "") -> list[tuple]:
        pfx = _reject_traversal(prefix)
        if pfx and not pfx.endswith("/"):
            pfx += "/"
        bucket = self._bucket_client()
        iterator = bucket.list_blobs(prefix=pfx, delimiter="/")
        dirs: list[tuple] = []
        files: list[tuple] = []
        plen = len(pfx)
        for blob in iterator:
            name = str(blob.name)
            if name == pfx:
                continue
            child = name[plen:]
            if child and "/" not in child:
                files.append((child, False, int(getattr(blob, "size", 0) or 0),
                              _mtime_ms(getattr(blob, "updated", None))))
        for prefix_name in getattr(iterator, "prefixes", ()) or ():
            name = str(prefix_name)[plen:].rstrip("/")
            if name:
                dirs.append((name, True, 0, None))
        dirs.sort()
        files.sort()
        return dirs + files

    def put(self, key: str, data: bytes) -> ObjectStat:
        raise NotImplementedError("gcs mount is read-only")

    def delete(self, key: str) -> bool:
        raise NotImplementedError("gcs mount is read-only")


def build_gcs_store(mount) -> GCSStore:
    """Construct a read-only GCS mount using ADC or anonymous access."""
    from fabric_shortcut_proxy.storage.gcs_auth import resolve_gcs_auth, validate_gcs_auth

    auth = resolve_gcs_auth(mount)
    problems = validate_gcs_auth(auth)
    if problems:
        raise ValueError("; ".join(problems))
    try:
        from google.cloud import storage
    except ImportError as exc:
        raise RuntimeError(
            "the 'gcs' mount backend needs google-cloud-storage; install "
            "pip install 'fabric-shortcut-proxy[gcsproxy]'"
        ) from exc
    client = (
        storage.Client.create_anonymous_client()
        if auth.mode == "anonymous"
        else storage.Client()
    )
    return GCSStore(bucket=mount.root, client=client)
