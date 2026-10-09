"""Writable Azure Blob artifact store for materialized data and control records."""
from __future__ import annotations

import base64
import contextlib
import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime
from typing import BinaryIO, Iterable

from fabric_shortcut_proxy.runtime.artifact_store import (
    ArtifactStore,
    ObjectConflict,
    ObjectNotFound,
    ObjectStat,
    _STREAM_CHUNK,
    _input_chunks,
    _normalize_key,
)

_LOCK_KEY = "_control/.artifact-store-lock"
_LOCK_DURATION_SECONDS = 60
_LOCK_WAIT_SECONDS = 10
_UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024


def _status_code(exc: Exception) -> int | None:
    value = getattr(exc, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _is_not_found(exc: Exception) -> bool:
    return _status_code(exc) == 404


def _is_conflict(exc: Exception) -> bool:
    return _status_code(exc) in (409, 412)


def _mtime_ms(value) -> int | None:
    return int(value.timestamp() * 1000) if isinstance(value, datetime) else None


class AzureBlobArtifactStore(ArtifactStore):
    """Writable store using conditional Blob operations and a short lease lock.

    The lock serializes compare-and-swap and fenced mutations across processes.
    Fenced operations are limited to small control records; bulk artifacts use
    Blob's block upload commit and are never copied back for hashing.
    """

    def __init__(self, *, container, lock_wait_seconds: float = _LOCK_WAIT_SECONDS):
        self._container = container
        self._lock_wait_seconds = lock_wait_seconds

    def _blob(self, key: str):
        return self._container.get_blob_client(_normalize_key(key))

    @staticmethod
    def _read_blob(blob) -> bytes | None:
        try:
            return blob.download_blob().readall()
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def _read_fence(self, fence_key: str, owner_id: str, fence: int) -> bool:
        raw = self._read_blob(self._blob(fence_key))
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
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
            return False

    @contextlib.contextmanager
    def _mutation_lock(self):
        lock_blob = self._blob(_LOCK_KEY)
        try:
            lock_blob.upload_blob(b"", overwrite=False)
        except Exception as exc:
            if not _is_conflict(exc):
                raise

        deadline = time.monotonic() + self._lock_wait_seconds
        lease = None
        while lease is None:
            try:
                lease = lock_blob.acquire_lease(
                    lease_duration=_LOCK_DURATION_SECONDS
                )
            except Exception as exc:
                if not _is_conflict(exc):
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        "timed out acquiring Azure artifact-store mutation lease"
                    ) from exc
                time.sleep(0.05)
        try:
            yield
        finally:
            lease.release()

    def put(self, key: str, data: bytes) -> ObjectStat:
        k = _normalize_key(key)
        value = bytes(data)
        self._blob(k).upload_blob(
            value,
            overwrite=True,
            metadata={"sha256": hashlib.sha256(value).hexdigest()},
            max_concurrency=4,
        )
        stat = self.head(k)
        if stat is None:
            raise ObjectNotFound(k)
        return stat

    def put_stream(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        k = _normalize_key(key)
        blob = self._blob(k)
        digest = hashlib.sha256()
        total = 0
        block_ids = []
        block_prefix = uuid.uuid4().hex
        pending = bytearray()

        def stage(block: bytes, index: int) -> None:
            block_id = base64.b64encode(
                f"{block_prefix}:{index:08d}".encode("ascii")
            ).decode("ascii")
            blob.stage_block(block_id=block_id, data=block, length=len(block))
            block_ids.append(block_id)

        for chunk in _input_chunks(data):
            pending.extend(chunk)
            total += len(chunk)
            while len(pending) >= _UPLOAD_BLOCK_SIZE:
                block = bytes(pending[:_UPLOAD_BLOCK_SIZE])
                del pending[:_UPLOAD_BLOCK_SIZE]
                digest.update(block)
                stage(block, len(block_ids))
        if pending:
            block = bytes(pending)
            digest.update(block)
            stage(block, len(block_ids))
        if length is not None and total != length:
            raise ValueError(
                f"artifact stream length mismatch: expected {length}, received {total}"
            )
        metadata = {"sha256": digest.hexdigest()}
        if block_ids:
            blob.commit_block_list(block_ids, metadata=metadata)
        else:
            blob.upload_blob(b"", overwrite=True, metadata=metadata)
        stat = self.head(k)
        if stat is None:
            raise ObjectNotFound(k)
        return stat

    def put_stream_if_absent(
        self,
        key: str,
        data: BinaryIO | Iterable[bytes],
        *,
        length: int | None = None,
    ) -> ObjectStat:
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        k = _normalize_key(key)
        blob = self._blob(k)
        digest = hashlib.sha256()
        total = 0
        block_ids = []
        block_prefix = uuid.uuid4().hex
        pending = bytearray()

        def stage(block: bytes, index: int) -> None:
            block_id = base64.b64encode(
                f"{block_prefix}:{index:08d}".encode("ascii")
            ).decode("ascii")
            blob.stage_block(block_id=block_id, data=block, length=len(block))
            block_ids.append(block_id)

        for chunk in _input_chunks(data):
            pending.extend(chunk)
            total += len(chunk)
            while len(pending) >= _UPLOAD_BLOCK_SIZE:
                block = bytes(pending[:_UPLOAD_BLOCK_SIZE])
                del pending[:_UPLOAD_BLOCK_SIZE]
                digest.update(block)
                stage(block, len(block_ids))
        if pending:
            block = bytes(pending)
            digest.update(block)
            stage(block, len(block_ids))
        if length is not None and total != length:
            raise ValueError(
                f"artifact stream length mismatch: expected {length}, received {total}"
            )

        content_hash = digest.hexdigest()
        try:
            if block_ids:
                from azure.core import MatchConditions

                blob.commit_block_list(
                    block_ids,
                    metadata={"sha256": content_hash},
                    etag="*",
                    match_condition=MatchConditions.IfMissing,
                )
            else:
                blob.upload_blob(
                    b"",
                    overwrite=False,
                    metadata={"sha256": content_hash},
                )
        except Exception as exc:
            if not _is_conflict(exc):
                raise
            if self.verify(k, size=total, content_hash=content_hash):
                stat = self.head(k)
                if stat is not None:
                    return stat
            raise ObjectConflict(
                f"immutable artifact key already contains other data: {k}"
            ) from exc
        stat = self.head(k)
        if stat is None:
            raise ObjectNotFound(k)
        return stat

    def get(
        self,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(
        self,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
        chunk_size: int = _STREAM_CHUNK,
    ):
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if length is not None and length < 0:
            raise ValueError("length must be >= 0")
        if chunk_size < 1:
            raise ValueError("chunk_size must be >= 1")
        k = _normalize_key(key)
        options = {}
        if offset:
            options["offset"] = offset
        if length is not None:
            options["length"] = length
        try:
            downloader = self._blob(k).download_blob(**options)
            for chunk in downloader.chunks():
                for start in range(0, len(chunk), chunk_size):
                    yield chunk[start : start + chunk_size]
        except Exception as exc:
            if _is_not_found(exc):
                raise ObjectNotFound(k) from None
            raise

    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        try:
            props = self._blob(k).get_blob_properties()
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        return ObjectStat(
            k,
            int(getattr(props, "size", 0) or 0),
            _mtime_ms(getattr(props, "last_modified", None)),
            str(props.etag) if getattr(props, "etag", None) else None,
        )

    def verify(self, key: str, *, size: int, content_hash: str) -> bool:
        k = _normalize_key(key)
        try:
            props = self._blob(k).get_blob_properties()
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise
        if int(getattr(props, "size", 0) or 0) != size:
            return False
        metadata = getattr(props, "metadata", None) or {}
        stored_hash = next(
            (
                str(value)
                for name, value in metadata.items()
                if str(name).lower() == "sha256"
            ),
            "",
        )
        return bool(stored_hash) and hmac.compare_digest(
            stored_hash.lower(), content_hash.lower()
        )

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def list(self, prefix: str = "") -> list[ObjectStat]:
        pfx = (prefix or "").replace("\\", "/").lstrip("/")
        if any(segment == ".." for segment in pfx.split("/")):
            raise ValueError(f"listing prefix must not contain '..': {prefix!r}")
        items = [
            ObjectStat(
                blob.name,
                int(getattr(blob, "size", 0) or 0),
                _mtime_ms(getattr(blob, "last_modified", None)),
                str(blob.etag) if getattr(blob, "etag", None) else None,
            )
            for blob in self._container.list_blobs(name_starts_with=pfx)
        ]
        return sorted(items, key=lambda item: item.key)

    def list_dir(self, prefix: str = "") -> list[tuple]:
        pfx = (prefix or "").replace("\\", "/").strip("/")
        if any(segment == ".." for segment in pfx.split("/")):
            raise ValueError(f"listing prefix must not contain '..': {prefix!r}")
        if pfx:
            pfx += "/"
        plen = len(pfx)
        directories: set[str] = set()
        files: list[tuple] = []
        for blob in self._container.walk_blobs(
            name_starts_with=pfx,
            delimiter="/",
        ):
            child = blob.name[plen:]
            if child.endswith("/"):
                if child[:-1]:
                    directories.add(child[:-1])
            elif child and "/" not in child:
                files.append(
                    (
                        child,
                        False,
                        int(getattr(blob, "size", 0) or 0),
                        _mtime_ms(getattr(blob, "last_modified", None)),
                    )
                )
        return [
            *((name, True, 0, None) for name in sorted(directories)),
            *sorted(files),
        ]

    def delete(self, key: str) -> bool:
        k = _normalize_key(key)
        try:
            self._blob(k).delete_blob()
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise

    def compare_and_swap(
        self,
        key: str,
        expected: bytes | None,
        data: bytes,
    ) -> bool:
        k = _normalize_key(key)
        blob = self._blob(k)
        with self._mutation_lock():
            current = self._read_blob(blob)
            if current != expected:
                return False
            try:
                if current is None:
                    blob.upload_blob(
                        bytes(data),
                        overwrite=False,
                        metadata={"sha256": hashlib.sha256(data).hexdigest()},
                    )
                else:
                    from azure.core import MatchConditions

                    props = blob.get_blob_properties()
                    blob.upload_blob(
                        bytes(data),
                        overwrite=True,
                        etag=props.etag,
                        match_condition=MatchConditions.IfNotModified,
                        metadata={"sha256": hashlib.sha256(data).hexdigest()},
                    )
            except Exception as exc:
                if _is_conflict(exc) or _is_not_found(exc):
                    return False
                raise
            return True

    def fenced_put(
        self,
        fence_key: str,
        owner_id: str,
        fence: int,
        key: str,
        data: bytes,
    ) -> bool:
        with self._mutation_lock():
            if not self._read_fence(fence_key, owner_id, fence):
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
        with self._mutation_lock():
            if not self._read_fence(fence_key, owner_id, fence):
                return None
            return self.delete(key)

    def guarded_put_batch(
        self,
        guard_key: str,
        expected_guard: bytes,
        values: dict[str, bytes],
    ) -> bool:
        with self._mutation_lock():
            if self._read_blob(self._blob(guard_key)) != expected_guard:
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
        with self._mutation_lock():
            if (
                not self._read_fence(fence_key, owner_id, fence)
                or self._read_blob(self._blob(guard_key)) != expected_guard
            ):
                return False
            for key, data in values.items():
                self.put(key, data)
            return True


def build_azure_artifact_store() -> AzureBlobArtifactStore:
    """Build the configured writable Azure Blob store with identity credentials."""
    from azure.storage.blob import BlobServiceClient, ExponentialRetry

    from fabric_shortcut_proxy import config
    from fabric_shortcut_proxy.security.azure_credential import get_credential

    account_url = config.ARTIFACT_STORE_ACCOUNT_URL.rstrip("/")
    container_name = config.ARTIFACT_STORE_CONTAINER.strip()
    if not account_url or not container_name:
        raise ValueError(
            "Azure artifact store requires ARTIFACT_STORE_ACCOUNT_URL and "
            "ARTIFACT_STORE_CONTAINER"
        )
    credential = get_credential(
        config.ARTIFACT_STORE_AUTH_MODE,
        tenant_id=config.ARTIFACT_STORE_TENANT_ID,
        client_id=config.ARTIFACT_STORE_CLIENT_ID,
        token_file=config.ARTIFACT_STORE_TOKEN_FILE,
    )
    service = BlobServiceClient(
        account_url=account_url,
        credential=credential,
        retry_policy=ExponentialRetry(
            initial_backoff=1,
            increment_base=2,
            retry_total=4,
        ),
    )
    return AzureBlobArtifactStore(
        container=service.get_container_client(container_name)
    )
