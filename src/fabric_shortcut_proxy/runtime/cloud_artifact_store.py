"""Cloud data-plane stores. Multi-object Manager control mutations are unsupported."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import hashlib
import tempfile
import time
from typing import BinaryIO, Iterable

from fabric_shortcut_proxy.observability.logging import get_logger
from fabric_shortcut_proxy.runtime.artifact_store import (
    ArtifactStore, ObjectConflict, ObjectNotFound, ObjectStat,
    _input_chunks, _normalize_key, _STREAM_CHUNK,
)
from fabric_shortcut_proxy.storage.s3_store import _is_not_found, _reject_traversal

log = get_logger(__name__)
_UPLOAD_CHUNK = 8 * 1024 * 1024


def _mtime(value) -> int | None:
    return int(value.timestamp() * 1000) if isinstance(value, datetime) else None


def _conflict(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("ResponseMetadata", {}).get("HTTPStatusCode") in {409, 412}
    return getattr(exc, "code", None) in {409, 412}


def _gcs_missing(exc: Exception) -> bool:
    return getattr(exc, "code", None) == 404


@contextmanager
def _staged_stream(data: BinaryIO | Iterable[bytes], length: int | None):
    if length is not None and length < 0:
        raise ValueError("length must be >= 0")
    with tempfile.TemporaryFile() as staged:
        digest = hashlib.sha256()
        size = 0
        for chunk in _input_chunks(data):
            staged.write(chunk)
            digest.update(chunk)
            size += len(chunk)
        if length is not None and size != length:
            raise ValueError(f"artifact stream length mismatch: expected {length}, received {size}")
        staged.seek(0)
        yield staged, size, digest.hexdigest()


class S3ArtifactStore(ArtifactStore):
    """Writable S3 objects with bounded-memory multipart uploads and native CAS."""

    def __init__(self, *, bucket: str, client, prefix: str = "") -> None:
        if not bucket:
            raise ValueError("S3 artifact bucket must be non-empty")
        self._bucket = bucket
        self._client = client
        self._prefix = _reject_traversal(prefix).strip("/")
        if self._prefix:
            self._prefix += "/"

    def _key(self, key: str) -> str:
        return self._prefix + _normalize_key(key)

    def put(self, key: str, data: bytes) -> ObjectStat:
        return self.put_stream(key, [data], length=len(data))

    def _upload(self, key, stream, size, digest, *, if_absent=False):
        options = {"Bucket": self._bucket, "Key": self._key(key)}
        condition = {"IfNoneMatch": "*"} if if_absent else {}
        if size <= _UPLOAD_CHUNK:
            self._client.put_object(
                **options, Body=stream.read(_UPLOAD_CHUNK),
                Metadata={"sha256": digest}, **condition,
            )
            return
        upload_id = self._client.create_multipart_upload(
            **options, Metadata={"sha256": digest}
        )["UploadId"]
        try:
            parts = []
            while chunk := stream.read(_UPLOAD_CHUNK):
                number = len(parts) + 1
                if number > 10_000:
                    raise ValueError("S3 artifact exceeds the 10000-part upload limit")
                result = self._client.upload_part(
                    **options, UploadId=upload_id, PartNumber=number, Body=chunk,
                )
                parts.append({"PartNumber": number, "ETag": result["ETag"]})
            self._client.complete_multipart_upload(
                **options, UploadId=upload_id, MultipartUpload={"Parts": parts}, **condition,
            )
        except Exception:
            try:
                self._client.abort_multipart_upload(**options, UploadId=upload_id)
            except Exception as cleanup_error:
                log.error("artifact_multipart_abort_failed", key=key, detail=str(cleanup_error))
            raise

    def _put(self, key, data, length, *, if_absent):
        k = _normalize_key(key)
        started = time.perf_counter()
        committed = False
        with _staged_stream(data, length) as (staged, size, digest):
            try:
                self._upload(k, staged, size, digest, if_absent=if_absent)
                committed = True
            except Exception as exc:
                if not if_absent or not _conflict(exc):
                    raise
                if not self.verify(k, size=size, content_hash=digest):
                    raise ObjectConflict(f"immutable artifact key already contains other data: {k}") from exc
        stat = self.head(k)
        if stat is None:
            raise ObjectNotFound(k)
        if committed:
            from fabric_shortcut_proxy.observability import metrics
            metrics.record_artifact_upload(size, time.perf_counter() - started)
        return stat

    def put_stream(self, key, data, *, length=None) -> ObjectStat:
        return self._put(key, data, length, if_absent=False)

    def put_stream_if_absent(self, key, data, *, length=None) -> ObjectStat:
        return self._put(key, data, length, if_absent=True)

    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        try:
            result = self._client.head_object(Bucket=self._bucket, Key=self._key(k))
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise
        return ObjectStat(k, int(result["ContentLength"]), _mtime(result.get("LastModified")), result.get("ETag"))

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def get(self, key: str, *, offset=0, length=None) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(self, key, *, offset=0, length=None, chunk_size=_STREAM_CHUNK):
        if offset < 0 or (length is not None and length < 0) or chunk_size < 1:
            raise ValueError("invalid artifact read range or chunk size")
        k = _normalize_key(key)
        options = {"Bucket": self._bucket, "Key": self._key(k)}
        if offset or length is not None:
            stat = self.head(k)
            if stat is None:
                raise ObjectNotFound(k)
            if length == 0 or offset >= stat.size:
                return
            end = "" if length is None else min(stat.size, offset + length) - 1
            options["Range"] = f"bytes={offset}-{end}"
        try:
            result = self._client.get_object(**options)
        except Exception as exc:
            if _is_not_found(exc):
                raise ObjectNotFound(k) from None
            raise
        body = result["Body"]
        try:
            yield from body.iter_chunks(chunk_size)
        finally:
            body.close()

    def list(self, prefix: str = "") -> list[ObjectStat]:
        options = {"Bucket": self._bucket, "Prefix": self._prefix + _reject_traversal(prefix)}
        results = []
        while True:
            page = self._client.list_objects_v2(**options)
            for item in page.get("Contents", []):
                results.append(ObjectStat(
                    item["Key"][len(self._prefix):], int(item["Size"]), _mtime(item.get("LastModified")),
                ))
            if not page.get("IsTruncated"):
                return sorted(results, key=lambda item: item.key)
            token = page.get("NextContinuationToken")
            if not token or token == options.get("ContinuationToken"):
                raise RuntimeError("S3 returned an invalid listing continuation token")
            options["ContinuationToken"] = token

    def delete(self, key: str) -> bool:
        if self.head(key) is None:
            return False
        self._client.delete_object(Bucket=self._bucket, Key=self._key(key))
        return True

    def compare_and_swap(self, key: str, expected: bytes | None, data: bytes) -> bool:
        options = {"Bucket": self._bucket, "Key": self._key(key)}
        try:
            result = self._client.get_object(**options)
        except Exception as exc:
            if not _is_not_found(exc):
                raise
            if expected is not None:
                return False
            condition = {"IfNoneMatch": "*"}
        else:
            body = result["Body"]
            try:
                current = body.read()
            finally:
                body.close()
            if current != expected:
                return False
            condition = {"IfMatch": result["ETag"]}
        try:
            self._client.put_object(
                **options, Body=data, Metadata={"sha256": hashlib.sha256(data).hexdigest()}, **condition,
            )
            return True
        except Exception as exc:
            if _conflict(exc):
                return False
            raise


class GCSArtifactStore(ArtifactStore):
    """GCS objects with generation preconditions, ADC and resumable uploads."""

    def __init__(self, *, bucket, prefix: str = "") -> None:
        self._bucket = bucket
        self._prefix = _reject_traversal(prefix).strip("/")
        if self._prefix:
            self._prefix += "/"

    def _blob(self, key):
        return self._bucket.blob(self._prefix + _normalize_key(key))

    def put(self, key: str, data: bytes) -> ObjectStat:
        return self.put_stream(key, [data], length=len(data))

    def _put(self, key, data, length, *, if_absent):
        k = _normalize_key(key)
        started = time.perf_counter()
        committed = False
        blob = self._blob(k)
        with _staged_stream(data, length) as (staged, size, digest):
            blob.metadata = {"sha256": digest}
            options = {"if_generation_match": 0} if if_absent else {}
            try:
                with blob.open("wb", chunk_size=_UPLOAD_CHUNK, **options) as writer:
                    while chunk := staged.read(_UPLOAD_CHUNK):
                        writer.write(chunk)
                committed = True
            except Exception as exc:
                if not if_absent or not _conflict(exc):
                    raise
                if not self.verify(k, size=size, content_hash=digest):
                    raise ObjectConflict(f"immutable artifact key already contains other data: {k}") from exc
        stat = self.head(k)
        if stat is None:
            raise ObjectNotFound(k)
        if committed:
            from fabric_shortcut_proxy.observability import metrics
            metrics.record_artifact_upload(size, time.perf_counter() - started)
        return stat

    def put_stream(self, key, data, *, length=None) -> ObjectStat:
        return self._put(key, data, length, if_absent=False)

    def put_stream_if_absent(self, key, data, *, length=None) -> ObjectStat:
        return self._put(key, data, length, if_absent=True)

    def head(self, key: str) -> ObjectStat | None:
        k = _normalize_key(key)
        blob = self._blob(k)
        try:
            blob.reload()
        except Exception as exc:
            if _gcs_missing(exc):
                return None
            raise
        return ObjectStat(k, int(blob.size), _mtime(blob.updated), str(blob.generation))

    def exists(self, key: str) -> bool:
        return self.head(key) is not None

    def get(self, key: str, *, offset=0, length=None) -> bytes:
        return b"".join(self.get_stream(key, offset=offset, length=length))

    def get_stream(self, key, *, offset=0, length=None, chunk_size=_STREAM_CHUNK):
        if offset < 0 or (length is not None and length < 0) or chunk_size < 1:
            raise ValueError("invalid artifact read range or chunk size")
        blob = self._blob(key)
        try:
            blob.reload()
            remaining = max(0, int(blob.size) - offset)
            if length is not None:
                remaining = min(remaining, length)
            if remaining == 0:
                return
            with blob.open("rb", chunk_size=_STREAM_CHUNK, raw_download=True) as reader:
                reader.seek(offset)
                while remaining:
                    chunk = reader.read(min(remaining, chunk_size))
                    if not chunk:
                        raise IOError("GCS artifact stream ended before its declared size")
                    remaining -= len(chunk)
                    yield chunk
        except Exception as exc:
            if _gcs_missing(exc):
                raise ObjectNotFound(_normalize_key(key)) from None
            raise

    def list(self, prefix: str = "") -> list[ObjectStat]:
        return sorted([
            ObjectStat(
                blob.name[len(self._prefix):], int(blob.size), _mtime(blob.updated), str(blob.generation),
            )
            for blob in self._bucket.list_blobs(prefix=self._prefix + _reject_traversal(prefix))
        ], key=lambda item: item.key)

    def delete(self, key: str) -> bool:
        try:
            self._blob(key).delete()
            return True
        except Exception as exc:
            if _gcs_missing(exc):
                return False
            raise

    def compare_and_swap(self, key: str, expected: bytes | None, data: bytes) -> bool:
        blob = self._blob(key)
        try:
            blob.reload()
        except Exception as exc:
            if not _gcs_missing(exc):
                raise
            if expected is not None:
                return False
            generation = 0
        else:
            generation = int(blob.generation)
            try:
                current = blob.download_as_bytes(if_generation_match=generation, raw_download=True)
            except Exception as exc:
                if _conflict(exc) or _gcs_missing(exc):
                    return False
                raise
            if current != expected:
                return False
        try:
            blob.metadata = {"sha256": hashlib.sha256(data).hexdigest()}
            blob.upload_from_string(data, if_generation_match=generation)
            return True
        except Exception as exc:
            if _conflict(exc):
                return False
            raise


def build_cloud_data_store(provider: str, *, bucket: str, prefix: str = "", region: str = "") -> ArtifactStore:
    """Use the SDK workload-identity chain, never a secret embedded in a profile."""
    if not bucket:
        raise ValueError("cloud artifact bucket must be non-empty")
    _reject_traversal(prefix)
    if provider == "s3":
        import boto3
        return S3ArtifactStore(
            bucket=bucket, prefix=prefix, client=boto3.client("s3", region_name=region or None),
        )
    if provider == "gcs":
        from google.cloud import storage
        return GCSArtifactStore(bucket=storage.Client().bucket(bucket), prefix=prefix)
    raise ValueError(f"unsupported cloud data store provider: {provider}")
