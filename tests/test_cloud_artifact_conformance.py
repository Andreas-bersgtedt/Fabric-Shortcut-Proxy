from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import io

import pytest

from fabric_shortcut_proxy.runtime.artifact_store import (
    LocalDirStore, MemoryStore, ObjectConflict, ObjectNotFound,
)
from fabric_shortcut_proxy.runtime.azure_artifact_store import AzureBlobArtifactStore
from fabric_shortcut_proxy.runtime.cloud_artifact_store import GCSArtifactStore, S3ArtifactStore
from tests.cloud_artifact_fakes import CloudError, FakeGCSBucket, FakeS3
from tests.test_azure_artifact_store import _Container


def test_s3_sdk_has_native_conditional_put_and_multipart_completion():
    session = pytest.importorskip("botocore.session")
    model = session.Session().get_service_model("s3")
    for operation in ("PutObject", "CompleteMultipartUpload"):
        assert {"IfMatch", "IfNoneMatch"} <= set(model.operation_model(operation).input_shape.members)


@pytest.fixture(params=["memory", "local", "azure", "s3", "gcs"])
def store(request, tmp_path):
    if request.param == "memory":
        return MemoryStore()
    if request.param == "local":
        return LocalDirStore(str(tmp_path))
    if request.param == "azure":
        pytest.importorskip("azure.core")
        return AzureBlobArtifactStore(container=_Container())
    if request.param == "s3":
        return S3ArtifactStore(bucket="test", client=FakeS3(), prefix="pool-a")
    return GCSArtifactStore(bucket=FakeGCSBucket(), prefix="pool-a")


def test_shared_stream_range_digest_and_metadata(store):
    body = b"PAR1" + bytes(range(256)) * 13
    stat = store.put_stream("warehouse/g/data", io.BytesIO(body), length=len(body))
    assert stat.key == "warehouse/g/data"
    assert stat.size == len(body)
    assert store.get(stat.key) == body
    assert store.get(stat.key, offset=3, length=7) == body[3:10]
    assert b"".join(store.get_stream(stat.key, chunk_size=5)) == body
    digest = hashlib.sha256(body).hexdigest()
    assert store.verify(stat.key, size=len(body), content_hash=digest)
    assert not store.verify(stat.key, size=len(body), content_hash="0" * 64)
    assert not store.verify(stat.key, size=len(body) + 1, content_hash=digest)


def test_shared_immutable_put_and_conflict(store):
    store.put_stream_if_absent("g/data", [b"same"])
    store.put_stream_if_absent("g/data", [b"same"])
    with pytest.raises(ObjectConflict):
        store.put_stream_if_absent("g/data", [b"different"])
    assert store.get("g/data") == b"same"


def test_shared_length_failure_never_publishes(store):
    with pytest.raises(ValueError, match="length mismatch"):
        store.put_stream("g/new", [b"short"], length=99)
    assert not store.exists("g/new")
    store.put("g/old", b"old")
    with pytest.raises(ValueError, match="length mismatch"):
        store.put_stream("g/old", [b"new"], length=99)
    assert store.get("g/old") == b"old"


def test_shared_sorted_listing_and_delete(store):
    for key in ["g/b", "g/a", "g/d", "g/c", "other/z"]:
        store.put(key, key.encode())
    assert [stat.key for stat in store.list("g/")] == ["g/a", "g/b", "g/c", "g/d"]
    assert store.delete("g/a")
    assert not store.delete("g/a")
    with pytest.raises(ObjectNotFound):
        store.get("missing")


def test_shared_key_and_prefix_confinement(store):
    for key in ["../escape", "a/../../escape", "..\\escape"]:
        with pytest.raises(ValueError):
            store.put(key, b"bad")
    store.put("/g\\data", b"ok")
    assert store.get("g/data") == b"ok"


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_listing_prefix_is_confined(provider):
    store = (
        S3ArtifactStore(bucket="b", client=FakeS3(), prefix="pool-a")
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket(), prefix="pool-a")
    )
    for key in ["../escape", "a/../../escape", "..\\escape"]:
        with pytest.raises(ValueError):
            store.list(key)
    store.put("data", b"ok")
    assert [item.key for item in store.list()] == ["data"]


def test_shared_compare_and_swap(store):
    assert store.compare_and_swap("catalog", None, b"one")
    assert not store.compare_and_swap("catalog", None, b"wrong")
    assert not store.compare_and_swap("catalog", b"wrong", b"two")
    assert store.compare_and_swap("catalog", b"one", b"two")
    assert store.get("catalog") == b"two"


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_cas_race_has_one_winner(provider):
    store = (
        S3ArtifactStore(bucket="b", client=FakeS3())
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket())
    )
    store.put("catalog", b"original")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda number: store.compare_and_swap("catalog", b"original", str(number).encode()),
            range(8),
        ))
    assert sum(results) == 1
    with pytest.raises(NotImplementedError):
        store.fenced_put("fence", "owner", 1, "key", b"bad")
    with pytest.raises(NotImplementedError):
        store.guarded_put_batch("guard", b"expected", {"key": b"bad"})
    assert not store.exists("key")


def test_s3_stream_closes_response_when_consumer_stops():
    client = FakeS3()
    store = S3ArtifactStore(bucket="b", client=client)
    store.put("g/data", b"abcd")
    stream = store.get_stream("g/data", chunk_size=1)
    assert next(stream) == b"a"
    stream.close()
    assert client.last_body.closed


def test_s3_multipart_is_immutable_and_aborts_failures():
    client = FakeS3()
    store = S3ArtifactStore(bucket="b", client=client)
    body = b"x" * (8 * 1024 * 1024 + 1)
    store.put_stream_if_absent("g/data", [body])
    assert client.completed_options == {"IfNoneMatch": "*"}
    assert not client.uploads
    client.fail_part = True
    with pytest.raises(CloudError) as failure:
        store.put_stream("g/failure", [body])
    assert failure.value.code == 503
    assert not client.uploads
    assert not store.exists("g/failure")


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_empty_and_invalid_ranges(provider):
    store = (
        S3ArtifactStore(bucket="b", client=FakeS3())
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket())
    )
    store.put("empty", b"")
    assert store.get("empty") == b""
    store.put("data", b"x")
    assert store.get("data", length=0) == b""
    assert store.get("data", offset=2) == b""
    for options in [{"offset": -1}, {"length": -1}, {"chunk_size": 0}]:
        with pytest.raises(ValueError):
            list(store.get_stream("data", **options))


def test_s3_non_missing_errors_are_not_hidden(monkeypatch):
    client = FakeS3()
    store = S3ArtifactStore(bucket="b", client=client)
    def forbidden(**kwargs):
        raise CloudError(403)
    monkeypatch.setattr(client, "head_object", forbidden)
    with pytest.raises(CloudError) as failure:
        store.head("data")
    assert failure.value.code == 403


def test_s3_malformed_pagination_fails_instead_of_truncating(monkeypatch):
    client = FakeS3()
    store = S3ArtifactStore(bucket="b", client=client)
    monkeypatch.setattr(client, "list_objects_v2", lambda **kwargs: {"IsTruncated": True})
    with pytest.raises(RuntimeError, match="invalid listing"):
        store.list()


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_upload_reader_is_bounded(provider):
    class BoundedRead(io.BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024
            return super().read(size)
    store = (
        S3ArtifactStore(bucket="b", client=FakeS3())
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket())
    )
    body = b"x" * (8 * 1024 * 1024 + 1)
    store.put_stream("data", BoundedRead(body), length=len(body))
    assert store.get("data") == body
