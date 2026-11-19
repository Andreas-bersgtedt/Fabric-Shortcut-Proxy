from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import io
from pathlib import Path

import pytest

from fabric_shortcut_proxy.runtime.artifact_store import (
    LocalDirStore, MemoryStore, ObjectConflict, ObjectNotFound,
)
from fabric_shortcut_proxy.runtime.azure_artifact_store import AzureBlobArtifactStore
from fabric_shortcut_proxy.runtime.cloud_artifact_store import (
    GCSArtifactStore,
    S3ArtifactStore,
    _conflict,
    _gcs_missing,
)
from tests.cloud_artifact_fakes import (
    CloudError,
    FakeGCSBlob,
    FakeGCSBucket,
    FakeS3,
)


def test_gcs_sdk_status_errors_are_classified():
    class InvalidResponse(Exception):
        pass

    assert _conflict(InvalidResponse("Request failed with status code", 412))
    assert not _conflict(InvalidResponse("Request failed with status code", 500))
    assert _gcs_missing(InvalidResponse("Request failed with status code", 404))
from tests.live_artifact_support import (
    ScopedAzureContainer, cleanup_azure, cleanup_cloud, live_prefix, live_store,
    selected_live_providers,
)
from tests.test_azure_artifact_store import _Container


def test_s3_sdk_has_native_conditional_put_and_multipart_completion():
    session = pytest.importorskip("botocore.session")
    model = session.Session().get_service_model("s3")
    for operation in ("PutObject", "CompleteMultipartUpload"):
        assert {"IfMatch", "IfNoneMatch"} <= set(model.operation_model(operation).input_shape.members)


def test_gcs_sdk_retry_is_bounded():
    storage = pytest.importorskip("google.cloud.storage")
    from fabric_shortcut_proxy.runtime.cloud_artifact_store import _gcs_retry_kwargs

    kwargs = _gcs_retry_kwargs(storage.Blob.open)
    assert kwargs
    retry = kwargs["retry"]
    assert retry._deadline == 120.0


@pytest.fixture(params=["memory", "local", "azure", "s3", "gcs", *selected_live_providers()])
def store(request, tmp_path):
    if request.param == "memory":
        yield MemoryStore()
    elif request.param == "local":
        yield LocalDirStore(str(tmp_path))
    elif request.param == "azure":
        pytest.importorskip("azure.core")
        yield AzureBlobArtifactStore(container=_Container())
    elif request.param == "s3":
        yield S3ArtifactStore(bucket="test", client=FakeS3(), prefix="pool-a")
    elif request.param == "gcs":
        yield GCSArtifactStore(bucket=FakeGCSBucket(), prefix="pool-a")
    else:
        with live_store(request.param.removeprefix("live-")) as cloud_store:
            yield cloud_store


@pytest.mark.parametrize("prefix", ["", "/", "../other", "x//other", "test/"])
def test_live_acceptance_requires_a_constrained_prefix(monkeypatch, prefix):
    monkeypatch.setenv("FSP_LIVE_ARTIFACT_PREFIX", prefix)
    with pytest.raises(ValueError):
        live_prefix()


def test_live_acceptance_requires_explicit_known_providers(monkeypatch):
    monkeypatch.delenv("FSP_LIVE_ARTIFACT_PROVIDERS", raising=False)
    assert selected_live_providers() == []
    monkeypatch.setenv("FSP_LIVE_ARTIFACT_PROVIDERS", "s3,gcs")
    assert selected_live_providers() == ["live-s3", "live-gcs"]
    for selection in ("s3,unknown", "s3,s3", "all"):
        monkeypatch.setenv("FSP_LIVE_ARTIFACT_PROVIDERS", selection)
        with pytest.raises(ValueError):
            selected_live_providers()


class ScopedFakeAzureContainer(_Container):
    def list_blobs(self, *, name_starts_with="", include=None):
        committed = list(super().list_blobs(name_starts_with=name_starts_with))
        if not include:
            return committed
        assert include == ["uncommittedblobs"]
        committed_names = {item.name for item in committed}
        uncommitted = [
            type("Blob", (), {"name": name})()
            for name in self.uncommitted
            if name.startswith(name_starts_with) and name not in committed_names
        ]
        return committed + uncommitted

    def delete_blob(self, name):
        if name in self.objects:
            self.get_blob_client(name).delete_blob()
        self.uncommitted.pop(name, None)


def test_live_azure_scope_includes_lock_and_cleanup_only_deletes_its_prefix():
    container = ScopedFakeAzureContainer()
    prefix = "fsp-tests/unique-run/"
    scoped = ScopedAzureContainer(container, prefix)
    store = AzureBlobArtifactStore(container=scoped)
    container.get_blob_client("fsp-tests/unique-run-other/keep").upload_blob(b"keep")
    store.put("g/data", b"value")
    with pytest.raises(ValueError, match="length mismatch"):
        store.put_stream("g/uncommitted", [b"short"], length=99)
    assert store.compare_and_swap("catalog", None, b"record")
    assert {item.key for item in store.list()} == {
        "_control/.artifact-store-lock", "catalog", "g/data",
    }

    cleanup_azure(container, prefix)
    assert set(container.objects) == {"fsp-tests/unique-run-other/keep"}
    assert not container.uncommitted


def test_live_azure_cleanup_rejects_an_out_of_scope_listing():
    class InvalidListing:
        def list_blobs(self, **_kwargs):
            return [type("Blob", (), {"name": "another-prefix/object"})()]

        def delete_blob(self, _name):
            pytest.fail("cleanup must not delete an out-of-scope blob")

    with pytest.raises(RuntimeError, match="escaped"):
        cleanup_azure(InvalidListing(), "fsp-tests/unique-run/")


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_live_cloud_cleanup_preserves_neighboring_prefixes(provider):
    if provider == "s3":
        client = FakeS3()
        scoped = S3ArtifactStore(bucket="b", client=client, prefix="fsp-tests/unique-run")
        neighbor = S3ArtifactStore(bucket="b", client=client, prefix="fsp-tests/unique-run-other")
    else:
        bucket = FakeGCSBucket()
        scoped = GCSArtifactStore(bucket=bucket, prefix="fsp-tests/unique-run")
        neighbor = GCSArtifactStore(bucket=bucket, prefix="fsp-tests/unique-run-other")
    scoped.put("g/data", b"test")
    neighbor.put("g/data", b"keep")
    cleanup_cloud(scoped)
    assert not scoped.list()
    assert neighbor.get("g/data") == b"keep"


@pytest.mark.parametrize("provider", ["azure", "s3", "gcs"])
def test_live_fixture_creates_and_cleans_a_unique_prefix_without_touching_neighbors(
    monkeypatch, provider,
):
    monkeypatch.setenv("FSP_LIVE_ARTIFACT_PREFIX", "fsp-tests")
    if provider == "azure":
        identity = pytest.importorskip("azure.identity")
        blob_sdk = pytest.importorskip("azure.storage.blob")
        container = ScopedFakeAzureContainer()
        container.get_blob_client("fsp-tests/neighbor/keep").upload_blob(b"keep")

        class Credential:
            def close(self):
                pass

        class Service:
            def __init__(self, *, account_url, credential):
                assert account_url == "https://test.blob.core.windows.net"
                assert isinstance(credential, Credential)

            def get_container_client(self, name):
                assert name == "test-container"
                return container

            def close(self):
                pass

        monkeypatch.setattr(identity, "DefaultAzureCredential", Credential)
        monkeypatch.setattr(blob_sdk, "BlobServiceClient", Service)
        monkeypatch.setenv("FSP_LIVE_AZURE_ACCOUNT_URL", "https://test.blob.core.windows.net")
        monkeypatch.setenv("FSP_LIVE_AZURE_CONTAINER", "test-container")
        objects = container.objects
    elif provider == "s3":
        boto3 = pytest.importorskip("boto3")
        sdk_client = FakeS3()
        monkeypatch.setattr(boto3, "client", lambda service, region_name: sdk_client)
        monkeypatch.setenv("FSP_LIVE_S3_BUCKET", "test-bucket")
        monkeypatch.setenv("FSP_LIVE_S3_REGION", "us-east-1")
        neighbor = S3ArtifactStore(bucket="test-bucket", client=sdk_client, prefix="fsp-tests/neighbor")
        neighbor.put("keep", b"keep")
        objects = sdk_client.objects
    else:
        storage = pytest.importorskip("google.cloud.storage")
        bucket = FakeGCSBucket()

        class StorageClient:
            def bucket(self, name):
                assert name == "test-bucket"
                return bucket

        monkeypatch.setattr(storage, "Client", StorageClient)
        monkeypatch.setenv("FSP_LIVE_GCS_BUCKET", "test-bucket")
        neighbor = GCSArtifactStore(bucket=bucket, prefix="fsp-tests/neighbor")
        neighbor.put("keep", b"keep")
        objects = bucket.objects

    with live_store(provider) as test_store:
        test_store.put("data", b"test")
        assert len(objects) == 2
        assert any(key.startswith("fsp-tests/") and key.endswith("/data") for key in objects)
    assert set(objects) == {"fsp-tests/neighbor/keep"}


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


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_verification_uses_properties_without_object_download(
    provider,
    monkeypatch,
):
    body = b"property-verified"
    digest = hashlib.sha256(body).hexdigest()
    if provider == "s3":
        client = FakeS3()
        store = S3ArtifactStore(bucket="b", client=client)
        store.put("g/data", body)
        monkeypatch.setattr(
            client,
            "get_object",
            lambda **_kwargs: pytest.fail(
                "verification downloaded the S3 object"
            ),
        )
    else:
        store = GCSArtifactStore(bucket=FakeGCSBucket())
        store.put("g/data", body)
        monkeypatch.setattr(
            FakeGCSBlob,
            "download_as_bytes",
            lambda *_args, **_kwargs: pytest.fail(
                "verification downloaded the GCS object"
            ),
        )

    assert store.verify(
        "g/data",
        size=len(body),
        content_hash=digest,
    )


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_upload_uses_the_configured_temp_directory(provider, tmp_path, monkeypatch):
    from fabric_shortcut_proxy.runtime import cloud_artifact_store

    store = (
        S3ArtifactStore(bucket="b", client=FakeS3())
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket())
    )
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    temporary_file = cloud_artifact_store.tempfile.NamedTemporaryFile
    files = []

    def open_temporary_file(*args, **kwargs):
        handle = temporary_file(*args, **kwargs)
        files.append(Path(handle.name))
        return handle

    def chunks():
        assert files
        assert files[-1].parent == tmp_path
        assert files[-1].is_file()
        yield b"test"

    monkeypatch.setattr(cloud_artifact_store.tempfile, "NamedTemporaryFile", open_temporary_file)
    store.put_stream("g/data", chunks(), length=4)
    assert len(files) == 1
    assert not files[0].exists()
    assert store.get("g/data") == b"test"
    with pytest.raises(ValueError, match="length mismatch"):
        store.put_stream("g/failed", chunks(), length=99)
    assert len(files) == 2
    assert not files[1].exists()
    assert not store.exists("g/failed")


@pytest.mark.parametrize("provider", ["s3", "gcs"])
def test_cloud_upload_rejects_an_unavailable_temp_directory(provider, tmp_path, monkeypatch):
    store = (
        S3ArtifactStore(bucket="b", client=FakeS3())
        if provider == "s3" else GCSArtifactStore(bucket=FakeGCSBucket())
    )
    monkeypatch.setenv("TMPDIR", str(tmp_path / "missing"))
    with pytest.raises(OSError):
        store.put("g/data", b"test")
    assert not store.exists("g/data")


def test_shared_immutable_put_and_conflict(store):
    store.put_stream_if_absent("g/data", [b"same"])
    store.put_stream_if_absent("g/data", [b"same"])
    with pytest.raises(ObjectConflict):
        store.put_stream_if_absent("g/data", [b"different"])
    assert store.get("g/data") == b"same"


def test_shared_large_immutable_upload_and_retry(store):
    body = b"x" * (8 * 1024 * 1024 + 1)
    digest = hashlib.sha256(body).hexdigest()
    store.put_stream_if_absent("g/large", io.BytesIO(body), length=len(body))
    store.put_stream_if_absent("g/large", io.BytesIO(body), length=len(body))
    with pytest.raises(ObjectConflict):
        store.put_stream_if_absent("g/large", io.BytesIO(body[:-1] + b"y"), length=len(body))
    assert store.verify("g/large", size=len(body), content_hash=digest)
    assert store.get("g/large", offset=len(body) - 8, length=8) == body[-8:]


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


def test_shared_compare_and_swap_race_has_one_winner(store):
    store.put("race", b"original")
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(
            lambda number: store.compare_and_swap("race", b"original", str(number).encode()),
            range(8),
        ))
    assert sum(results) == 1


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
