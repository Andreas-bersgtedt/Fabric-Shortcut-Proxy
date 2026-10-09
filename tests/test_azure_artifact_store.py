from __future__ import annotations

import hashlib
import io
import json
import sys
import threading
import types
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.observability import metrics
from fabric_shortcut_proxy.runtime.artifact_store import ObjectConflict, ObjectNotFound
from fabric_shortcut_proxy.runtime.azure_artifact_store import AzureBlobArtifactStore


class _AzureError(Exception):
    def __init__(self, status_code: int):
        self.status_code = status_code


class _Properties:
    def __init__(self, name: str, data: bytes, etag: str, metadata=None):
        self.name = name
        self.size = len(data)
        self.etag = etag
        self.last_modified = datetime(2026, 10, 9, tzinfo=timezone.utc)
        self.metadata = metadata or {}


class _Downloader:
    def __init__(self, data: bytes):
        self.data = data

    def readall(self) -> bytes:
        return self.data

    def chunks(self):
        yield self.data[:3]
        yield self.data[3:]


class _Lease:
    def __init__(self, container):
        self._container = container

    def release(self):
        self._container.lease_releases += 1
        self._container.lease_active = False


class _Blob:
    def __init__(self, container, key: str):
        self._container = container
        self._key = key

    def upload_blob(
        self,
        data,
        *,
        overwrite=False,
        etag=None,
        match_condition=None,
        length=None,
        metadata=None,
        **kwargs,
    ):
        assert kwargs.get("max_concurrency", 1) >= 1
        current = self._container.objects.get(self._key)
        if not overwrite and current is not None:
            raise _AzureError(409)
        if etag is not None and etag != self._container.etags[self._key]:
            raise _AzureError(412)
        if hasattr(data, "read"):
            value = data.read()
        elif isinstance(data, (bytes, bytearray, memoryview)):
            value = bytes(data)
        else:
            value = b"".join(data)
        if length is not None and len(value) != length:
            raise ValueError("upload length mismatch")
        self._container.objects[self._key] = value
        self._container.metadata[self._key] = dict(metadata or {})
        self._container.next_etag += 1
        self._container.etags[self._key] = f'"{self._container.next_etag}"'
        self._container.last_match_condition = match_condition

    def get_blob_properties(self):
        try:
            data = self._container.objects[self._key]
        except KeyError:
            raise _AzureError(404) from None
        return _Properties(
            self._key,
            data,
            self._container.etags[self._key],
            self._container.metadata[self._key],
        )

    def download_blob(self, *, offset=None, length=None):
        try:
            data = self._container.objects[self._key]
        except KeyError:
            raise _AzureError(404) from None
        if offset is not None or length is not None:
            start = offset or 0
            data = data[start : start + length] if length is not None else data[start:]
        return _Downloader(data)

    def delete_blob(self):
        if self._key not in self._container.objects:
            raise _AzureError(404)
        del self._container.objects[self._key]
        del self._container.etags[self._key]
        del self._container.metadata[self._key]

    def stage_block(self, *, block_id, data, length):
        assert len(data) == length
        self._container.uncommitted.setdefault(self._key, {})[block_id] = bytes(data)

    def commit_block_list(
        self, block_ids, *, metadata=None, etag=None, match_condition=None
    ):
        if self._container.commit_barrier is not None:
            self._container.commit_barrier.wait(timeout=5)
        with self._container.commit_lock:
            self._container.last_match_condition = match_condition
            if (
                getattr(match_condition, "name", "") == "IfMissing"
                and self._key in self._container.objects
            ):
                raise _AzureError(409)
            blocks = self._container.uncommitted.pop(self._key)
            self._container.objects[self._key] = b"".join(
                blocks[block_id] for block_id in block_ids
            )
            self._container.metadata[self._key] = dict(metadata or {})
            self._container.next_etag += 1
            self._container.etags[self._key] = f'"{self._container.next_etag}"'

    def acquire_lease(self, *, lease_duration):
        assert lease_duration == 60
        if self._container.lease_active:
            raise _AzureError(409)
        self._container.lease_active = True
        return _Lease(self._container)


class _Container:
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.etags = {}
        self.uncommitted = {}
        self.next_etag = 0
        self.lease_active = False
        self.lease_releases = 0
        self.last_match_condition = None
        self.commit_lock = threading.Lock()
        self.commit_barrier = None

    def get_blob_client(self, key: str):
        return _Blob(self, key)

    def list_blobs(self, *, name_starts_with=""):
        for key, data in sorted(self.objects.items()):
            if key.startswith(name_starts_with):
                yield _Properties(key, data, self.etags[key])

    def walk_blobs(self, *, name_starts_with="", delimiter="/"):
        seen = set()
        for key, data in sorted(self.objects.items()):
            if not key.startswith(name_starts_with):
                continue
            rest = key[len(name_starts_with):]
            separator = rest.find(delimiter)
            if separator < 0:
                yield _Properties(key, data, self.etags[key])
            else:
                folder = name_starts_with + rest[: separator + 1]
                if folder not in seen:
                    seen.add(folder)
                    yield _Properties(folder, b"", self.etags[key])


@pytest.fixture
def container():
    return _Container()


def _store(container) -> AzureBlobArtifactStore:
    return AzureBlobArtifactStore(container=container)


def _install_match_conditions(monkeypatch, **values):
    match_conditions = types.SimpleNamespace(**values)
    azure_core = types.ModuleType("azure.core")
    setattr(azure_core, "MatchConditions", match_conditions)
    azure_package = sys.modules.get("azure") or types.ModuleType("azure")
    monkeypatch.setitem(sys.modules, "azure", azure_package)
    monkeypatch.setitem(sys.modules, "azure.core", azure_core)
    return match_conditions


def test_blob_store_put_head_delete_and_range(container):
    store = _store(container)
    stat = store.put("warehouse/sales/data.parquet", b"PAR1payload")

    assert stat.key == "warehouse/sales/data.parquet"
    assert stat.size == 11
    assert stat.etag
    assert store.get(stat.key, offset=4, length=7) == b"payload"
    assert b"".join(store.get_stream(stat.key, offset=0, length=4)) == b"PAR1"
    assert store.verify(
        stat.key,
        size=stat.size,
        content_hash=hashlib.sha256(b"PAR1payload").hexdigest(),
    )
    assert not store.verify(
        stat.key,
        size=stat.size,
        content_hash=hashlib.sha256(b"not-the-same").hexdigest(),
    )
    assert store.delete(stat.key)
    assert not store.delete(stat.key)


def test_blob_upload_records_location_bytes_and_duration(
    container, monkeypatch
):
    monkeypatch.setattr(config, "AGENT_LOCATION", "site-a", raising=False)
    metrics.reset()

    _store(container).put("warehouse/sales/data.parquet", b"PAR1payload")

    counters = metrics.snapshot()["counters"]
    byte_series = counters["fsp_artifact_upload_bytes_total"][0]
    duration_series = counters["fsp_artifact_upload_duration_seconds_total"][0]
    assert byte_series["labels"] == {"location": "site-a"}
    assert byte_series["value"] == len(b"PAR1payload")
    assert duration_series["labels"] == {"location": "site-a"}
    assert duration_series["value"] > 0


def test_blob_store_stream_upload_does_not_join_before_sdk(container):
    store = _store(container)
    source = io.BytesIO(b"large-object")

    stat = store.put_stream("large.bin", source, length=12)

    assert stat.size == 12
    assert container.objects["large.bin"] == b"large-object"
    assert store.verify(
        "large.bin",
        size=12,
        content_hash=hashlib.sha256(b"large-object").hexdigest(),
    )


def test_blob_store_stream_length_mismatch_does_not_commit(container):
    store = _store(container)

    with pytest.raises(ValueError, match="length mismatch"):
        store.put_stream("partial.bin", io.BytesIO(b"partial"), length=20)

    assert not store.exists("partial.bin")


def test_blob_store_immutable_stream_upload_is_idempotent_and_rejects_conflicts(
    container, monkeypatch
):
    _install_match_conditions(
        monkeypatch,
        IfMissing=types.SimpleNamespace(name="IfMissing"),
    )
    store = _store(container)
    key = "immutable.bin"

    first = store.put_stream_if_absent(key, io.BytesIO(b"complete-object"))
    retry = store.put_stream_if_absent(key, io.BytesIO(b"complete-object"))

    assert retry.etag == first.etag
    assert container.last_match_condition.name == "IfMissing"
    assert container.objects[key] == b"complete-object"
    with pytest.raises(ObjectConflict, match="immutable artifact key"):
        store.put_stream_if_absent(key, io.BytesIO(b"different-object"))
    assert container.objects[key] == b"complete-object"


def test_concurrent_immutable_publishers_cannot_replace_each_other(
    container, monkeypatch
):
    _install_match_conditions(
        monkeypatch,
        IfMissing=types.SimpleNamespace(name="IfMissing"),
    )
    store = _store(container)
    key = "concurrent-immutable.bin"
    payloads = (b"publisher-one", b"publisher-two")
    container.commit_barrier = threading.Barrier(2)

    def upload(payload):
        try:
            return store.put_stream_if_absent(key, io.BytesIO(payload)).size
        except ObjectConflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(upload, payloads))

    assert outcomes.count("conflict") == 1
    assert container.objects[key] in payloads


def test_interrupted_block_upload_never_exposes_partial_object(container, monkeypatch):
    import fabric_shortcut_proxy.runtime.azure_artifact_store as azure_store

    store = _store(container)
    key = "interrupted.bin"
    blob = store._blob(key)
    original_stage = blob.stage_block
    calls = 0

    def fail_after_first_block(*, block_id, data, length):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated connection loss")
        original_stage(block_id=block_id, data=data, length=length)

    monkeypatch.setattr(azure_store, "_UPLOAD_BLOCK_SIZE", 4)
    monkeypatch.setattr(blob, "stage_block", fail_after_first_block)
    monkeypatch.setattr(store, "_blob", lambda _key: blob)

    with pytest.raises(OSError, match="connection loss"):
        store.put_stream(key, io.BytesIO(b"abcdefghijkl"), length=12)

    assert key not in container.objects
    assert len(container.uncommitted[key]) == 1


def test_interrupted_immutable_upload_never_exposes_partial_object(
    container, monkeypatch
):
    import fabric_shortcut_proxy.runtime.azure_artifact_store as azure_store

    store = _store(container)
    key = "interrupted-immutable.bin"
    blob = store._blob(key)
    original_stage = blob.stage_block
    calls = 0

    def fail_after_first_block(*, block_id, data, length):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated connection loss")
        original_stage(block_id=block_id, data=data, length=length)

    monkeypatch.setattr(azure_store, "_UPLOAD_BLOCK_SIZE", 4)
    monkeypatch.setattr(blob, "stage_block", fail_after_first_block)
    monkeypatch.setattr(store, "_blob", lambda _key: blob)

    with pytest.raises(OSError, match="connection loss"):
        store.put_stream_if_absent(key, io.BytesIO(b"abcdefghijkl"), length=12)

    assert key not in container.objects
    assert len(container.uncommitted[key]) == 1


def test_azure_store_factory_builds_sdk_clients(monkeypatch):
    from fabric_shortcut_proxy import config
    from fabric_shortcut_proxy.runtime.artifact_store import build_store

    container = _Container()
    service_options = {}
    retry_options = {}

    class _RetryPolicy:
        def __init__(self, *, initial_backoff, increment_base, retry_total):
            retry_options.update(
                initial_backoff=initial_backoff,
                increment_base=increment_base,
                retry_total=retry_total,
            )
            self.initial_backoff = initial_backoff
            self.increment_base = increment_base
            self.total_retries = retry_total

    class _BlobServiceClient:
        def __init__(self, *, account_url, credential, retry_policy):
            service_options.update(
                account_url=account_url,
                credential=credential,
                retry_policy=retry_policy,
            )
            self.account_url = account_url
            self.credential = credential
            self.retry_policy = retry_policy

        def get_container_client(self, name):
            assert name == "artifacts"
            return container

    azure_blob = types.ModuleType("azure.storage.blob")
    azure_blob.BlobServiceClient = _BlobServiceClient
    azure_blob.ExponentialRetry = _RetryPolicy
    azure_package = sys.modules.get("azure") or types.ModuleType("azure")
    azure_storage = types.ModuleType("azure.storage")
    monkeypatch.setitem(sys.modules, "azure", azure_package)
    monkeypatch.setitem(sys.modules, "azure.storage", azure_storage)
    monkeypatch.setitem(sys.modules, "azure.storage.blob", azure_blob)

    from fabric_shortcut_proxy.security import azure_credential

    credentials = object()
    monkeypatch.setattr(
        azure_credential,
        "get_credential",
        lambda mode, **kwargs: credentials,
    )

    monkeypatch.setattr(config, "ARTIFACT_STORE_ACCOUNT_URL", "https://unit.blob.core.windows.net")
    monkeypatch.setattr(config, "ARTIFACT_STORE_CONTAINER", "artifacts")
    monkeypatch.setattr(config, "ARTIFACT_STORE_AUTH_MODE", "managed_identity")
    monkeypatch.setattr(config, "ARTIFACT_STORE_CLIENT_ID", "")
    monkeypatch.setattr(config, "ARTIFACT_STORE_TENANT_ID", "")
    monkeypatch.setattr(config, "ARTIFACT_STORE_TOKEN_FILE", "")

    store = build_store("azure")

    assert isinstance(store, AzureBlobArtifactStore)
    assert store._container is container
    assert service_options["account_url"] == "https://unit.blob.core.windows.net"
    assert service_options["credential"] is credentials
    assert retry_options == {
        "retry_total": 4,
        "initial_backoff": 1,
        "increment_base": 2,
    }


def test_blob_store_not_found_and_path_validation(container):
    store = _store(container)
    with pytest.raises(ObjectNotFound):
        store.get("missing")
    assert store.head("missing") is None
    with pytest.raises(ValueError):
        store.put("../escape", b"x")
    with pytest.raises(ValueError):
        store.list("a/../")


def test_blob_store_listing_is_sorted_and_delimiter_aware(container):
    store = _store(container)
    store.put("warehouse/z.parquet", b"z")
    store.put("warehouse/a.parquet", b"a")
    store.put("warehouse/nested/data.parquet", b"d")

    assert [item.key for item in store.list("warehouse/")] == [
        "warehouse/a.parquet",
        "warehouse/nested/data.parquet",
        "warehouse/z.parquet",
    ]
    assert store.list_dir("warehouse") == [
        ("nested", True, 0, None),
        ("a.parquet", False, 1, 1_791_504_000_000),
        ("z.parquet", False, 1, 1_791_504_000_000),
    ]


def test_compare_and_swap_uses_etag_match_condition(container, monkeypatch):
    _install_match_conditions(
        monkeypatch,
        IfNotModified="IfNotModified",
    )
    store = _store(container)

    assert store.compare_and_swap("state.json", None, b"one")
    assert not store.compare_and_swap("state.json", None, b"two")
    assert not store.compare_and_swap("state.json", b"wrong", b"two")
    assert store.compare_and_swap("state.json", b"one", b"two")
    assert container.last_match_condition == "IfNotModified"
    assert container.lease_releases == 4


def test_fenced_mutations_check_current_lease_and_guard(container):
    store = _store(container)
    fence = {
        "owner_id": "manager-a",
        "fence": 4,
        "renew_ms": 1_800_000_000_000,
        "ttl_ms": 60_000,
    }
    store.put("lease.json", json.dumps(fence).encode())
    store.put("guard.json", b"version-1")

    assert store.fenced_put("lease.json", "manager-a", 4, "task.json", b"task")
    assert not store.fenced_put("lease.json", "manager-b", 4, "bad.json", b"bad")
    assert store.fenced_guarded_put_batch(
        "lease.json",
        "manager-a",
        4,
        "guard.json",
        b"version-1",
        {"request.json": b"request"},
    )
    assert not store.fenced_guarded_put_batch(
        "lease.json",
        "manager-a",
        4,
        "guard.json",
        b"stale",
        {"wrong.json": b"wrong"},
    )
    assert store.get("request.json") == b"request"
    assert not store.exists("bad.json")
    assert not store.exists("wrong.json")
    assert container.lease_releases == 4


def test_generation_fence_rejects_expired_or_replaced_generation(container):
    store = _store(container)
    now_ms = int(__import__("time").time() * 1000)
    store.put(
        "coordinator.json",
        json.dumps({
            "generation_id": "generation-a",
            "fence": 1,
            "expires_at_ms": now_ms + 60_000,
        }).encode(),
    )

    assert store.fenced_put(
        "coordinator.json", "generation-a", 1, "staged/a.parquet", b"a"
    )
    store.put(
        "coordinator.json",
        json.dumps({
            "generation_id": "generation-b",
            "fence": 2,
            "expires_at_ms": now_ms + 60_000,
        }).encode(),
    )

    assert not store.fenced_put(
        "coordinator.json", "generation-a", 1, "staged/stale.parquet", b"stale"
    )
    assert not store.exists("staged/stale.parquet")
