"""Google Cloud Storage mount backend tests using an SDK-style fake."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from fabric_shortcut_proxy.runtime.artifact_store import ObjectNotFound
from fabric_shortcut_proxy.storage import gcs_auth
from fabric_shortcut_proxy.storage.gcs_store import GCSStore
from fabric_shortcut_proxy.storage.mounts import Mount
import fabric_shortcut_proxy.storage.mounts as mounts


class _NotFound(Exception):
    code = 404


class _Blob:
    def __init__(self, bucket, name, data=None):
        self._bucket = bucket
        self.name = name
        self.size = len(data) if data is not None else 0
        self.updated = datetime(2026, 7, 30, tzinfo=UTC)
        self.generation = "123"
        self._data = data

    def reload(self):
        if self._data is None:
            raise _NotFound()

    def download_as_bytes(self, start=0, end=None, raw_download=True):
        if self._data is None:
            raise _NotFound()
        if end is None:
            end = len(self._data) - 1
        return self._data[start:end + 1]


class _BlobIterator(list):
    def __init__(self, values, prefixes=()):
        super().__init__(values)
        self.prefixes = set(prefixes)


class FakeGCSBucket:
    def __init__(self, objects=None):
        self._objects = dict(objects or {})

    def blob(self, name):
        data = self._objects.get(name)
        return _Blob(self, name, data)

    def list_blobs(self, prefix="", delimiter=None):
        matches = sorted(name for name in self._objects if name.startswith(prefix))
        if delimiter:
            files = []
            prefixes = set()
            for name in matches:
                rest = name[len(prefix):]
                cut = rest.find(delimiter)
                if cut == -1:
                    files.append(name)
                else:
                    prefixes.add(prefix + rest[:cut + 1])
            return _BlobIterator(
                [_Blob(self, name, self._objects[name]) for name in files],
                prefixes,
            )
        return _BlobIterator(
            [_Blob(self, name, self._objects[name]) for name in matches]
        )


class FakeGCSClient:
    def __init__(self, bucket):
        self._bucket = bucket

    def bucket(self, name):
        assert name == "upstream"
        return self._bucket


def _store(objects):
    return GCSStore(bucket="upstream", client=FakeGCSClient(FakeGCSBucket(objects)))


def test_gcs_auth_defaults_to_adc_and_rejects_inline_credentials():
    assert gcs_auth.resolve_gcs_auth(Mount("b", "gcs", root="upstream")).mode == "adc"
    assert gcs_auth.resolve_gcs_auth(
        Mount("b", "gcs", root="upstream", auth="anonymous")
    ).mode == "anonymous"
    with pytest.raises(KeyError, match="ADC/workload identity"):
        gcs_auth.resolve_gcs_auth(
            Mount("b", "gcs", root="upstream", credential="gcs-key")
        )


def test_gcs_store_head_get_list_and_range():
    payload = bytes(range(256)) * 40
    store = _store({
        "top.txt": b"hello",
        "sub/a.bin": payload,
        "sub/deep/b.bin": b"nested",
    })
    head = store.head("top.txt")
    assert head is not None and head.size == 5
    assert store.head("missing.txt") is None
    assert b"".join(store.get_stream("sub/a.bin")) == payload
    assert b"".join(store.get_stream("sub/a.bin", offset=100, length=50)) == payload[100:150]
    assert {item.key for item in store.list("")} == {
        "top.txt", "sub/a.bin", "sub/deep/b.bin",
    }
    entries = {name: is_dir for name, is_dir, *_ in store.list_dir("")}
    assert entries == {"sub": True, "top.txt": False}
    assert {name for name, *_ in store.list_dir("sub/")} == {"a.bin", "deep"}


def test_gcs_store_missing_and_traversal_and_read_only():
    store = _store({"a.txt": b"x"})
    with pytest.raises(ObjectNotFound):
        list(store.get_stream("missing.txt"))
    with pytest.raises(ValueError):
        store.head("../secrets.txt")
    with pytest.raises(ValueError):
        store.list_dir("../")
    with pytest.raises(NotImplementedError):
        store.put("a.txt", b"changed")
    with pytest.raises(NotImplementedError):
        store.delete("a.txt")


def test_gcs_mount_is_registered_and_validated(monkeypatch):
    assert "gcs" in mounts._SUPPORTED_BACKENDS
    mount = mounts._mount_from_json({
        "bucket": "gcs-exposed",
        "backend": "gcs",
        "root": "upstream",
        "prefix": "curated",
        "auth": "adc",
    })
    assert mount.backend == "gcs"
    assert mount.root == "upstream"
    assert mount.prefix == "curated/"

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS", {"gcs-exposed": mount})
    assert mounts.validate_mounts() == []


def test_gcs_mount_config_rejects_inline_credential_and_bad_auth():
    from fabric_shortcut_proxy.configbuilder.router import _validate_mounts_payload

    clean, errors = _validate_mounts_payload([{
        "bucket": "gcs-exposed",
        "backend": "gcs",
        "root": "upstream",
        "auth": "adc",
    }])
    assert clean == [{
        "bucket": "gcs-exposed",
        "backend": "gcs",
        "root": "upstream",
        "prefix": "",
        "read_only": True,
        "credential": "",
        "auth": "adc",
    }]
    assert errors == []

    _, errors = _validate_mounts_payload([{
        "bucket": "gcs-exposed",
        "backend": "gcs",
        "root": "upstream",
        "credential": "inline-secret",
        "auth": "adc",
    }])
    assert any("ADC/workload identity" in error for error in errors)
