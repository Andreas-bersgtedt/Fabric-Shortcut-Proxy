"""
Phase 3 storage-proxy tests — native Azure Blob / ADLS Gen2 mount backend.

Covers the outbound-auth parser/validation, the ``AzureBlobStore`` mapped onto a
fake Azure ContainerClient (head / ranged get_stream / list / one-level list_dir /
``..`` confinement / read-only), end-to-end passthrough through ``s3.router`` with
an ``azure`` mount, and the /api/azure-credentials endpoints. No azure SDK or live
backend is required — the client is a stub.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import config
import storage.mounts as mounts
from storage.mounts import Mount
from runtime.artifact_store import ObjectNotFound
from storage.azure_store import AzureBlobStore
from storage import azure_auth
from s3.router import router as s3_router


# ---------------------------------------------------------------------------
# Fake Azure ContainerClient
# ---------------------------------------------------------------------------

class _ResourceNotFoundError(Exception):
    status_code = 404


class _Props:
    def __init__(self, name, size, mtime):
        self.name = name
        self.size = size
        self.last_modified = mtime


class _Prefix:
    def __init__(self, name):
        self.name = name          # ends with "/"


class _Downloader:
    def __init__(self, data: bytes):
        self._data = data

    def chunks(self):
        for i in range(0, len(self._data), 512):
            yield self._data[i:i + 512]


class _BlobClient:
    def __init__(self, parent, key):
        self._parent = parent
        self._key = key

    def get_blob_properties(self):
        if self._key not in self._parent._objs:
            raise _ResourceNotFoundError(self._key)
        return _Props(self._key, len(self._parent._objs[self._key]), self._parent._mtime)


class FakeContainerClient:
    def __init__(self, objects: dict[str, bytes]):
        self._objs = dict(objects)
        self._mtime = datetime(2026, 7, 30, tzinfo=timezone.utc)

    def get_blob_client(self, key):
        return _BlobClient(self, key)

    def download_blob(self, key, offset=None, length=None):
        if key not in self._objs:
            raise _ResourceNotFoundError(key)
        data = self._objs[key]
        if offset is not None:
            end = (offset + length) if length is not None else len(data)
            data = data[offset:end]
        return _Downloader(data)

    def list_blobs(self, name_starts_with=""):
        for k in sorted(self._objs):
            if k.startswith(name_starts_with):
                yield _Props(k, len(self._objs[k]), self._mtime)

    def walk_blobs(self, name_starts_with="", delimiter="/"):
        seen_dirs: set[str] = set()
        for k in sorted(self._objs):
            if not k.startswith(name_starts_with):
                continue
            rest = k[len(name_starts_with):]
            cut = rest.find(delimiter)
            if cut == -1:
                yield _Props(k, len(self._objs[k]), self._mtime)
            else:
                d = name_starts_with + rest[: cut + 1]
                if d not in seen_dirs:
                    seen_dirs.add(d)
                    yield _Prefix(d)


def _store(objects, *, container="up"):
    return AzureBlobStore(container=container, client=FakeContainerClient(objects))


# ---------------------------------------------------------------------------
# Auth parsing / validation
# ---------------------------------------------------------------------------

def test_parse_infers_mode_from_field():
    assert azure_auth.parse_azure_auth({"connection_string": "x"}).mode == "connection_string"
    assert azure_auth.parse_azure_auth({"account_key": "x"}).mode == "account_key"
    assert azure_auth.parse_azure_auth({"sas_token": "?sv=x"}).mode == "sas"
    assert azure_auth.parse_azure_auth({"sas_token": "?sv=x"}).sas_token == "sv=x"   # leading ? stripped


def test_validate_flags_missing_fields():
    assert azure_auth.validate_azure_auth(azure_auth.parse_azure_auth({"mode": "account_key"}))
    assert not azure_auth.validate_azure_auth(
        azure_auth.parse_azure_auth({"mode": "account_key", "account_key": "K"}))
    assert azure_auth.validate_azure_auth(azure_auth.parse_azure_auth({"mode": "aad_client_secret"}))
    assert not azure_auth.validate_azure_auth(azure_auth.parse_azure_auth(
        {"mode": "aad_client_secret", "tenant_id": "t", "client_id": "c", "client_secret": "s"}))
    assert not azure_auth.validate_azure_auth(azure_auth.parse_azure_auth({"mode": "default"}))
    assert not azure_auth.validate_azure_auth(azure_auth.parse_azure_auth({"mode": "anonymous"}))
    assert azure_auth.validate_azure_auth(azure_auth.parse_azure_auth({"mode": "bogus"}))


def test_all_coverage_modes_recognized():
    for mode in ("connection_string", "account_key", "sas",
                 "aad_client_secret", "managed_identity", "default", "anonymous"):
        assert mode in azure_auth.SUPPORTED_MODES


def test_account_url_derivation():
    m = Mount("b", "azure", root="c", account="acct")
    assert azure_auth.account_url_for(azure_auth.parse_azure_auth({"mode": "default"}),
                                      azure_auth.options_from_mount(m)) == "https://acct.blob.core.windows.net"
    m2 = Mount("b", "azure", root="c", endpoint="https://acct.blob.core.chinacloudapi.cn")
    assert "chinacloudapi" in azure_auth.account_url_for(
        azure_auth.parse_azure_auth({"mode": "default"}), azure_auth.options_from_mount(m2))


def test_resolve_requires_credential_or_auth():
    with pytest.raises(KeyError):
        azure_auth.resolve_azure_auth(Mount("b", "azure", root="c", account="a"))
    assert azure_auth.resolve_azure_auth(
        Mount("b", "azure", root="c", account="a", auth="default")).mode == "default"


def test_resolve_reads_credential_store():
    class _Store:
        def get_secret(self, sid):
            return {"mode": "account_key", "account_key": "K"} if sid == "az1" else None

    auth = azure_auth.resolve_azure_auth(
        Mount("b", "azure", root="c", account="a", credential="az1"), store=_Store())
    assert auth.mode == "account_key" and auth.account_key == "K"


# ---------------------------------------------------------------------------
# AzureBlobStore
# ---------------------------------------------------------------------------

def test_head_and_missing():
    store = _store({"a/b.txt": b"hello"})
    st = store.head("a/b.txt")
    assert st is not None and st.size == 5 and st.mtime_ms and st.mtime_ms > 0
    assert store.head("nope.txt") is None
    assert store.exists("a/b.txt") and not store.exists("nope.txt")


def test_get_stream_full_and_range():
    payload = bytes(range(256)) * 40
    store = _store({"a/b.bin": payload})
    assert b"".join(store.get_stream("a/b.bin")) == payload
    assert b"".join(store.get_stream("a/b.bin", offset=100, length=50)) == payload[100:150]


def test_get_stream_missing_raises():
    store = _store({})
    with pytest.raises(ObjectNotFound):
        list(store.get_stream("nope.bin"))


def test_list_and_list_dir_one_level():
    store = _store({"top.txt": b"t", "sub/data.bin": b"x", "sub/deep/y.txt": b"y"})
    assert {s.key for s in store.list("")} == {"top.txt", "sub/data.bin", "sub/deep/y.txt"}
    entries = {name: is_dir for name, is_dir, *_ in store.list_dir("")}
    assert entries == {"sub": True, "top.txt": False}
    lvl = {name for name, *_ in store.list_dir("sub/")}
    assert lvl == {"data.bin", "deep"}


def test_traversal_rejected():
    store = _store({"a.txt": b"x"})
    with pytest.raises(ValueError):
        store.head("../secrets.txt")
    with pytest.raises(ValueError):
        store.list_dir("../")


def test_read_only():
    store = _store({})
    with pytest.raises(NotImplementedError):
        store.put("k", b"x")
    with pytest.raises(NotImplementedError):
        store.delete("k")


# ---------------------------------------------------------------------------
# End-to-end through s3.router with an azure mount
# ---------------------------------------------------------------------------

@pytest.fixture
def az_proxy_app(monkeypatch):
    objs = {"readme.txt": b"hello world", "sub/data.bin": bytes(range(256)) * 8}
    fake = FakeContainerClient(objs)

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS",
                        {"blobvault": Mount("blobvault", "azure", root="up", account="acct", auth="anonymous")})
    monkeypatch.setattr(mounts, "_backends", {"blobvault": AzureBlobStore(container="up", client=fake)})

    app = FastAPI()
    app.include_router(s3_router)
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_azure_passthrough_list_and_get(az_proxy_app):
    async with _client(az_proxy_app) as c:
        r = await c.get("/blobvault?list-type=2&delimiter=/")
        assert r.status_code == 200
        assert "readme.txt" in r.text and "<CommonPrefixes>" in r.text and "sub/" in r.text

        g = await c.get("/blobvault/readme.txt")
        assert g.status_code == 200 and g.content == b"hello world"

        gr = await c.get("/blobvault/readme.txt", headers={"Range": "bytes=0-4"})
        assert gr.status_code == 206 and gr.content == b"hello"


async def test_azure_passthrough_missing_key_404(az_proxy_app):
    async with _client(az_proxy_app) as c:
        g = await c.get("/blobvault/nope.txt")
        assert g.status_code == 404 and "NoSuchKey" in g.text


async def test_azure_mount_advertised_in_listbuckets(az_proxy_app):
    async with _client(az_proxy_app) as c:
        r = await c.get("/")
        assert r.status_code == 200 and "blobvault" in r.text


# ---------------------------------------------------------------------------
# Azure credential endpoints + mount persistence/validation
# ---------------------------------------------------------------------------

@pytest.fixture
def cb_app(tmp_path, monkeypatch):
    from fastapi import FastAPI as _FastAPI
    from configbuilder.router import router as cb_router
    monkeypatch.setattr(config, "ENABLE_CREDENTIAL_STORE", True, raising=False)
    monkeypatch.setattr(config, "CREDENTIAL_STORE_PATH", str(tmp_path / "credentials.json"), raising=False)
    a = _FastAPI()
    a.include_router(cb_router)
    return a


async def test_azure_credential_save_list_resolve_delete(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/azure-credentials", json={
            "credential_id": "blobvault",
            "auth": {"mode": "account_key", "account_key": "BASE64KEY=="}})
        d = r.json()
        assert d["ok"] is True and d["mode"] == "account_key"

        lst = (await c.get("/_config/api/azure-credentials")).json()
        assert lst["ok"] is True and "blobvault" in lst["ids"]

        store = CredentialStore(str(tmp_path / "credentials.json"))
        auth = azure_auth.resolve_azure_auth(
            Mount("blobvault", "azure", root="up", account="a", credential="blobvault"), store=store)
        assert auth.mode == "account_key" and auth.account_key == "BASE64KEY=="

        del_resp = (await c.request("DELETE", "/_config/api/azure-credentials/blobvault")).json()
        assert del_resp["ok"] is True and del_resp["removed"] is True


async def test_azure_credential_rejects_invalid(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/azure-credentials", json={
            "credential_id": "bad", "auth": {"mode": "aad_client_secret"}})   # missing tenant/client/secret
        assert r.status_code == 400 and r.json()["ok"] is False


async def test_azure_mount_save_persists_knobs_and_validates(cb_app, tmp_path, monkeypatch):
    monkeypatch.setenv("MOUNTS_CONFIG_FILE", str(tmp_path / "config.mounts.json"))
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/mounts", json={"mounts": [{
            "bucket": "blobvault", "backend": "azure", "root": "reports",
            "account": "mystorageacct", "credential": "blobvault", "prefix": "2026"}]})
        d = r.json()
        assert d["ok"] is True and d["count"] == 1
        written = (tmp_path / "config.mounts.json").read_text(encoding="utf-8")
        assert "mystorageacct" in written and "reports" in written

        # azure mount missing both credential and auth is rejected.
        r2 = await c.post("/_config/api/mounts", json={"mounts": [{
            "bucket": "noauth", "backend": "azure", "root": "c", "account": "a"}]})
        assert r2.status_code == 400 and "credential" in " ".join(r2.json()["errors"]).lower()

        # azure mount missing both account and endpoint (non-conn-string) is rejected.
        r3 = await c.post("/_config/api/mounts", json={"mounts": [{
            "bucket": "noacct", "backend": "azure", "root": "c", "auth": "default"}]})
        assert r3.status_code == 400 and "account" in " ".join(r3.json()["errors"]).lower()
