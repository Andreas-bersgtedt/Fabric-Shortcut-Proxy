"""
Phase 2 storage-proxy tests — native S3 mount backend.

Covers the outbound-auth parser/validation, the ``S3Store`` mapped onto a fake
boto3 S3 client (head / ranged get_stream / list / one-level list_dir / pagination
/ ``..`` confinement / read-only), and end-to-end passthrough through ``s3.router``
with an ``s3`` mount. No boto3 or live backend is required — the client is a stub.
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
from storage.s3_store import S3Store
from storage import s3_auth
from s3.router import router as s3_router


# ---------------------------------------------------------------------------
# Fake boto3 S3 client
# ---------------------------------------------------------------------------

class _ClientError(Exception):
    """Mimics botocore.exceptions.ClientError enough for S3Store's not-found check."""

    def __init__(self, code: str, status: int = 404):
        super().__init__(code)
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def iter_chunks(self, chunk_size: int):
        for i in range(0, len(self._data), chunk_size):
            yield self._data[i:i + chunk_size]


class FakeS3Client:
    """A minimal in-memory S3 with fixed-size pages to exercise pagination."""

    def __init__(self, objects: dict[str, bytes], *, page_size: int = 1000):
        self._objs = dict(objects)
        self._mtime = datetime(2026, 7, 30, tzinfo=timezone.utc)
        self._page_size = page_size

    def head_object(self, *, Bucket, Key):
        if Key not in self._objs:
            raise _ClientError("404")
        return {"ContentLength": len(self._objs[Key]), "LastModified": self._mtime}

    def get_object(self, *, Bucket, Key, Range=None):
        if Key not in self._objs:
            raise _ClientError("NoSuchKey")
        data = self._objs[Key]
        if Range:
            spec = Range.split("=", 1)[1]
            start_s, end_s = spec.split("-", 1)
            start = int(start_s)
            end = int(end_s) if end_s else len(data) - 1
            data = data[start:end + 1]
        return {"Body": _Body(data), "ContentLength": len(data), "LastModified": self._mtime}

    def list_objects_v2(self, *, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        keys = sorted(k for k in self._objs if k.startswith(Prefix))
        contents_keys: list[str] = []
        common: set[str] = set()
        if Delimiter:
            for k in keys:
                rest = k[len(Prefix):]
                cut = rest.find(Delimiter)
                if cut == -1:
                    contents_keys.append(k)
                else:
                    common.add(Prefix + rest[: cut + 1])
        else:
            contents_keys = keys
        # Paginate contents by page_size using an integer offset token.
        start = int(ContinuationToken) if ContinuationToken else 0
        page = contents_keys[start:start + self._page_size]
        next_start = start + self._page_size
        truncated = next_start < len(contents_keys)
        resp = {
            "Contents": [{"Key": k, "Size": len(self._objs[k]), "LastModified": self._mtime} for k in page],
            "IsTruncated": truncated,
        }
        if truncated:
            resp["NextContinuationToken"] = str(next_start)
        if Delimiter and not ContinuationToken:
            resp["CommonPrefixes"] = [{"Prefix": p} for p in sorted(common)]
        return resp


def _store(objects, *, bucket="up", page_size=1000):
    return S3Store(bucket=bucket, client=FakeS3Client(objects, page_size=page_size))


# ---------------------------------------------------------------------------
# Auth parsing / validation
# ---------------------------------------------------------------------------

def test_parse_infers_static_and_session():
    assert s3_auth.parse_s3_auth({"access_key": "AK", "secret_key": "S"}).mode == "static"
    assert s3_auth.parse_s3_auth({"access_key": "AK", "secret_key": "S",
                                  "session_token": "T"}).mode == "session"


def test_validate_flags_missing_fields():
    assert s3_auth.validate_s3_auth(s3_auth.parse_s3_auth({"mode": "static"}))  # missing keys
    assert not s3_auth.validate_s3_auth(
        s3_auth.parse_s3_auth({"mode": "static", "access_key": "AK", "secret_key": "S"}))
    assert s3_auth.validate_s3_auth(s3_auth.parse_s3_auth({"mode": "assume_role"}))  # needs role_arn
    assert not s3_auth.validate_s3_auth(
        s3_auth.parse_s3_auth({"mode": "assume_role", "role_arn": "arn:aws:iam::1:role/r"}))
    assert not s3_auth.validate_s3_auth(s3_auth.parse_s3_auth({"mode": "anonymous"}))
    assert not s3_auth.validate_s3_auth(s3_auth.parse_s3_auth({"mode": "instance"}))
    assert s3_auth.validate_s3_auth(s3_auth.parse_s3_auth({"mode": "bogus"}))


def test_all_coverage_modes_recognized():
    for mode in ("static", "session", "assume_role", "web_identity",
                 "profile", "sso", "instance", "process", "anonymous"):
        assert mode in s3_auth.SUPPORTED_MODES


def test_options_defaults_path_style_for_custom_endpoint():
    m = Mount("b", "s3", root="up", endpoint="https://minio.local")
    assert s3_auth.options_from_mount(m).addressing_style == "path"
    m2 = Mount("b", "s3", root="up", verify_tls="false")
    assert s3_auth.options_from_mount(m2).verify is False


def test_resolve_requires_credential_or_auth():
    with pytest.raises(KeyError):
        s3_auth.resolve_s3_auth(Mount("b", "s3", root="up"))
    assert s3_auth.resolve_s3_auth(Mount("b", "s3", root="up", auth="anonymous")).mode == "anonymous"


def test_resolve_reads_credential_store():
    class _Store:
        def get_secret(self, sid):
            return {"mode": "static", "access_key": "AK", "secret_key": "S"} if sid == "cred1" else None

    auth = s3_auth.resolve_s3_auth(Mount("b", "s3", root="up", credential="cred1"), store=_Store())
    assert auth.mode == "static" and auth.access_key == "AK"


# ---------------------------------------------------------------------------
# S3Store
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
    assert b"".join(store.get_stream("a/b.bin", chunk_size=1024)) == payload
    assert b"".join(store.get_stream("a/b.bin", offset=100, length=50, chunk_size=7)) == payload[100:150]


def test_get_stream_missing_raises():
    store = _store({})
    with pytest.raises(ObjectNotFound):
        list(store.get_stream("nope.bin"))


def test_list_dir_one_level():
    store = _store({"top.txt": b"t", "sub/data.bin": b"x", "sub/deep/y.txt": b"y"})
    entries = {name: is_dir for name, is_dir, *_ in store.list_dir("")}
    assert entries == {"sub": True, "top.txt": False}
    lvl = {name for name, *_ in store.list_dir("sub/")}
    assert lvl == {"data.bin", "deep"}


def test_list_follows_pagination():
    objs = {f"data/f{i:04d}.bin": b"x" for i in range(2500)}
    store = _store(objs, page_size=1000)
    got = store.list("data/")
    assert len(got) == 2500          # all three pages walked, nothing dropped


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
# End-to-end through s3.router with an s3 mount
# ---------------------------------------------------------------------------

@pytest.fixture
def s3_proxy_app(monkeypatch):
    objs = {"readme.txt": b"hello world", "sub/data.bin": bytes(range(256)) * 8}
    fake = FakeS3Client(objs)

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS",
                        {"s3vault": Mount("s3vault", "s3", root="up", auth="anonymous")})
    # Inject the fake client instead of building a real boto3 client.
    monkeypatch.setattr(mounts, "_backends", {"s3vault": S3Store(bucket="up", client=fake)})

    app = FastAPI()
    app.include_router(s3_router)
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_s3_passthrough_list_and_get(s3_proxy_app):
    async with _client(s3_proxy_app) as c:
        r = await c.get("/s3vault?list-type=2&delimiter=/")
        assert r.status_code == 200
        assert "readme.txt" in r.text and "<CommonPrefixes>" in r.text and "sub/" in r.text

        g = await c.get("/s3vault/readme.txt")
        assert g.status_code == 200 and g.content == b"hello world"

        gr = await c.get("/s3vault/readme.txt", headers={"Range": "bytes=0-4"})
        assert gr.status_code == 206 and gr.content == b"hello"


async def test_s3_passthrough_missing_key_404(s3_proxy_app):
    async with _client(s3_proxy_app) as c:
        g = await c.get("/s3vault/nope.txt")
        assert g.status_code == 404 and "NoSuchKey" in g.text


async def test_s3_mount_advertised_in_listbuckets(s3_proxy_app):
    async with _client(s3_proxy_app) as c:
        r = await c.get("/")
        assert r.status_code == 200 and "s3vault" in r.text


# ---------------------------------------------------------------------------
# S3 credential endpoints (Storage tab wiring) + round-trip resolve
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


async def test_s3_credential_save_list_resolve_delete(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/s3-credentials", json={
            "credential_id": "s3vault",
            "auth": {"mode": "static", "access_key": "AKIA...", "secret_key": "SECRET"}})
        d = r.json()
        assert d["ok"] is True and d["mode"] == "static"

        lst = (await c.get("/_config/api/s3-credentials")).json()
        assert lst["ok"] is True and "s3vault" in lst["ids"]

        # The saved secret resolves back for a mount referencing it.
        store = CredentialStore(str(tmp_path / "credentials.json"))
        auth = s3_auth.resolve_s3_auth(Mount("s3vault", "s3", root="up", credential="s3vault"), store=store)
        assert auth.mode == "static" and auth.access_key == "AKIA..."

        del_resp = (await c.request("DELETE", "/_config/api/s3-credentials/s3vault")).json()
        assert del_resp["ok"] is True and del_resp["removed"] is True


async def test_s3_credential_rejects_invalid(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/s3-credentials", json={
            "credential_id": "bad", "auth": {"mode": "static"}})   # missing keys
        assert r.status_code == 400 and r.json()["ok"] is False


async def test_s3_mount_save_persists_connection_knobs(cb_app, tmp_path, monkeypatch):
    monkeypatch.setenv("MOUNTS_CONFIG_FILE", str(tmp_path / "config.mounts.json"))
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/mounts", json={"mounts": [{
            "bucket": "s3vault", "backend": "s3", "root": "reports-bucket",
            "endpoint": "https://minio.local:9000", "region": "us-east-1",
            "addressing_style": "path", "credential": "s3vault", "prefix": "2026"}]})
        d = r.json()
        assert d["ok"] is True and d["count"] == 1
        written = (tmp_path / "config.mounts.json").read_text(encoding="utf-8")
        assert "minio.local" in written and "reports-bucket" in written and "path" in written

        # s3 mount missing both credential and auth is rejected.
        r2 = await c.post("/_config/api/mounts", json={"mounts": [{
            "bucket": "noauth", "backend": "s3", "root": "b"}]})
        assert r2.status_code == 400 and "credential" in " ".join(r2.json()["errors"]).lower()
