"""
Storage proxy (Phase 1) tests — mount registry, streaming get_stream, and the
passthrough S3 handlers wired into s3.router, plus coexistence with the DB path.
"""
from __future__ import annotations

import os
import pathlib

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import config
from runtime.artifact_store import LocalDirStore, ObjectNotFound
import storage.mounts as mounts
from storage.mounts import Mount
from s3.router import router as s3_router


# ---------------------------------------------------------------------------
# get_stream (streaming reads)
# ---------------------------------------------------------------------------

def test_local_get_stream_full_and_range(tmp_path):
    store = LocalDirStore(str(tmp_path))
    payload = bytes(range(256)) * 40          # 10 240 bytes
    store.put("a/b.bin", payload)

    got = b"".join(store.get_stream("a/b.bin", chunk_size=1024))
    assert got == payload

    # Ranged: offset 100, length 50.
    part = b"".join(store.get_stream("a/b.bin", offset=100, length=50, chunk_size=7))
    assert part == payload[100:150]


def test_local_get_stream_missing_raises(tmp_path):
    store = LocalDirStore(str(tmp_path))
    with pytest.raises(ObjectNotFound):
        list(store.get_stream("nope.bin"))


def test_head_reports_size_and_mtime(tmp_path):
    store = LocalDirStore(str(tmp_path))
    store.put("f.txt", b"hello")
    st = store.head("f.txt")
    assert st is not None and st.size == 5 and st.mtime_ms and st.mtime_ms > 0


# ---------------------------------------------------------------------------
# mount registry
# ---------------------------------------------------------------------------

def test_norm_prefix_and_mount_from_json():
    assert mounts._norm_prefix("") == ""
    assert mounts._norm_prefix("/reports/") == "reports/"
    m = mounts._mount_from_json({"bucket": "b", "backend": "LOCAL", "root": "/x",
                                 "prefix": "sub", "read_only": True})
    assert m.bucket == "b" and m.backend == "local" and m.prefix == "sub/"


def test_get_mount_gated_by_enabled(monkeypatch):
    monkeypatch.setattr(mounts, "MOUNTS", {"m": Mount("m", "local", "/x")})
    monkeypatch.delenv("ENABLE_STORAGE_PROXY", raising=False)
    monkeypatch.setattr(mounts.system_config, "ENABLE_STORAGE_PROXY", False, raising=False)
    assert mounts.get_mount("m") is None            # disabled => inert
    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    assert mounts.get_mount("m") is not None


def test_validate_mounts_flags_bad_root(monkeypatch, tmp_path):
    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS", {
        "good": Mount("good", "local", str(tmp_path)),
        "bad": Mount("bad", "local", str(tmp_path / "missing")),
    })
    problems = mounts.validate_mounts()
    assert any("bad" in p for p in problems)
    assert not any("good" in p for p in problems)


# ---------------------------------------------------------------------------
# passthrough via the real s3.router (end-to-end) + coexistence
# ---------------------------------------------------------------------------

@pytest.fixture
def proxy_app(tmp_path, monkeypatch):
    # A temp "share" with a couple of files + a subfolder.
    (tmp_path / "sub").mkdir()
    (tmp_path / "readme.txt").write_bytes(b"hello world")
    (tmp_path / "sub" / "data.bin").write_bytes(bytes(range(256)) * 8)   # 2048 bytes

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS",
                        {"secure-nfs": Mount("secure-nfs", "local", str(tmp_path))})
    monkeypatch.setattr(mounts, "_backends", {})     # fresh backend cache

    app = FastAPI()
    app.include_router(s3_router)
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_passthrough_list_flat_and_delimited(proxy_app):
    async with _client(proxy_app) as c:
        r = await c.get("/secure-nfs?list-type=2")
        assert r.status_code == 200 and "application/xml" in r.headers["content-type"]
        assert "readme.txt" in r.text and "sub/data.bin" in r.text

        r = await c.get("/secure-nfs?list-type=2&delimiter=/")
        assert "readme.txt" in r.text
        assert "<CommonPrefixes>" in r.text and "sub/" in r.text
        assert "sub/data.bin" not in r.text          # rolled up under the prefix


async def test_passthrough_head_and_get_full(proxy_app):
    async with _client(proxy_app) as c:
        h = await c.head("/secure-nfs/readme.txt")
        assert h.status_code == 200 and h.headers["Content-Length"] == "11"
        assert "Last-Modified" in h.headers

        g = await c.get("/secure-nfs/readme.txt")
        assert g.status_code == 200 and g.content == b"hello world"
        assert g.headers["Accept-Ranges"] == "bytes"


async def test_passthrough_ranged_get(proxy_app):
    async with _client(proxy_app) as c:
        g = await c.get("/secure-nfs/readme.txt", headers={"Range": "bytes=0-4"})
        assert g.status_code == 206
        assert g.content == b"hello"
        assert g.headers["Content-Range"] == "bytes 0-4/11"

        # Suffix range: last 5 bytes.
        g2 = await c.get("/secure-nfs/readme.txt", headers={"Range": "bytes=-5"})
        assert g2.status_code == 206 and g2.content == b"world"


async def test_passthrough_unsatisfiable_range_416(proxy_app):
    async with _client(proxy_app) as c:
        g = await c.get("/secure-nfs/readme.txt", headers={"Range": "bytes=100-200"})
        assert g.status_code == 416


async def test_passthrough_missing_key_404(proxy_app):
    async with _client(proxy_app) as c:
        g = await c.get("/secure-nfs/nope.txt")
        assert g.status_code == 404 and "NoSuchKey" in g.text


async def test_passthrough_confinement_rejects_traversal(proxy_app):
    # Call the handler directly so the client can't normalize the ".." away.
    from storage import passthrough

    class _Req:
        headers: dict = {}
        query_params: dict = {}

    resp = passthrough.get_object(mounts.MOUNTS["secure-nfs"], "../secrets.txt", _Req())
    assert resp.status_code == 403                 # backend refuses to escape root


async def test_coexistence_db_bucket_still_served(proxy_app):
    async with _client(proxy_app) as c:
        # The DB warehouse bucket is NOT a mount -> resolves via the Iceberg path
        # (empty snapshot set here => a valid empty listing, not a passthrough).
        r = await c.get(f"/{config.BUCKET_NAME}?list-type=2")
        assert r.status_code == 200
        # An unknown, unmounted bucket is a NoSuchBucket, proving mounts don't hijack.
        r2 = await c.get("/some-random-bucket?list-type=2")
        assert r2.status_code == 404 and "NoSuchBucket" in r2.text


# ---------------------------------------------------------------------------
# config-builder mount endpoints (Storage tab wiring)
# ---------------------------------------------------------------------------

@pytest.fixture
def cb_app():
    from fastapi import FastAPI as _FastAPI
    from configbuilder.router import router as cb_router
    a = _FastAPI()
    a.include_router(cb_router)
    return a


async def test_mounts_endpoint_get(cb_app):
    async with _client(cb_app) as c:
        r = await c.get("/_config/api/mounts")
        d = r.json()
        assert d["ok"] is True
        assert "local" in d["supported_backends"]
        assert d["reserved_bucket"] == config.BUCKET_NAME
        assert isinstance(d["mounts"], list)


async def test_mounts_endpoint_save_and_reject(cb_app, tmp_path, monkeypatch):
    monkeypatch.setenv("MOUNTS_CONFIG_FILE", str(tmp_path / "config.mounts.json"))
    async with _client(cb_app) as c:
        # Valid save (omit 'enabled' so config.system.json isn't touched).
        r = await c.post("/_config/api/mounts", json={"mounts": [
            {"bucket": "secure-nfs", "backend": "local", "root": str(tmp_path), "prefix": "reports"}]})
        d = r.json()
        assert d["ok"] is True and d["count"] == 1 and d["restart_required"] is True
        written = (tmp_path / "config.mounts.json").read_text(encoding="utf-8")
        assert "secure-nfs" in written and "reports" in written

        # Reserved bucket rejected.
        r2 = await c.post("/_config/api/mounts", json={"mounts": [
            {"bucket": config.BUCKET_NAME, "backend": "local", "root": str(tmp_path)}]})
        assert r2.status_code == 400 and "reserved" in " ".join(r2.json()["errors"]).lower()

        # Invalid bucket name rejected.
        r3 = await c.post("/_config/api/mounts", json={"mounts": [
            {"bucket": "Bad_Name", "backend": "local", "root": str(tmp_path)}]})
        assert r3.status_code == 400


async def test_mount_test_endpoint(cb_app, tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_bytes(b"x")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/mounts/test",
                         json={"backend": "local", "root": str(tmp_path)})
        d = r.json()
        assert d["ok"] is True and d["sample_count"] >= 2

        r2 = await c.post("/_config/api/mounts/test",
                          json={"backend": "local", "root": str(tmp_path / "missing")})
        assert r2.json()["ok"] is False
