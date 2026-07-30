"""
Phase 4 tests — proxy access keys, per-key authorization, the SigV4 secret
resolver, audit events, and end-to-end middleware enforcement on a mounted bucket.
"""
from __future__ import annotations

import os
import pathlib

_DB = pathlib.Path(__file__).parent / "test_access_control.db"
os.environ.setdefault("DB_URL", f"sqlite+aiosqlite:///{_DB.as_posix()}")

import httpx
import pytest

import config
from s3.auth import verify_signature, SigV4Error
from security import access_keys as ak

botocore = pytest.importorskip("botocore")
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

_HOST = "s3.local"
_REGION = "us-east-1"


def _sign(method, path, query="", *, key, secret):
    url = f"http://{_HOST}{path}"
    params = None
    if query:
        from urllib.parse import parse_qsl
        params = dict(parse_qsl(query, keep_blank_values=True))
    req = AWSRequest(method=method, url=url, headers={"host": _HOST}, params=params)
    S3SigV4Auth(Credentials(key, secret), "s3", _REGION).add_auth(req)
    return dict(req.headers)


class FakeStore:
    """Minimal store exposing just what security.access_keys reads."""

    def __init__(self, records: dict[str, dict]):
        self._r = records

    def list_access_key_ids(self):
        return list(self._r)

    def get_access_key(self, kid):
        return self._r.get(kid)


def _rec(key_id, secret="s", buckets=("*",), prefixes=None, perms="read", enabled=True):
    return {
        "access_key_id": key_id, "secret_key": secret, "label": "",
        "allowed_buckets": list(buckets), "allowed_prefixes": prefixes or {},
        "permissions": perms, "enabled": enabled,
    }


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------

def test_parse_validate_generate():
    good = ak.parse_access_key(_rec("FSPKEY0000000001", buckets=["b1"]))
    assert not ak.validate_access_key(good)
    assert ak.validate_access_key(ak.parse_access_key({"access_key_id": "x", "secret_key": ""}))
    kid, secret = ak.generate_key()
    assert kid.startswith("FSP") and len(kid) >= 8 and len(secret) >= 20
    assert "secret_key" not in ak.to_public(good)


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------

def test_authorize_wildcard_and_bucket_scope():
    st = FakeStore({"W": _rec("W", buckets=["*"]), "S": _rec("S", buckets=["s3vault"])})
    assert ak.authorize("W", "anything", "k", "GET", store=st) is None
    assert ak.authorize("S", "s3vault", "k", "GET", store=st) is None
    assert ak.authorize("S", "other", "k", "GET", store=st) is not None


def test_authorize_prefix_scope():
    st = FakeStore({"P": _rec("P", buckets=["s3vault"], prefixes={"s3vault": ["2026"]})})
    assert ak.authorize("P", "s3vault", "2026/report.parquet", "GET", store=st) is None
    assert ak.authorize("P", "s3vault", "2025/report.parquet", "GET", store=st) is not None


def test_authorize_write_denied_and_disabled():
    st = FakeStore({"W": _rec("W", buckets=["*"]), "D": _rec("D", buckets=["*"], enabled=False)})
    assert ak.authorize("W", "b", "k", "PUT", store=st) is not None      # read-only
    assert ak.authorize("D", "b", "k", "GET", store=st) is not None      # disabled


def test_authorize_legacy_identity_is_wildcard():
    st = FakeStore({})
    assert ak.authorize("legacy-key", "any", "k", "GET", store=st) is None


def test_resolve_secret_store_then_legacy(monkeypatch):
    st = FakeStore({"K": _rec("K", secret="topsecret", enabled=True)})
    assert ak.resolve_secret("K", store=st) == "topsecret"
    monkeypatch.setattr(config, "ACCESS_KEY_ID", "LEGACY", raising=False)
    monkeypatch.setattr(config, "SECRET_ACCESS_KEY", "legsecret", raising=False)
    assert ak.resolve_secret("LEGACY", store=st) == "legsecret"
    assert ak.resolve_secret("unknown", store=st) is None


# ---------------------------------------------------------------------------
# verify_signature resolver path
# ---------------------------------------------------------------------------

def test_verify_signature_with_resolver():
    path = "/secure-nfs/readme.txt"
    headers = _sign("GET", path, key="FSPKEY0000000001", secret="sekret")
    identity = verify_signature("GET", path, "", headers,
                                secret_resolver=lambda k: "sekret" if k == "FSPKEY0000000001" else None)
    assert identity == "FSPKEY0000000001"


def test_verify_signature_resolver_unknown_key():
    path = "/secure-nfs/readme.txt"
    headers = _sign("GET", path, key="FSPKEY0000000001", secret="sekret")
    with pytest.raises(SigV4Error) as exc:
        verify_signature("GET", path, "", headers, secret_resolver=lambda k: None)
    assert exc.value.code == "InvalidAccessKeyId"


# ---------------------------------------------------------------------------
# Audit ring
# ---------------------------------------------------------------------------

def test_audit_records_and_scrubs(monkeypatch):
    from observability import audit
    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True, raising=False)
    audit.record(identity="FSPKEY", bucket="secure-nfs", key="a/b.txt", backend="local",
                 method="GET", status=200, bytes_=123)
    ev = audit.recent(1)[-1]
    assert ev["identity"] == "FSPKEY" and ev["bucket"] == "secure-nfs" and ev["bytes"] == 123


# ---------------------------------------------------------------------------
# Access-key endpoints + credential-store round-trip
# ---------------------------------------------------------------------------

@pytest.fixture
def cb_app(tmp_path, monkeypatch):
    from fastapi import FastAPI
    from configbuilder.router import router as cb_router
    monkeypatch.setattr(config, "ENABLE_CREDENTIAL_STORE", True, raising=False)
    monkeypatch.setattr(config, "CREDENTIAL_STORE_PATH", str(tmp_path / "credentials.json"), raising=False)
    ak.invalidate_cache()
    a = FastAPI()
    a.include_router(cb_router)
    return a


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_access_key_create_list_rotate_delete(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/access-keys", json={
            "label": "finance", "allowed_buckets": ["secure-nfs"], "permissions": "read"})
        d = r.json()
        assert d["ok"] is True and d["generated"] is True and d["secret_key"]
        kid = d["access_key_id"]

        lst = (await c.get("/_config/api/access-keys")).json()
        assert lst["ok"] is True and any(k["access_key_id"] == kid for k in lst["keys"])

        rot = (await c.post(f"/_config/api/access-keys/{kid}/rotate", json={})).json()
        assert rot["ok"] is True and rot["secret_key"]

        dele = (await c.request("DELETE", f"/_config/api/access-keys/{kid}")).json()
        assert dele["ok"] is True and dele["removed"] is True


async def test_access_key_rejects_invalid(cb_app, tmp_path):
    from security.credential_store import CredentialStore
    if not CredentialStore(str(tmp_path / "credentials.json")).available:
        pytest.skip("no encryption backend available on this host")
    async with _client(cb_app) as c:
        r = await c.post("/_config/api/access-keys", json={"allowed_buckets": []})  # no buckets
        assert r.status_code == 400 and r.json()["ok"] is False


# ---------------------------------------------------------------------------
# End-to-end middleware enforcement on a mounted bucket
# ---------------------------------------------------------------------------

@pytest.fixture
def mount_app(tmp_path, monkeypatch):
    (tmp_path / "readme.txt").write_bytes(b"hello world")
    import storage.mounts as mounts
    from storage.mounts import Mount
    from runtime.artifact_store import LocalDirStore

    monkeypatch.setenv("ENABLE_STORAGE_PROXY", "1")
    monkeypatch.setattr(mounts, "MOUNTS", {"secure-nfs": Mount("secure-nfs", "local", str(tmp_path))})
    monkeypatch.setattr(mounts, "_backends", {"secure-nfs": LocalDirStore(str(tmp_path))})
    monkeypatch.setattr(config, "ENFORCE_MOUNT_AUTH", True, raising=False)
    monkeypatch.setattr(config, "REQUIRE_SIGV4", False, raising=False)
    monkeypatch.setattr(config, "ACCESS_KEY_ID", "AKIATESTLEGACY001", raising=False)
    monkeypatch.setattr(config, "SECRET_ACCESS_KEY", "legacy-secret", raising=False)
    monkeypatch.setattr(config, "CREDENTIAL_STORE_PATH", str(tmp_path / "credentials.json"), raising=False)
    monkeypatch.setattr(config, "ENABLE_CREDENTIAL_STORE", True, raising=False)
    ak.invalidate_cache()

    from main import app
    return app


def _host_client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=f"http://{_HOST}")


async def test_mount_forces_auth_and_legacy_key_allowed(mount_app, monkeypatch):
    async with _host_client(mount_app) as c:
        # Unsigned request to a mounted bucket is rejected even with REQUIRE_SIGV4 off.
        r = await c.get("/secure-nfs/readme.txt")
        assert r.status_code == 403 and "AccessDenied" in r.text

        # Signed with the legacy key (implicit wildcard) succeeds.
        headers = _sign("GET", "/secure-nfs/readme.txt",
                        key=config.ACCESS_KEY_ID, secret=config.SECRET_ACCESS_KEY)
        ok = await c.get("/secure-nfs/readme.txt", headers=headers)
        assert ok.status_code == 200 and ok.content == b"hello world"


async def test_mount_denies_out_of_scope_acl_key(mount_app, tmp_path):
    from security.credential_store import CredentialStore
    st = CredentialStore(str(tmp_path / "credentials.json"))
    if not st.available:
        pytest.skip("no encryption backend available on this host")
    ak.save_access_key(ak.parse_access_key(_rec("FSPSCOPED00000001", secret="aclsecret",
                                                 buckets=["other-bucket"])), store=st)
    async with _host_client(mount_app) as c:
        headers = _sign("GET", "/secure-nfs/readme.txt", key="FSPSCOPED00000001", secret="aclsecret")
        r = await c.get("/secure-nfs/readme.txt", headers=headers)
        assert r.status_code == 403 and "AccessDenied" in r.text
