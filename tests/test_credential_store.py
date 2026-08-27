"""
Credential store tests: encryption-agnostic behaviour via an injected cipher,
env-var naming, startup hydration, and the config-builder credential endpoints.

Real DPAPI (Windows) / Fernet round-trips are exercised opportunistically when
those backends are available on the host.
"""
from __future__ import annotations

import os

import pytest
import httpx
from fastapi import FastAPI

import config
from configbuilder.router import router as cb_router
import security.credential_store as cs
from security.credential_store import CredentialStore, env_var_for, hydrate_environment


class _FakeCipher:
    """Reversible non-plaintext transform for cross-platform tests (never used in prod)."""
    name = "fake"

    def encrypt(self, plaintext: bytes) -> bytes:
        return bytes(b ^ 0x5A for b in plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return bytes(b ^ 0x5A for b in ciphertext)


def _fake_store(tmp_path) -> CredentialStore:
    return CredentialStore(str(tmp_path / "credentials.json"), cipher=_FakeCipher())


# ---------------------------------------------------------------------------
# env-var naming
# ---------------------------------------------------------------------------

def test_env_var_for_default_and_named():
    assert env_var_for("default") == "DB_URL"
    assert env_var_for("") == "DB_URL"
    assert env_var_for(None) == "DB_URL"
    assert env_var_for("warehouse_pg") == "DB_URL_WAREHOUSE_PG"
    assert env_var_for("SO Data") == "DB_URL_SO_DATA"
    assert env_var_for("a-b.c") == "DB_URL_A_B_C"


# ---------------------------------------------------------------------------
# round-trip via injected cipher
# ---------------------------------------------------------------------------

def test_set_get_roundtrip_and_encrypted_at_rest(tmp_path):
    st = _fake_store(tmp_path)
    url = "mssql+aioodbc://user:s3cr3t@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server"
    st.set_url("default", url)
    assert st.get_url("default") == url
    # The on-disk file must NOT contain the plaintext password.
    raw = (tmp_path / "credentials.json").read_text(encoding="utf-8")
    assert "s3cr3t" not in raw
    assert "enc" in raw


def test_list_delete_and_status_hide_secrets(tmp_path):
    st = _fake_store(tmp_path)
    st.set_url("default", "postgresql+asyncpg://u:pw@h/db")
    st.set_url("warehouse", "postgresql+asyncpg://u:pw2@h2/db2")
    assert st.list_ids() == ["default", "warehouse"]

    status = st.status()
    assert status["available"] is True
    assert status["backend"] == "fake"
    ids = {c["id"]: c for c in status["connections"]}
    assert ids["default"]["env_var"] == "DB_URL"
    assert ids["warehouse"]["env_var"] == "DB_URL_WAREHOUSE"
    # status never leaks a URL / password
    import json as _json
    assert "pw" not in _json.dumps(status)

    assert st.delete("warehouse") is True
    assert st.delete("warehouse") is False
    assert st.list_ids() == ["default"]


def test_env_overrides_maps_connection_ids(tmp_path):
    st = _fake_store(tmp_path)
    st.set_url("default", "postgresql+asyncpg://u:pw@h/db")
    st.set_url("SO Data", "mssql+aioodbc://u:pw@h/db")
    ov = st.env_overrides()
    assert ov["DB_URL"] == "postgresql+asyncpg://u:pw@h/db"
    assert ov["DB_URL_SO_DATA"] == "mssql+aioodbc://u:pw@h/db"


def test_set_url_rejects_empty(tmp_path):
    st = _fake_store(tmp_path)
    with pytest.raises(ValueError):
        st.set_url("default", "")


def test_set_url_rejects_masked(tmp_path):
    st = _fake_store(tmp_path)
    with pytest.raises(ValueError):
        st.set_url("default", "mssql+aioodbc://SQL2IB:***@host:1433/db")


def test_looks_masked_helper():
    assert cs.looks_masked("mssql+aioodbc://u:***@h/db") is True
    assert cs.looks_masked("mssql+aioodbc://u:realpw@h/db") is False
    assert cs.looks_masked("") is False


def test_env_overrides_skip_masked_entries(tmp_path):
    # Simulate a store polluted by an older buggy save (masked password on disk).
    st = _fake_store(tmp_path)
    st.set_url("default", "postgresql+asyncpg://u:realpw@h/db")
    # Inject a masked entry directly, bypassing the set_url guard.
    data = st._load()
    import base64 as _b64
    data["connections"]["warehouse"] = {
        "enc": _b64.b64encode(_FakeCipher().encrypt(
            b"mssql+aioodbc://u:***@h/db")).decode("ascii"),
        "updated_at": "x",
    }
    st._save(data)
    ov = st.env_overrides()
    assert ov.get("DB_URL") == "postgresql+asyncpg://u:realpw@h/db"
    assert "DB_URL_WAREHOUSE" not in ov          # masked value is never hydrated


def test_unavailable_store_refuses_to_store_plaintext(tmp_path, monkeypatch):
    monkeypatch.setattr(cs, "_select_cipher", lambda _p: None)
    st = CredentialStore(str(tmp_path / "credentials.json"))
    assert st.available is False
    assert st.backend_name == "unavailable"
    assert st.get_url("default") is None
    assert st.env_overrides() == {}
    with pytest.raises(RuntimeError):
        st.set_url("default", "postgresql+asyncpg://u:pw@h/db")


def test_backend_mismatch_is_not_readable(tmp_path):
    path = str(tmp_path / "credentials.json")
    CredentialStore(path, cipher=_FakeCipher()).set_url("default", "postgresql+asyncpg://u:pw@h/db")

    class _OtherCipher(_FakeCipher):
        name = "other"

    other = CredentialStore(path, cipher=_OtherCipher())
    assert other.get_url("default") is None          # cannot decrypt across backends
    assert other.env_overrides() == {}


# ---------------------------------------------------------------------------
# startup hydration
# ---------------------------------------------------------------------------

def test_hydrate_fills_missing_but_never_overrides(tmp_path, monkeypatch):
    st = _fake_store(tmp_path)
    st.set_url("default", "postgresql+asyncpg://u:pw@h/db")
    st.set_url("warehouse", "postgresql+asyncpg://u:pw2@h2/db2")

    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("DB_URL_WAREHOUSE", "already-set-by-operator")
    monkeypatch.setenv("ENABLE_CREDENTIAL_STORE", "1")

    hydrated = hydrate_environment(st)
    assert "DB_URL" in hydrated
    assert "DB_URL_WAREHOUSE" not in hydrated          # operator's env wins
    assert os.environ["DB_URL"] == "postgresql+asyncpg://u:pw@h/db"
    assert os.environ["DB_URL_WAREHOUSE"] == "already-set-by-operator"


def test_hydrate_respects_disable_flag(tmp_path, monkeypatch):
    st = _fake_store(tmp_path)
    st.set_url("default", "postgresql+asyncpg://u:pw@h/db")
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.setenv("ENABLE_CREDENTIAL_STORE", "0")
    assert hydrate_environment(st) == []
    assert "DB_URL" not in os.environ


# ---------------------------------------------------------------------------
# native backend round-trip (opportunistic)
# ---------------------------------------------------------------------------

def test_native_backend_roundtrip_if_available(tmp_path):
    st = CredentialStore(str(tmp_path / "credentials.json"))
    if not st.available:
        pytest.skip("no native encryption backend on this host")
    url = "mssql+aioodbc://user:p%40ss@host:1433/db"
    st.set_url("default", url)
    assert st.get_url("default") == url
    raw = (tmp_path / "credentials.json").read_text(encoding="utf-8")
    assert "p%40ss" not in raw


# ---------------------------------------------------------------------------
# config-builder endpoints
# ---------------------------------------------------------------------------

@pytest.fixture
def cred_app(tmp_path, monkeypatch):
    # Force the router's store to use the deterministic fake cipher.
    monkeypatch.setattr("configbuilder.router._store",
                        lambda: CredentialStore(str(tmp_path / "creds.json"), cipher=_FakeCipher()))
    monkeypatch.setattr(config, "ENABLE_CREDENTIAL_STORE", True, raising=False)
    a = FastAPI()
    a.include_router(cb_router)
    return a


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_endpoint_save_list_delete(cred_app):
    async with _client(cred_app) as c:
        r = await c.post("/_config/api/credentials", json={
            "connection_id": "default",
            "db_url": "mssql+aioodbc://user:s3cr3t@host:1433/db",
        })
        d = r.json()
        assert r.status_code == 200 and d["ok"] is True
        assert d["env_var"] == "DB_URL"
        assert d["applied"] is False and d["restarted"] == 0

        r = await c.get("/_config/api/credentials")
        d = r.json()
        assert d["ok"] is True
        ids = [x["id"] for x in d["connections"]]
        assert "default" in ids
        assert "s3cr3t" not in r.text                 # never returns the secret

        r = await c.request("DELETE", "/_config/api/credentials/default")
        assert r.json()["removed"] is True


async def test_endpoint_builds_url_from_fields(cred_app):
    async with _client(cred_app) as c:
        r = await c.post("/_config/api/credentials", json={
            "connection_id": "warehouse_pg",
            "connection": {"dialect": "postgresql", "host": "h", "database": "db",
                           "username": "u", "password": "pw"},
        })
        d = r.json()
        assert d["ok"] is True
        assert d["env_var"] == "DB_URL_WAREHOUSE_PG"


async def test_endpoint_apply_reports_manager_restart_required(cred_app, monkeypatch):
    async def restart_agents(_request):
        return 2

    monkeypatch.setattr("configbuilder.router._restart_agents", restart_agents)
    async with _client(cred_app) as client:
        response = await client.post("/_config/api/credentials", json={
            "connection_id": "default",
            "db_url": "mssql+aioodbc://user:secret@host:1433/db",
            "apply": True,
        })

    body = response.json()
    assert response.status_code == 200
    assert body["restarted"] == 2
    assert body["manager_restart_required"] is True
    assert "Open Mirroring" in body["note"]


async def test_endpoint_rejects_empty(cred_app):
    async with _client(cred_app) as c:
        r = await c.post("/_config/api/credentials", json={"connection_id": "default"})
        assert r.status_code == 400
        assert r.json()["ok"] is False


async def test_endpoint_rejects_masked_url(cred_app):
    async with _client(cred_app) as c:
        r = await c.post("/_config/api/credentials", json={
            "connection_id": "default",
            "db_url": "mssql+aioodbc://SQL2IB:***@host:1433/db",
        })
        assert r.status_code == 400
        assert "***" in r.json()["error"]
