"""Phase 3 UX (issue #16) — Config Builder Key Vault settings + status/test API.

The Key Vault settings are categorized in the catalog, and the /_config panel is
driven by GET /api/keyvault (status) + POST /api/keyvault/test (live probe). The
Azure SDK is not installed in the test env, so 'test' returns the install hint.
"""
from __future__ import annotations

import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import config
from configbuilder.router import router as cb_router


@pytest.fixture(scope="module")
def app():
    a = FastAPI()
    a.include_router(cb_router)
    return a


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def test_settings_catalog_categorizes_keyvault():
    m = {s["key"]: s for s in config.settings_catalog()}
    for key in ("keyvault_uri", "auth_mode", "require_keyvault", "keyvault_refresh_seconds",
                "keyvault_cache_ttl", "azure_tenant_id", "azure_client_id"):
        assert m[key]["category"] == "Entra ID & Key Vault"
        assert m[key]["help"]
    assert m["auth_mode"]["choices"] == ["default", "managed_identity", "service_principal"]


async def test_keyvault_status_disabled_by_default(app, monkeypatch):
    import security.keyvault as kv
    monkeypatch.setattr(kv, "sdk_available", lambda: True)
    async with _client(app) as c:
        r = await c.get("/_config/api/keyvault")
    assert r.status_code == 200
    d = r.json()
    assert d["enabled"] is False
    assert d["status"] == "disabled"
    assert d["sdk_installed"] is False


async def test_keyvault_test_no_uri_returns_clear_error(app):
    async with _client(app) as c:
        r = await c.post("/_config/api/keyvault/test", json={})
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is False
    assert "no Key Vault URI" in d["error"]


async def test_keyvault_test_without_sdk_reports_install_hint(app, monkeypatch):
    import security.keyvault as kv
    monkeypatch.setattr(kv, "sdk_available", lambda: False)
    async with _client(app) as c:
        r = await c.post(
            "/_config/api/keyvault/test",
            json={"vault_uri": "https://myvault.vault.azure.net", "auth_mode": "managed_identity"},
        )
    assert r.status_code == 200
    d = r.json()
    assert d["ok"] is False
    assert "azure-keyvault-secrets" in d["detail"]
    assert d["vault"] == "myvault.vault.azure.net"
