"""Standalone HTTP Basic auth gate over the Manager's operator surface."""
from __future__ import annotations

import base64
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import httpx
import pytest
from fastapi import FastAPI

import config
from enterprise.control.auth import ManagerAuthMiddleware, manager_auth_active


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(ManagerAuthMiddleware)

    @app.get("/_manager/api/fleet")
    async def fleet():
        return {"ok": True}

    @app.get("/agents")
    async def agents():
        return {"agents": []}

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.post("/control/register")
    async def register():
        return {"lease_id": "x"}

    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


def _basic(user: str, pw: str) -> dict:
    token = base64.b64encode(f"{user}:{pw}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


@pytest.fixture
def _enable_auth(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_USERNAME", "operator", raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "s3cret", raising=False)


async def test_disabled_auth_fails_closed(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", False, raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "s3cret", raising=False)
    assert manager_auth_active() is False
    async with _client(_app()) as c:
        assert (await c.get("/_manager/api/fleet")).status_code == 503


async def test_missing_password_fails_closed(monkeypatch):
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", "", raising=False)
    assert manager_auth_active() is True
    async with _client(_app()) as c:
        assert (await c.get("/_manager/api/fleet")).status_code == 503


async def test_protected_requires_credentials(_enable_auth):
    async with _client(_app()) as c:
        r = await c.get("/_manager/api/fleet")
        assert r.status_code == 401
        assert r.headers.get("www-authenticate", "").lower().startswith("basic")

        assert (await c.get("/agents")).status_code == 401


async def test_wrong_and_malformed_credentials_rejected(_enable_auth):
    async with _client(_app()) as c:
        assert (await c.get("/_manager/api/fleet", headers=_basic("operator", "nope"))).status_code == 401
        assert (await c.get("/_manager/api/fleet", headers=_basic("who", "s3cret"))).status_code == 401
        assert (await c.get("/_manager/api/fleet", headers={"Authorization": "Basic not-base64!!"})).status_code == 401
        assert (await c.get("/_manager/api/fleet", headers={"Authorization": "Bearer s3cret"})).status_code == 401


async def test_correct_credentials_pass(_enable_auth):
    async with _client(_app()) as c:
        r = await c.get("/_manager/api/fleet", headers=_basic("operator", "s3cret"))
        assert r.status_code == 200 and r.json() == {"ok": True}


async def test_valid_local_session_also_passes_manager_basic_gate(_enable_auth, tmp_path, monkeypatch):
    from security.authorization import User
    from security.identity import IdentityProvider, identity_provider

    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    provider = IdentityProvider(str(identity_path))
    provider.create_or_replace(User("ops", roles=("monitor_troubleshooter",)), "correct horse battery staple")
    session_provider = identity_provider()
    user = session_provider.authenticate("ops", "correct horse battery staple")
    session = session_provider.create_session(user)
    async with _client(_app()) as c:
        response = await c.get("/_manager/api/fleet", cookies={"fsp_session": session})
    assert response.status_code == 200


async def test_health_exempt_control_protected(_enable_auth):
    async with _client(_app()) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.post("/control/register")).status_code == 401
        assert (await c.post("/control/register",
                             headers=_basic("operator", "s3cret"))).status_code == 200
