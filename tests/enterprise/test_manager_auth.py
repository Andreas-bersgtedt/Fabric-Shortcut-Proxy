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

    @app.get("/_config")
    async def config_builder():
        return {"page": "config"}

    @app.get("/_config/api/authorization/status")
    async def authorization_status():
        return {"enforced": True}

    @app.post("/_config/api/authorization/login")
    async def authorization_login():
        return {"ok": True}

    @app.get("/_config/api/tables")
    async def config_tables():
        return {"tables": []}

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


def _standalone_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(ManagerAuthMiddleware, operator_only=True)

    @app.get("/_admin/stats")
    async def stats():
        return {"ok": True}

    @app.get("/data-bucket/object")
    async def data_object():
        return {"data": True}

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


async def test_operator_auth_denial_is_audited_without_credentials(_enable_auth, monkeypatch):
    from observability import audit

    monkeypatch.setattr(config, "ENABLE_AUDIT_LOG", True, raising=False)
    async with _client(_app()) as c:
        response = await c.get(
            "/_manager/api/fleet",
            headers={**_basic("operator", "wrong-secret"), "X-Request-ID": "operator-denied"},
        )
    assert response.status_code == 401
    event = next(item for item in audit.recent() if item.get("request_id") == "operator-denied")
    assert event["action"] == "operator_auth"
    assert event["outcome"] == "denied"
    assert event["path"] == "/_manager/api/fleet"
    assert "wrong-secret" not in str(event)


async def test_correct_credentials_pass(_enable_auth):
    async with _client(_app()) as c:
        r = await c.get("/_manager/api/fleet", headers=_basic("operator", "s3cret"))
        assert r.status_code == 200 and r.json() == {"ok": True}


async def test_standalone_gate_protects_only_operator_routes(_enable_auth):
    async with _client(_standalone_app()) as c:
        data = await c.get("/data-bucket/object")
        denied = await c.get("/_admin/stats")
        s3_credential = await c.get(
            "/_admin/stats",
            headers={"Authorization": "AWS4-HMAC-SHA256 Credential=AKIA/example"},
        )
        allowed = await c.get("/_admin/stats", headers=_basic("operator", "s3cret"))
    assert data.status_code == 200
    assert denied.status_code == 401
    assert s3_credential.status_code == 401
    assert allowed.status_code == 200


async def test_basic_identity_reaches_rbac_in_composed_stack(_enable_auth):
    from security.authorization_middleware import AuthorizationMiddleware

    app = FastAPI()
    app.add_middleware(AuthorizationMiddleware)
    app.add_middleware(ManagerAuthMiddleware)

    @app.post("/_config/api/save")
    async def save():
        return {"ok": True}

    async with _client(app) as c:
        denied = await c.post("/_config/api/save")
        allowed = await c.post(
            "/_config/api/save", headers=_basic("operator", "s3cret")
        )
    assert denied.status_code == 401
    assert allowed.status_code == 200


@pytest.mark.parametrize(
    ("enabled", "password"),
    [(False, "configured"), (True, "")],
)
async def test_standalone_incomplete_auth_fails_closed_only_for_operator_routes(
    monkeypatch, enabled, password
):
    monkeypatch.setattr(config, "MANAGER_AUTH_ENABLED", enabled, raising=False)
    monkeypatch.setattr(config, "MANAGER_AUTH_PASSWORD", password, raising=False)
    async with _client(_standalone_app()) as c:
        assert (await c.get("/_admin/stats")).status_code == 503
        assert (await c.get("/data-bucket/object")).status_code == 200


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
        c.cookies.set("fsp_session", session)
        response = await c.get("/_manager/api/fleet")
    assert response.status_code == 200


async def test_valid_oidc_bearer_also_passes_manager_basic_gate(_enable_auth, monkeypatch):
    from security.authorization import User

    monkeypatch.setattr(
        "security.identity.authenticate_oidc_token",
        lambda token: User("external-ops", roles=("monitor_troubleshooter",))
        if token == "signed-token" else None,
    )
    async with _client(_app()) as c:
        allowed = await c.get(
            "/_manager/api/fleet", headers={"Authorization": "Bearer signed-token"}
        )
        denied = await c.get(
            "/_manager/api/fleet", headers={"Authorization": "Bearer invalid-token"}
        )
    assert allowed.status_code == 200
    assert denied.status_code == 401


async def test_identity_login_bootstrap_avoids_browser_basic_challenge(_enable_auth):
    async with _client(_app()) as c:
        assert (await c.get("/_config")).status_code == 200
        assert (await c.get("/_config/api/authorization/status")).status_code == 200
        assert (await c.post("/_config/api/authorization/login")).status_code == 200
        protected = await c.get("/_config/api/tables")
    assert protected.status_code == 401
    assert "www-authenticate" not in protected.headers


async def test_config_builder_bootstrap_does_not_depend_on_authz_mode(_enable_auth, monkeypatch):
    monkeypatch.delenv("FSP_AUTHZ_ENFORCE", raising=False)
    async with _client(_app()) as c:
        shell = await c.get("/_config")
        protected = await c.get("/_config/api/tables")
    assert shell.status_code == 200
    assert protected.status_code == 401
    assert "www-authenticate" not in protected.headers


async def test_health_exempt_control_protected(_enable_auth):
    async with _client(_app()) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.post("/control/register")).status_code == 401
        assert (await c.post("/control/register",
                             headers=_basic("operator", "s3cret"))).status_code == 200
