from __future__ import annotations

import pytest
from fastapi import Request

from security.authorization import (
    AuthorizationError,
    PermissionGrant,
    User,
    UserDirectory,
    authorize,
    authenticate_admin_token,
    require,
)


def test_monitor_troubleshooter_can_monitor_and_troubleshoot_only():
    user = User("ops-user", roles=("monitor_troubleshooter",))

    assert user.can("monitor.read")
    assert user.can("troubleshoot.read")
    assert not user.can("config.read")
    assert not user.can("config.write")
    assert not user.can("security.metadata.read")
    assert not user.can("security.credentials.admin")
    assert not user.can("tokenization.policy.admin")
    assert not user.can("users.admin")


def test_context_scoped_grant_matches_only_its_context():
    user = User(
        "dev-operator",
        grants=(PermissionGrant("config.write", {"environment": "development"}),),
    )
    assert user.can("config.write", {"environment": "development", "table": "sales"})
    assert not user.can("config.write", {"environment": "production"})
    assert not user.can("config.write")


def test_wildcard_context_grant_and_disabled_user():
    user = User(
        "support",
        grants=(PermissionGrant("troubleshoot.read", {"environment": "*"}),),
    )
    assert authorize(user, "troubleshoot.read", {"environment": "production"}).allowed
    disabled = User("disabled", roles=("system_administrator",), enabled=False)
    assert not authorize(disabled, "system.admin").allowed


def test_unknown_permission_is_fail_closed():
    user = User("ops", roles=("system_administrator",))
    decision = authorize(user, "credentials.read")
    assert not decision.allowed
    assert decision.reason == "unknown permission"
    with pytest.raises(AuthorizationError, match="permission denied"):
        require(User("ops"), "monitor.read")


def test_user_rejects_unknown_role_and_invalid_grant():
    with pytest.raises(ValueError, match="unknown roles"):
        User("ops", roles=("root",))
    with pytest.raises(ValueError, match="unknown permission"):
        PermissionGrant("security.keys.read")


def test_user_directory_round_trips_secret_free_identity_metadata(tmp_path):
    directory = UserDirectory([
        User(
            "ops-user", roles=("monitor_troubleshooter",),
            grants=(PermissionGrant("troubleshoot.read", {"environment": "prod"}),),
        ),
    ])
    path = tmp_path / "users.json"
    directory.save(str(path))
    loaded = UserDirectory.load(str(path))
    assert loaded.list_public() == directory.list_public()
    assert "password" not in path.read_text(encoding="utf-8").lower()
    assert loaded.get("ops-user").can("troubleshoot.read", {"environment": "prod"})


def test_user_directory_rejects_credentials_and_unknown_user():
    with pytest.raises(ValueError, match="credentials or tokens"):
        User.from_dict({"user_id": "ops", "password_hash": "x"})
    with pytest.raises(PermissionError, match="user not found"):
        UserDirectory().get("missing")


def test_user_directory_protects_last_enabled_system_admin():
    directory = UserDirectory([
        User("admin", roles=("system_administrator",)),
        User("second", roles=("system_administrator",)),
    ])
    directory.disable("second")
    with pytest.raises(PermissionError, match="last enabled"):
        directory.disable("admin")


def test_admin_token_authentication_is_constant_time_and_system_admin(monkeypatch):
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    user = authenticate_admin_token("admin-test-token")
    assert user is not None
    assert user.user_id == "admin-token"
    assert user.can("users.admin")
    assert authenticate_admin_token("wrong") is None


async def test_authorization_endpoints_require_admin_and_hide_user_secrets(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    user_path = tmp_path / "users.json"
    UserDirectory([
        User("ops-user", roles=("monitor_troubleshooter",)),
    ]).save(str(user_path))
    monkeypatch.setenv("FSP_USER_DIRECTORY_FILE", str(user_path))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        denied = await client.get("/_config/api/authorization/users")
        me = await client.get(
            "/_config/api/authorization/me",
            headers={"X-Admin-Token": "admin-test-token"},
        )
        users = await client.get(
            "/_config/api/authorization/users",
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert denied.status_code == 401
    assert me.status_code == 200
    assert "system.admin" in me.json()["permissions"]
    assert users.status_code == 200
    assert users.json()["users"][0]["user_id"] == "ops-user"
    assert "password" not in users.text.lower()


async def test_authorization_user_mutations_are_admin_only_and_preserve_last_admin(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    user_path = tmp_path / "users.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(tmp_path / "identities.json"))
    UserDirectory([User("admin", roles=("system_administrator",))]).save(str(user_path))
    monkeypatch.setenv("FSP_USER_DIRECTORY_FILE", str(user_path))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    payload = {
        "user_id": "support", "roles": ["monitor_troubleshooter"],
        "password": "correct horse battery staple",
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        denied = await client.post("/_config/api/authorization/users", json=payload)
        created = await client.post(
            "/_config/api/authorization/users", json=payload,
            headers={"X-Admin-Token": "admin-test-token"},
        )
        disabled = await client.delete(
            "/_config/api/authorization/users/support",
            headers={"X-Admin-Token": "admin-test-token"},
        )
        last_admin = await client.delete(
            "/_config/api/authorization/users/admin",
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert denied.status_code == 401
    assert created.status_code == 200
    assert created.json()["user"]["user_id"] == "support"
    assert disabled.status_code == 200
    assert last_admin.status_code == 409

    from security.identity import IdentityProvider
    assert IdentityProvider(str(tmp_path / "identities.json")).authenticate(
        "support", "correct horse battery staple"
    ) is None


async def test_user_creation_validates_password_before_metadata_write(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI

    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    user_path = tmp_path / "users.json"
    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_USER_DIRECTORY_FILE", str(user_path))
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/_config/api/authorization/users",
            json={"user_id": "invalid", "roles": ["viewer"], "password": "short"},
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert response.status_code == 400
    assert not user_path.exists()
    assert not identity_path.exists()


async def test_local_login_session_me_and_logout(tmp_path, monkeypatch):
    import httpx
    from fastapi import FastAPI
    from security.identity import IdentityProvider

    identity_path = tmp_path / "identities.json"
    monkeypatch.setenv("FSP_IDENTITY_FILE", str(identity_path))
    IdentityProvider(str(identity_path)).create_or_replace(
        User("ops", roles=("monitor_troubleshooter",)),
        "correct horse battery staple",
    )
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        login = await client.post(
            "/_config/api/authorization/login",
            json={"user_id": "ops", "password": "correct horse battery staple"},
        )
        me = await client.get("/_config/api/authorization/me")
        logout = await client.post("/_config/api/authorization/logout")
        after = await client.get("/_config/api/authorization/me")
    assert login.status_code == 200
    assert "fsp_session" in login.cookies
    assert me.status_code == 200
    assert "monitor.read" in me.json()["permissions"]
    assert "config.write" not in me.json()["permissions"]
    assert logout.status_code == 200
    assert after.status_code == 401


async def test_authorization_status_reports_only_enforcement_mode(monkeypatch):
    import httpx
    from fastapi import FastAPI

    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "1")
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/_config/api/authorization/status")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "enforced": True}


def test_authorization_route_map_separates_security_from_config():
    from security.authorization_middleware import _permission

    assert _permission("/_config/api/credentials", "GET") == "security.metadata.read"
    assert _permission("/_config/api/credentials", "POST") == "security.credentials.admin"
    assert _permission("/_config/api/access-keys/key", "DELETE") == "security.credentials.admin"
    assert _permission("/_config/api/save", "POST") == "config.write"
    assert _permission("/_config/api/tokenization/policies", "GET") == "tokenization.policy.read"
    assert _permission("/_config/api/tokenization/policies", "POST") == "tokenization.policy.admin"


def test_authorization_context_ignores_caller_supplied_scope_claims():
    from starlette.requests import Request
    from security.authorization_middleware import _context

    scope = {
        "type": "http", "path": "/_config/api/tokenization/policies/policy-v1",
        "query_string": b"environment=production", "headers": [
            (b"x-fsp-context-environment", b"production"),
        ],
    }
    context = _context(Request(scope))
    assert context == {"policy_namespace": "policy-v1"}


async def test_authorization_middleware_enforces_operator_functions(monkeypatch):
    import httpx
    from fastapi import FastAPI
    from security.authorization_middleware import AuthorizationMiddleware

    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "1")
    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    app = FastAPI()
    app.add_middleware(AuthorizationMiddleware)

    @app.get("/_config/api/safe-read")
    async def safe_read(request: Request):
        return {"ok": True, "user": request.state.user.user_id}

    @app.post("/_config/api/safe-write")
    async def safe_write():
        return {"ok": True}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        denied = await client.get("/_config/api/safe-read")
        allowed = await client.get(
            "/_config/api/safe-read",
            headers={"X-Admin-Token": "admin-test-token"},
        )
        write = await client.post(
            "/_config/api/safe-write",
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert denied.status_code == 401
    assert allowed.status_code == 200, allowed.text
    assert allowed.json()["user"] == "admin-token"
    assert write.status_code == 200


async def test_config_mutations_require_config_write_when_enforced(monkeypatch):
    import httpx
    from fastapi import FastAPI

    monkeypatch.setenv("ADMIN_TOKEN", "admin-test-token")
    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "1")
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        denied = await client.post("/_config/api/save", json={"settings": {"bucket": "x"}})
        allowed = await client.post(
            "/_config/api/save", json={"settings": {"unknown_setting": True}},
            headers={"X-Admin-Token": "admin-test-token"},
        )
    assert denied.status_code == 401
    assert allowed.status_code == 400
    assert "unknown_setting" in allowed.text
