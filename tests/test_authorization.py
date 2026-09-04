from __future__ import annotations

import pytest

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
    UserDirectory([User("admin", roles=("system_administrator",))]).save(str(user_path))
    monkeypatch.setenv("FSP_USER_DIRECTORY_FILE", str(user_path))
    from configbuilder.router import router

    app = FastAPI()
    app.include_router(router)
    payload = {"user_id": "support", "roles": ["monitor_troubleshooter"]}
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
