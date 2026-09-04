from __future__ import annotations

import pytest

from security.authorization import (
    AuthorizationError,
    PermissionGrant,
    User,
    UserDirectory,
    authorize,
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
