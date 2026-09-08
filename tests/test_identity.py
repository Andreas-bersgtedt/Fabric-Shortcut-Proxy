from __future__ import annotations

import json

import pytest

from security.authorization import User
from security.identity import IdentityProvider, hash_password, verify_password


def test_password_hash_is_salted_and_verifiable():
    first = hash_password("correct horse battery staple")
    second = hash_password("correct horse battery staple")
    assert first != second
    assert verify_password("correct horse battery staple", first)
    assert not verify_password("wrong password", first)
    assert not verify_password("x", "malformed")


def test_identity_authentication_session_and_revocation(tmp_path):
    provider = IdentityProvider(str(tmp_path / "identities.json"), ttl_seconds=60)
    user = User("ops", roles=("monitor_troubleshooter",))
    provider.create_or_replace(user, "correct horse battery staple")
    assert provider.authenticate("ops", "wrong") is None
    authenticated = provider.authenticate("ops", "correct horse battery staple")
    assert authenticated is not None
    token = provider.create_session(authenticated)
    assert provider.resolve_session(token).user_id == "ops"
    provider.revoke_session(token)
    assert provider.resolve_session(token) is None
    raw = json.loads((tmp_path / "identities.json").read_text(encoding="utf-8"))
    assert "correct horse battery staple" not in json.dumps(raw)
    assert "credential_hash" in raw["identities"]["ops"]


def test_identity_disable_revokes_sessions(tmp_path):
    provider = IdentityProvider(str(tmp_path / "identities.json"))
    provider.create_or_replace(User("ops", roles=("monitor_troubleshooter",)), "correct horse battery staple")
    user = provider.authenticate("ops", "correct horse battery staple")
    token = provider.create_session(user)
    provider.disable("ops")
    assert provider.resolve_session(token) is None
    assert provider.authenticate("ops", "correct horse battery staple") is None


def test_short_password_rejected(tmp_path):
    with pytest.raises(ValueError, match="at least 12"):
        IdentityProvider(str(tmp_path / "identities.json")).create_or_replace(
            User("ops"), "short"
        )
