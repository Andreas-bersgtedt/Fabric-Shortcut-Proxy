"""Phase 2 (issue #16) — Key Vault startup hydration + background refresh.

Proves the never-fail posture: an unreachable vault falls back to the local
encrypted cache and only fails when `require_keyvault` is set and there is no
cache (owner directive). A fake SecretClient + identity cipher keep it SDK-free.
"""
from __future__ import annotations

import os

import pytest

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import system_config
from security import keyvault
from security.credential_store import CredentialStore

_ENV_VARS = ["DB_URL", "S3_SECRET_ACCESS_KEY", "ADMIN_TOKEN", "MANAGER_AUTH_PASSWORD"]


class ResourceNotFoundError(Exception):
    status_code = 404


class _FakeSecret:
    def __init__(self, value):
        self.value = value


class FakeSecretClient:
    def __init__(self, secrets):
        self._secrets = dict(secrets)

    def get_secret(self, name):
        if name not in self._secrets:
            raise ResourceNotFoundError(name)
        return _FakeSecret(self._secrets[name])


class _BoomClient:
    def get_secret(self, name):
        raise RuntimeError("network down")


class _IdCipher:
    name = "test"

    def encrypt(self, b):
        return b

    def decrypt(self, b):
        return b


def _source(secrets):
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v.vault.azure.net")
    return keyvault.KeyVaultSecretSource(cfg, client=FakeSecretClient(secrets))


def _boom_source():
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v.vault.azure.net")
    return keyvault.KeyVaultSecretSource(cfg, client=_BoomClient())


def _store(tmp_path):
    return CredentialStore(path=str(tmp_path / "cred.json"), cipher=_IdCipher())


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_hydrate_populates_env_and_cache(tmp_path):
    src = _source({
        "db-url": "postgresql://u:p@h/db",
        "s3-secret-access-key": "S3SEC",
        "admin-token": "ADM",
        "manager-auth-password": "MPW",
    })
    store = _store(tmp_path)
    hydrated = keyvault.hydrate_from_keyvault(store, source=src)

    assert set(hydrated) == set(_ENV_VARS)
    assert os.environ["DB_URL"] == "postgresql://u:p@h/db"
    assert os.environ["S3_SECRET_ACCESS_KEY"] == "S3SEC"
    assert os.environ["ADMIN_TOKEN"] == "ADM"
    assert os.environ["MANAGER_AUTH_PASSWORD"] == "MPW"
    # Written through to the encrypted cache + on-demand read-through attached.
    assert "default" in store.list_ids()
    assert store.get_secret("env:admin_token") == {"value": "ADM"}
    assert store.read_through is not None


def test_explicit_env_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_URL", "postgresql://explicit/win")
    src = _source({"db-url": "postgresql://kv/value"})
    hydrated = keyvault.hydrate_from_keyvault(_store(tmp_path), source=src)
    assert "DB_URL" not in hydrated
    assert os.environ["DB_URL"] == "postgresql://explicit/win"


def test_outage_falls_back_to_cache(tmp_path):
    store = _store(tmp_path)
    store.set_url("default", "postgresql://cached/db")
    store.set_secret("env:admin_token", {"value": "CACHEDADM"})

    hydrated = keyvault.hydrate_from_keyvault(store, source=_boom_source())

    assert os.environ["DB_URL"] == "postgresql://cached/db"
    assert os.environ["ADMIN_TOKEN"] == "CACHEDADM"
    assert "DB_URL" in hydrated and "ADMIN_TOKEN" in hydrated


def test_outage_no_cache_no_require_never_fails(tmp_path):
    hydrated = keyvault.hydrate_from_keyvault(_store(tmp_path), source=_boom_source())
    assert hydrated == []
    assert "DB_URL" not in os.environ


def test_require_keyvault_cold_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(system_config, "REQUIRE_KEYVAULT", True)
    with pytest.raises(keyvault.KeyVaultUnavailable):
        keyvault.hydrate_from_keyvault(_store(tmp_path), source=_boom_source())


def test_disabled_keyvault_is_noop():
    # No source + default config (no KEYVAULT_URI) => nothing happens.
    assert keyvault.hydrate_from_keyvault() == []
    assert "DB_URL" not in os.environ


def test_refresh_once_updates_cache_and_env(tmp_path):
    src = _source({"db-url": "postgresql://rotated/db", "admin-token": "NEWADM"})
    store = _store(tmp_path)
    names = keyvault.refresh_secrets_once(src, store)
    assert "DB_URL" in names and "ADMIN_TOKEN" in names
    assert os.environ["DB_URL"] == "postgresql://rotated/db"
    assert store.get_secret("env:admin_token") == {"value": "NEWADM"}


def test_refresh_never_raises_on_outage(tmp_path):
    assert keyvault.refresh_secrets_once(_boom_source(), _store(tmp_path)) == []
