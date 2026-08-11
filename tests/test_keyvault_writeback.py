"""Phase 4 (issue #16) — Key Vault write-back (Manager persists credentials to KV).

The CredentialStore write_through hook writes a saved credential to Key Vault after
the local encrypted save; it is fail-soft (a KV write failure never blocks the save).
A fake SecretClient captures writes so no Azure SDK is needed.
"""
from __future__ import annotations

import json
import os

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

import pytest

import system_config
from security import keyvault
from security.credential_store import CredentialStore


class ResourceNotFoundError(Exception):
    status_code = 404


class _FakeSecret:
    def __init__(self, value):
        self.value = value


class FakeSecretClient:
    def __init__(self, secrets=None):
        self.secrets = dict(secrets or {})
        self.set_calls = []

    def get_secret(self, name):
        if name not in self.secrets:
            raise ResourceNotFoundError(name)
        return _FakeSecret(self.secrets[name])

    def set_secret(self, name, value):
        self.set_calls.append((name, value))
        self.secrets[name] = value
        return _FakeSecret(value)


class _BoomSetClient:
    def set_secret(self, name, value):
        raise RuntimeError("write denied")

    def get_secret(self, name):
        raise RuntimeError("network down")


class _IdCipher:
    name = "test"

    def encrypt(self, b):
        return b

    def decrypt(self, b):
        return b


def _source(client=None):
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v.vault.azure.net")
    return keyvault.KeyVaultSecretSource(cfg, client=client or FakeSecretClient()), cfg


def _store(tmp_path):
    return CredentialStore(path=str(tmp_path / "cred.json"), cipher=_IdCipher())


# ---------------------------------------------------------------------------
# Source write methods
# ---------------------------------------------------------------------------

def test_set_by_name_and_secret_value():
    src, _ = _source()
    src.set_by_name("db-url", "postgresql://u:p@h/db")
    assert ("db-url", "postgresql://u:p@h/db") in src._client.set_calls
    src.set_secret_value("admin_token", "T0K")
    assert ("admin-token", "T0K") in src._client.set_calls


def test_set_raises_on_error():
    src, _ = _source(client=_BoomSetClient())
    with pytest.raises(keyvault.KeyVaultUnavailable):
        src.set_by_name("db-url", "x")


# ---------------------------------------------------------------------------
# CredentialStore write-back
# ---------------------------------------------------------------------------

def test_credential_store_writes_back_url_and_secret(tmp_path):
    src, cfg = _source()
    store = _store(tmp_path)
    store.write_through = keyvault.write_through_for(src, cfg)

    store.set_url("default", "postgresql://u:p@h/db")
    store.set_secret("blobvault", {"mode": "account_key", "account_key": "abc"})

    names = dict(src._client.set_calls)
    assert names["db-url"] == "postgresql://u:p@h/db"
    assert json.loads(names["blobvault"]) == {"mode": "account_key", "account_key": "abc"}
    # Local persistence still happened.
    assert store.get_url("default") == "postgresql://u:p@h/db"


def test_named_connection_writes_slugged_name(tmp_path):
    src, cfg = _source()
    store = _store(tmp_path)
    store.write_through = keyvault.write_through_for(src, cfg)
    store.set_url("warehouse_pg", "postgresql://u:p@h/wh")
    assert dict(src._client.set_calls)["db-url-warehouse-pg"] == "postgresql://u:p@h/wh"


def test_write_back_is_fail_soft(tmp_path):
    src, cfg = _source(client=_BoomSetClient())
    store = _store(tmp_path)
    store.write_through = keyvault.write_through_for(src, cfg)
    # KV write raises internally, but the local save must still succeed.
    store.set_url("default", "postgresql://u:p@h/db")
    assert store.get_url("default") == "postgresql://u:p@h/db"
    assert "default" in store.list_ids()


def test_no_write_through_is_unchanged(tmp_path):
    store = _store(tmp_path)
    store.set_url("default", "postgresql://u:p@h/db")   # write_through is None
    assert store.get_url("default") == "postgresql://u:p@h/db"


# ---------------------------------------------------------------------------
# attach_write_back gating
# ---------------------------------------------------------------------------

def test_attach_write_back_disabled_by_default(tmp_path):
    store = _store(tmp_path)
    assert keyvault.attach_write_back(store) is False
    assert store.write_through is None


def test_attach_write_back_enabled(tmp_path, monkeypatch):
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", True)
    monkeypatch.setattr(system_config, "KEYVAULT_URI", "https://v.vault.azure.net")
    store = _store(tmp_path)
    assert keyvault.attach_write_back(store) is True
    assert store.write_through is not None


def test_attach_write_back_no_uri(tmp_path, monkeypatch):
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", True)
    monkeypatch.setattr(system_config, "KEYVAULT_URI", "")
    store = _store(tmp_path)
    assert keyvault.attach_write_back(store) is False


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def test_write_back_setting_defaults_and_persists(tmp_path, monkeypatch):
    import config
    assert config.KEYVAULT_WRITE_BACK is False
    monkeypatch.chdir(tmp_path)
    config.write_config_updates({"keyvault_write_back": True})
    saved = json.loads((tmp_path / "config.system.json").read_text(encoding="utf-8"))["system"]
    assert saved["keyvault_write_back"] is True
