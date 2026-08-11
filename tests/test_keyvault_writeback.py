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


class _FakePoller:
    def wait(self):
        return None


class FakeSecretClient:
    def __init__(self, secrets=None):
        self.secrets = dict(secrets or {})
        self.set_calls = []
        self.deleted = []

    def get_secret(self, name):
        if name not in self.secrets:
            raise ResourceNotFoundError(name)
        return _FakeSecret(self.secrets[name])

    def set_secret(self, name, value):
        self.set_calls.append((name, value))
        self.secrets[name] = value
        return _FakeSecret(value)

    def begin_delete_secret(self, name):
        self.deleted.append(name)
        self.secrets.pop(name, None)
        return _FakePoller()


class _BoomSetClient:
    def set_secret(self, name, value):
        raise RuntimeError("write denied")

    def get_secret(self, name):
        raise RuntimeError("network down")

    def begin_delete_secret(self, name):
        raise RuntimeError("delete denied")


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


def test_access_key_writes_back_secret_and_acls(tmp_path):
    src, cfg = _source()
    store = _store(tmp_path)
    store.write_through = keyvault.write_through_for(src, cfg)
    record = {
        "access_key_id": "FSPKEY1",
        "secret_key": "s3cr3t",
        "label": "finance-reader",
        "allowed_buckets": ["secure-nfs", "s3vault"],
        "allowed_prefixes": {"s3vault": ["2026/", "2025/"]},
        "permissions": "read",
        "enabled": True,
    }
    store.set_access_key("FSPKEY1", record)
    written = dict(src._client.set_calls)["access-key-fspkey1"]
    # The whole record — secret AND ACL scope — is persisted to the vault.
    assert json.loads(written) == record


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
# Delete write-through (item 2)
# ---------------------------------------------------------------------------

def test_delete_through_removes_url_secret_and_access_key(tmp_path):
    src, cfg = _source()
    store = _store(tmp_path)
    store.write_through = keyvault.write_through_for(src, cfg)
    store.delete_through = keyvault.delete_through_for(src, cfg)

    store.set_url("warehouse_pg", "postgresql://u:p@h/wh")
    store.set_secret("blobvault", {"mode": "account_key"})
    store.set_access_key("FSPKEY1", {"access_key_id": "FSPKEY1", "secret_key": "x",
                                     "label": "", "allowed_buckets": ["*"],
                                     "allowed_prefixes": {}, "permissions": "read",
                                     "enabled": True})

    assert store.delete("warehouse_pg") is True
    assert store.delete_secret("blobvault") is True
    assert store.delete_access_key("FSPKEY1") is True

    assert "db-url-warehouse-pg" in src._client.deleted
    assert "blobvault" in src._client.deleted
    assert "access-key-fspkey1" in src._client.deleted


def test_delete_through_is_fail_soft(tmp_path):
    src, cfg = _source(client=_BoomSetClient())   # begin_delete_secret raises
    store = _store(tmp_path)
    store.delete_through = keyvault.delete_through_for(src, cfg)
    store.set_url("default", "postgresql://u:p@h/db")   # local only (write_through None)
    assert store.delete("default") is True              # local delete still succeeds
    assert store.get_url("default") is None


def test_delete_by_name_not_found_is_noop():
    src, _ = _source(client=FakeSecretClient())
    src.delete_by_name("never-existed")   # must not raise


# ---------------------------------------------------------------------------
# Config-file secret write-back (item 1)
# ---------------------------------------------------------------------------

def _patch_source(monkeypatch, client):
    orig = keyvault.KeyVaultSecretSource
    monkeypatch.setattr(keyvault, "KeyVaultSecretSource",
                        lambda cfg, **kw: orig(cfg, client=client))


def test_write_back_config_secrets_writes(monkeypatch):
    client = FakeSecretClient()
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", True)
    monkeypatch.setattr(system_config, "KEYVAULT_URI", "https://v.vault.azure.net")
    _patch_source(monkeypatch, client)
    done = keyvault.write_back_config_secrets({
        "secret_access_key": "AKIASECRET",
        "admin_token": "ADMIN",
        "manager_auth_password": "MPW",
        "unrelated": "ignored",
    })
    names = dict(client.set_calls)
    assert names["s3-secret-access-key"] == "AKIASECRET"
    assert names["admin-token"] == "ADMIN"
    assert names["manager-auth-password"] == "MPW"
    assert set(done) == {"secret_access_key", "admin_token", "manager_auth_password"}


def test_write_back_config_secret_empty_clears(monkeypatch):
    client = FakeSecretClient(secrets={"admin-token": "OLD"})
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", True)
    monkeypatch.setattr(system_config, "KEYVAULT_URI", "https://v.vault.azure.net")
    _patch_source(monkeypatch, client)
    done = keyvault.write_back_config_secrets({"admin_token": ""})
    assert "admin-token" in client.deleted
    assert done == ["admin_token"]


def test_write_back_config_secret_ignores_masked(monkeypatch):
    client = FakeSecretClient()
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", True)
    monkeypatch.setattr(system_config, "KEYVAULT_URI", "https://v.vault.azure.net")
    _patch_source(monkeypatch, client)
    done = keyvault.write_back_config_secrets({"admin_token": "abc***xyz"})
    assert client.set_calls == [] and client.deleted == []
    assert done == []


def test_write_back_config_secrets_disabled(monkeypatch):
    monkeypatch.setattr(system_config, "KEYVAULT_WRITE_BACK", False)
    assert keyvault.write_back_config_secrets({"admin_token": "X"}) == []


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
    assert store.delete_through is not None


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
