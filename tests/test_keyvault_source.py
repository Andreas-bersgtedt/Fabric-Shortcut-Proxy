"""Phase 1 (issue #16) — Azure Key Vault secret source + cache-first read-through.

Verifies the name convention, the source's found/not-found/error handling, the
connectivity probe, and the CredentialStore write-through on a local miss. A fake
SecretClient is injected so no real Azure SDK is required, and a trivial identity
cipher backs the store so the test runs without `cryptography`.
"""
from __future__ import annotations

import json
import os

import pytest

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

from security import keyvault
from security.credential_store import CredentialStore


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class ResourceNotFoundError(Exception):
    """Mirrors azure.core.exceptions.ResourceNotFoundError by name/status."""
    status_code = 404


class _FakeSecret:
    def __init__(self, value):
        self.value = value


class FakeSecretClient:
    def __init__(self, secrets):
        self._secrets = dict(secrets)      # kv name -> value
        self.calls = []

    def get_secret(self, name):
        self.calls.append(name)
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


def _source(secrets, **cfg_kwargs):
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v.vault.azure.net", **cfg_kwargs)
    return keyvault.KeyVaultSecretSource(cfg, client=FakeSecretClient(secrets)), cfg


def _store(tmp_path):
    return CredentialStore(path=str(tmp_path / "cred.json"), cipher=_IdCipher())


# ---------------------------------------------------------------------------
# Name convention
# ---------------------------------------------------------------------------

def test_secret_name_convention_defaults():
    assert keyvault.secret_name_for("db_url") == "db-url"
    assert keyvault.secret_name_for("s3_secret_access_key") == "s3-secret-access-key"
    assert keyvault.secret_name_for("admin_token") == "admin-token"
    # Unknown key -> slugified.
    assert keyvault.secret_name_for("Some_Mount_ID") == "some-mount-id"


def test_secret_name_override_map():
    cfg = keyvault.KeyVaultConfig(vault_uri="x", overrides={"db_url": "prod-db-conn"})
    assert keyvault.secret_name_for("db_url", cfg) == "prod-db-conn"


def test_config_enabled_flag():
    assert not keyvault.KeyVaultConfig().enabled
    assert keyvault.KeyVaultConfig(vault_uri="https://v").enabled


# ---------------------------------------------------------------------------
# Source fetch
# ---------------------------------------------------------------------------

def test_get_by_name_found_and_missing():
    src, _ = _source({"db-url": "postgresql://u:p@h/db"})
    assert src.get_by_name("db-url") == "postgresql://u:p@h/db"
    assert src.get_by_name("nope") is None


def test_get_secret_value_maps_name():
    src, _ = _source({"admin-token": "T0K"})
    assert src.get_secret_value("admin_token") == "T0K"


def test_transport_error_raises_unavailable():
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v")
    src = keyvault.KeyVaultSecretSource(cfg, client=_BoomClient())
    with pytest.raises(keyvault.KeyVaultUnavailable):
        src.get_by_name("db-url")


def test_probe_disabled_when_no_uri():
    src = keyvault.KeyVaultSecretSource(keyvault.KeyVaultConfig())
    ok, detail = src.probe()
    assert ok is False and "URI" in detail


def test_probe_ok_with_injected_client():
    src, _ = _source({})
    assert src.probe() == (True, "ok")


# ---------------------------------------------------------------------------
# CredentialStore cache-first read-through (write-through on miss)
# ---------------------------------------------------------------------------

def test_read_through_url_writes_through(tmp_path):
    src, cfg = _source({"db-url": "postgresql://u:p@h/db"})
    store = _store(tmp_path)
    store.read_through = keyvault.read_through_for(src, cfg)

    # Cold: not local -> resolves from KV and persists.
    assert store.get_url("default") == "postgresql://u:p@h/db"
    assert "default" in store.list_ids()
    # Warm: served locally, no further KV call.
    calls_before = len(src._client.calls)
    assert store.get_url("default") == "postgresql://u:p@h/db"
    assert len(src._client.calls) == calls_before


def test_read_through_named_connection_slug(tmp_path):
    src, cfg = _source({"db-url-warehouse-pg": "postgresql://u:p@h/wh"})
    store = _store(tmp_path)
    store.read_through = keyvault.read_through_for(src, cfg)
    assert store.get_url("warehouse_pg") == "postgresql://u:p@h/wh"


def test_read_through_secret_blob_writes_through(tmp_path):
    blob = {"mode": "account_key", "account_key": "abc123"}
    src, cfg = _source({"blobvault": json.dumps(blob)})
    store = _store(tmp_path)
    store.read_through = keyvault.read_through_for(src, cfg)
    assert store.get_secret("blobvault") == blob
    assert "blobvault" in store.list_secret_ids()


def test_read_through_absent_returns_none(tmp_path):
    src, cfg = _source({})
    store = _store(tmp_path)
    store.read_through = keyvault.read_through_for(src, cfg)
    assert store.get_url("default") is None
    assert store.list_ids() == []


def test_read_through_never_raises_on_source_error(tmp_path):
    cfg = keyvault.KeyVaultConfig(vault_uri="https://v")
    src = keyvault.KeyVaultSecretSource(cfg, client=_BoomClient())
    store = _store(tmp_path)
    store.read_through = keyvault.read_through_for(src, cfg)
    # Owner directive: an unreachable vault must never fail the caller.
    assert store.get_url("default") is None


def test_no_read_through_is_unchanged(tmp_path):
    store = _store(tmp_path)
    assert store.get_url("default") is None
    assert store.get_secret("x") is None


# ---------------------------------------------------------------------------
# Config section defaults
# ---------------------------------------------------------------------------

def test_auth_config_section_defaults():
    import config
    assert config.AUTH_MODE == "default"
    assert config.KEYVAULT_URI == ""
    assert config.REQUIRE_KEYVAULT is False
    assert config.KEYVAULT_REFRESH_SECONDS == 300
    assert config.KEYVAULT_CACHE_TTL == 0
