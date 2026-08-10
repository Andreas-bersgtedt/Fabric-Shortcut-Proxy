"""Phase 3 (issue #16) — Key Vault advisory status for /readyz + monitor.

Verifies the non-secret status snapshot (disabled / ok / degraded), that refresh
records the outcome, and that no secret value ever leaks into the status blob.
"""
from __future__ import annotations

import json
import os

import pytest

os.environ.setdefault("DB_URL", "sqlite+aiosqlite:///:memory:")

from security import keyvault
from security.credential_store import CredentialStore


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


def _cfg():
    return keyvault.KeyVaultConfig(vault_uri="https://myvault.vault.azure.net", auth_mode="managed_identity")


def _source(secrets):
    return keyvault.KeyVaultSecretSource(_cfg(), client=FakeSecretClient(secrets))


def _boom():
    return keyvault.KeyVaultSecretSource(_cfg(), client=_BoomClient())


def _store(tmp_path):
    return CredentialStore(path=str(tmp_path / "cred.json"), cipher=_IdCipher())


@pytest.fixture(autouse=True)
def _reset_status():
    keyvault._STATUS.update(ok=None, at="", secrets=[], error="")
    yield


def test_status_disabled_by_default():
    st = keyvault.status_snapshot()
    assert st["enabled"] is False
    assert st["status"] == "disabled"


def test_status_reports_non_secret_fields(tmp_path):
    st = keyvault.status_snapshot(cfg=_cfg(), store=_store(tmp_path))
    assert st["enabled"] is True
    assert st["vault"] == "myvault.vault.azure.net"      # host only, no scheme
    assert st["auth_mode"] == "managed_identity"
    assert st["status"] == "ok"                           # no failure recorded yet


def test_refresh_ok_status_and_no_secret_leak(tmp_path):
    src = _source({"db-url": "postgresql://u:S3CR3TPW@h/db", "admin-token": "T0PSECRET"})
    store = _store(tmp_path)
    keyvault.refresh_secrets_once(src, store)
    st = keyvault.status_snapshot(cfg=src.config, store=store)

    assert st["status"] == "ok"
    assert st["last_refresh"]["ok"] is True
    assert "DB_URL" in st["last_refresh"]["secrets"]
    assert st["cached"] is True
    # Secret values must never appear in the advisory status.
    blob = json.dumps(st)
    assert "S3CR3TPW" not in blob
    assert "T0PSECRET" not in blob


def test_refresh_degraded_on_outage(tmp_path):
    keyvault.refresh_secrets_once(_boom(), _store(tmp_path))
    st = keyvault.status_snapshot(cfg=_cfg())
    assert st["last_refresh"]["ok"] is False
    assert st["status"] == "degraded"
    assert "network down" in st["last_refresh"]["error"]
