"""Phase 0 (issue #16) — shared outbound Azure identity provider.

Verifies `security.azure_credential.get_credential` builds the right
`azure.identity` credential per mode, and that non-identity modes and a missing
SDK are handled. A fake `azure.identity` is injected so no real Azure SDK is
required (mirrors the stub approach in test_storage_proxy_azure.py).
"""
from __future__ import annotations

import sys
import types

import pytest

from security import azure_credential
from storage import azure_auth


def _install_fake_identity(monkeypatch):
    identity = types.ModuleType("azure.identity")

    class ClientSecretCredential:
        def __init__(self, tenant_id, client_id, client_secret):
            self.args = (tenant_id, client_id, client_secret)

    class ManagedIdentityCredential:
        def __init__(self, client_id=None):
            self.client_id = client_id

    class DefaultAzureCredential:
        def __init__(self):
            self.kind = "default"

    identity.ClientSecretCredential = ClientSecretCredential
    identity.ManagedIdentityCredential = ManagedIdentityCredential
    identity.DefaultAzureCredential = DefaultAzureCredential

    azure_pkg = sys.modules.get("azure") or types.ModuleType("azure")
    monkeypatch.setitem(sys.modules, "azure", azure_pkg)
    monkeypatch.setitem(sys.modules, "azure.identity", identity)
    return identity


def test_service_principal_both_aliases(monkeypatch):
    _install_fake_identity(monkeypatch)
    for mode in ("aad_client_secret", "service_principal"):
        cred = azure_credential.get_credential(
            mode, tenant_id="t", client_id="c", client_secret="s")
        assert cred.args == ("t", "c", "s")


def test_managed_identity_passes_client_id(monkeypatch):
    _install_fake_identity(monkeypatch)
    assert azure_credential.get_credential("managed_identity", client_id="mi").client_id == "mi"
    # No client id -> None (system-assigned).
    assert azure_credential.get_credential("managed_identity").client_id is None


def test_default_credential(monkeypatch):
    _install_fake_identity(monkeypatch)
    assert azure_credential.get_credential("default").kind == "default"


def test_case_insensitive_and_whitespace(monkeypatch):
    _install_fake_identity(monkeypatch)
    assert azure_credential.get_credential("  Default  ").kind == "default"


@pytest.mark.parametrize("mode", ["account_key", "sas", "anonymous", "", "bogus"])
def test_non_identity_modes_raise_value_error(mode):
    with pytest.raises(ValueError):
        azure_credential.get_credential(mode)


def test_missing_sdk_raises_install_hint():
    try:
        __import__("azure.identity")
    except ImportError:
        pass
    else:
        pytest.skip("azure-identity is installed")
    with pytest.raises(RuntimeError, match="azure-identity"):
        azure_credential.get_credential("default")


def test_azure_auth_delegates_identity_modes(monkeypatch):
    _install_fake_identity(monkeypatch)
    sp = azure_auth._credential_for(azure_auth.AzureAuthConfig(
        mode="aad_client_secret", tenant_id="t", client_id="c", client_secret="s"))
    assert sp.args == ("t", "c", "s")
    assert azure_auth._credential_for(azure_auth.AzureAuthConfig(
        mode="managed_identity", client_id="mi")).client_id == "mi"
    assert azure_auth._credential_for(azure_auth.AzureAuthConfig(mode="default")).kind == "default"


def test_azure_auth_non_identity_modes_unchanged():
    # These must not touch azure.identity at all.
    assert azure_auth._credential_for(azure_auth.AzureAuthConfig(
        mode="account_key", account_key="k")) == "k"
    assert azure_auth._credential_for(azure_auth.AzureAuthConfig(
        mode="sas", sas_token="tok")) == "tok"
    assert azure_auth._credential_for(azure_auth.AzureAuthConfig(mode="anonymous")) is None
