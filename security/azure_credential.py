"""Shared outbound Azure identity provider (``azure-identity``).

Single place that builds ``azure.identity`` credentials for Managed Identity,
Service Principal, and ``DefaultAzureCredential``, so storage mounts and — from
issue #16 onward — Key Vault access construct their identity the same way instead
of duplicating the logic. Extracted from ``storage/azure_auth.py`` (Phase 0).

``azure-identity`` is imported lazily so the core install needs no Azure SDK;
:func:`get_credential` raises a clear install hint when it is absent.
"""
from __future__ import annotations

# Auth modes that resolve to an ``azure.identity`` credential object. Both
# ``aad_client_secret`` (the mount config's name) and ``service_principal`` (the
# issue #16 wording) select a client-secret service principal.
IDENTITY_MODES = frozenset({
    "aad_client_secret", "service_principal", "managed_identity", "default",
})

_INSTALL_HINT = (
    "this Azure auth mode needs azure-identity; install it with "
    "pip install 'fabric-shortcut-proxy[azureblob]'"
)


def _require_identity() -> None:
    try:
        import azure.identity  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise RuntimeError(_INSTALL_HINT) from exc


def get_credential(
    mode: str,
    *,
    tenant_id: str = "",
    client_id: str = "",
    client_secret: str = "",
):
    """Build an ``azure.identity`` credential for an identity-based auth mode.

    Supports ``managed_identity``, ``default``, and service-principal
    (``aad_client_secret`` / ``service_principal``). Raises :class:`ValueError`
    for any mode that does not map to an ``azure.identity`` credential (e.g. the
    non-identity ``account_key`` / ``sas`` / ``anonymous`` modes handled by the
    caller), and :class:`RuntimeError` with an install hint when azure-identity is
    absent.
    """
    m = (mode or "").strip().lower()
    if m in ("aad_client_secret", "service_principal"):
        _require_identity()
        from azure.identity import ClientSecretCredential
        return ClientSecretCredential(tenant_id, client_id, client_secret)
    if m == "managed_identity":
        _require_identity()
        from azure.identity import ManagedIdentityCredential
        return ManagedIdentityCredential(client_id=client_id or None)
    if m == "default":
        _require_identity()
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    raise ValueError(f"mode {mode!r} does not resolve to an azure.identity credential")


def proxy_credential(cfg):
    """Build the proxy's OWN outbound Entra credential — the identity already
    configured for Key Vault (issue #16).

    Reuses ``AUTH_MODE`` + ``AZURE_TENANT_ID`` + ``AZURE_CLIENT_ID`` from the
    ``config`` module and the ``AZURE_CLIENT_SECRET`` env var (never a config
    file), so outbound Azure access (Key Vault, OneLake) shares one identity.
    """
    import os
    mode = (getattr(cfg, "AUTH_MODE", "default") or "default").strip().lower()
    return get_credential(
        mode,
        tenant_id=getattr(cfg, "AZURE_TENANT_ID", "") or "",
        client_id=getattr(cfg, "AZURE_CLIENT_ID", "") or "",
        client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
    )
