"""
Outbound Azure Blob / ADLS Gen2 authentication for ``azure`` mounts
(devplan/StorageProxy.md, Phase 3).

Resolves the upstream credential for a mount and builds an authenticated
``ContainerClient`` (azure-storage-blob). Aims for **maximum coverage** of the
Azure auth surface so a mount can front Blob storage, ADLS Gen2 (hierarchical
namespace), Azurite, and sovereign clouds:

  ``connection_string`` — a full storage connection string.
  ``account_key``       — the storage account's shared key.
  ``sas``               — an account/service SAS token.
  ``aad_client_secret`` — a service principal (tenant/client/secret) via azure-identity.
  ``managed_identity``  — system- or user-assigned managed identity.
  ``default``           — DefaultAzureCredential (env / MI / Azure CLI / VS Code).
  ``anonymous``         — public container (no credential).

Split of responsibilities (same as the s3 backend):
  * **Secret material** (keys, SAS, connection string, client secret) lives ONLY
    in the encrypted credential store, keyed by the mount's ``credential`` id.
  * **Non-secret connection knobs** (account name, account URL / endpoint) live on
    the :class:`Mount`.

azure-storage-blob / azure-identity are imported lazily so the core install needs
no Azure SDK; :func:`build_container_client` raises a clear install hint otherwise.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


SUPPORTED_MODES = frozenset({
    "connection_string", "account_key", "sas",
    "aad_client_secret", "managed_identity", "default", "anonymous",
})

_DEFAULT_BLOB_SUFFIX = "blob.core.windows.net"


@dataclass(frozen=True)
class AzureAuthConfig:
    """Parsed upstream Azure credential (secret material). Never logged."""
    mode: str
    connection_string: str = ""
    account_key: str = ""
    sas_token: str = ""
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""


@dataclass(frozen=True)
class AzureClientOptions:
    """Non-secret connection knobs for the upstream account."""
    account: str = ""              # storage account name
    account_url: str = ""          # explicit account URL (Azurite / sovereign clouds)
    endpoint_suffix: str = _DEFAULT_BLOB_SUFFIX


def parse_azure_auth(d: Mapping[str, Any]) -> AzureAuthConfig:
    """Parse a stored credential blob into an :class:`AzureAuthConfig`.

    When ``mode`` is omitted it is inferred from which secret field is present.
    """
    mode = str(d.get("mode") or "").strip().lower()
    if not mode:
        if d.get("connection_string"):
            mode = "connection_string"
        elif d.get("account_key"):
            mode = "account_key"
        elif d.get("sas_token"):
            mode = "sas"
        elif d.get("client_secret"):
            mode = "aad_client_secret"
        else:
            mode = ""
    return AzureAuthConfig(
        mode=mode,
        connection_string=str(d.get("connection_string") or ""),
        account_key=str(d.get("account_key") or ""),
        sas_token=str(d.get("sas_token") or "").lstrip("?"),
        tenant_id=str(d.get("tenant_id") or "").strip(),
        client_id=str(d.get("client_id") or "").strip(),
        client_secret=str(d.get("client_secret") or ""),
    )


def validate_azure_auth(auth: AzureAuthConfig) -> list[str]:
    """Return a list of problems with a parsed auth config (empty = OK)."""
    if not auth.mode:
        return ["missing auth 'mode' (and no secret field to infer one)"]
    if auth.mode not in SUPPORTED_MODES:
        return [f"unsupported auth mode {auth.mode!r} (use one of {sorted(SUPPORTED_MODES)})"]
    problems: list[str] = []
    if auth.mode == "connection_string" and not auth.connection_string:
        problems.append("connection_string auth needs 'connection_string'")
    elif auth.mode == "account_key" and not auth.account_key:
        problems.append("account_key auth needs 'account_key'")
    elif auth.mode == "sas" and not auth.sas_token:
        problems.append("sas auth needs 'sas_token'")
    elif auth.mode == "aad_client_secret":
        if not auth.tenant_id:
            problems.append("aad_client_secret auth needs 'tenant_id'")
        if not auth.client_id:
            problems.append("aad_client_secret auth needs 'client_id'")
        if not auth.client_secret:
            problems.append("aad_client_secret auth needs 'client_secret'")
    # managed_identity / default / anonymous need nothing here.
    return problems


def options_from_mount(mount) -> AzureClientOptions:
    """Derive :class:`AzureClientOptions` from a :class:`storage.mounts.Mount`."""
    return AzureClientOptions(
        account=getattr(mount, "account", "") or "",
        account_url=getattr(mount, "endpoint", "") or "",
        endpoint_suffix=getattr(mount, "endpoint_suffix", "") or _DEFAULT_BLOB_SUFFIX,
    )


def _needs_account(auth: AzureAuthConfig) -> bool:
    """Modes that build from an account URL (i.e. not a self-contained conn string)."""
    return auth.mode != "connection_string"


def account_url_for(auth: AzureAuthConfig, opts: AzureClientOptions) -> str:
    if opts.account_url:
        return opts.account_url.rstrip("/")
    if opts.account:
        return f"https://{opts.account}.{opts.endpoint_suffix}"
    return ""


def resolve_azure_auth(mount, *, store=None) -> AzureAuthConfig:
    """Resolve a mount's upstream auth from the credential store or inline mode.

    A mount with a ``credential`` id reads its (encrypted) blob from the store; a
    credential-less mount must declare an explicit ``auth`` mode (``default`` /
    ``managed_identity`` / ``anonymous``).
    """
    cid = (getattr(mount, "credential", "") or "").strip()
    if cid:
        st = store
        if st is None:
            from security.credential_store import CredentialStore
            st = CredentialStore()
        blob = st.get_secret(cid)
        if blob is None:
            raise KeyError(f"azure credential {cid!r} not found (or unreadable) in the credential store")
        return parse_azure_auth(blob)
    auth_mode = (getattr(mount, "auth", "") or "").strip().lower()
    if auth_mode:
        return parse_azure_auth({"mode": auth_mode})
    raise KeyError(
        f"mount {getattr(mount, 'bucket', '?')!r}: set a 'credential' id or an explicit "
        "'auth' mode ('default', 'managed_identity', or 'anonymous')")


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------

def _require_blob():
    try:
        from azure.storage.blob import BlobServiceClient  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise RuntimeError(
            "the 'azure' mount backend needs azure-storage-blob; install it with "
            "pip install 'fabric-shortcut-proxy[azureblob]'") from exc


def _require_identity():
    try:
        import azure.identity  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "this Azure auth mode needs azure-identity; install it with "
            "pip install 'fabric-shortcut-proxy[azureblob]'") from exc


def build_container_client(auth: AzureAuthConfig, opts: AzureClientOptions, container: str):
    """Build an authenticated Azure ``ContainerClient`` for the given auth + options."""
    _require_blob()
    from azure.storage.blob import BlobServiceClient

    if auth.mode == "connection_string":
        svc = BlobServiceClient.from_connection_string(auth.connection_string)
        return svc.get_container_client(container)

    account_url = account_url_for(auth, opts)
    if not account_url:
        raise ValueError("azure mount needs an 'account' name or an explicit account URL ('endpoint')")

    credential = _credential_for(auth)
    svc = BlobServiceClient(account_url=account_url, credential=credential)
    return svc.get_container_client(container)


def _credential_for(auth: AzureAuthConfig):
    if auth.mode == "account_key":
        return auth.account_key
    if auth.mode == "sas":
        return auth.sas_token
    if auth.mode == "anonymous":
        return None
    if auth.mode == "aad_client_secret":
        _require_identity()
        from azure.identity import ClientSecretCredential
        return ClientSecretCredential(auth.tenant_id, auth.client_id, auth.client_secret)
    if auth.mode == "managed_identity":
        _require_identity()
        from azure.identity import ManagedIdentityCredential
        return ManagedIdentityCredential(client_id=auth.client_id or None)
    if auth.mode == "default":
        _require_identity()
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    raise ValueError(f"unsupported azure auth mode: {auth.mode!r}")
