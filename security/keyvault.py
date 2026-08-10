"""Azure Key Vault secret source (issue #16, Phase 1).

A **write-through** source that resolves secrets from Azure Key Vault and lets the
encrypted :class:`~security.credential_store.CredentialStore` populate itself on a
local miss. The store's on-disk cache stays authoritative, so a Key Vault / Azure /
network outage never fails the caller (owner directive: cache-first, never-fail).

``azure-keyvault-secrets`` is imported lazily behind the optional ``keyvault``
extra; the identity is built through the shared :func:`security.azure_credential.get_credential`.
A test can inject a fake ``client`` so no real Azure SDK is needed.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from security.azure_credential import get_credential

# Fixed local-key -> Key Vault secret-name convention (decision 4). An optional
# per-deployment override map points a local key at a differently named secret.
DEFAULT_SECRET_NAMES = {
    "db_url": "db-url",
    "s3_secret_access_key": "s3-secret-access-key",
    "admin_token": "admin-token",
    "manager_auth_password": "manager-auth-password",
}

_INSTALL_HINT = (
    "Key Vault integration needs azure-keyvault-secrets; install it with "
    "pip install 'fabric-shortcut-proxy[keyvault]'"
)


class KeyVaultUnavailable(RuntimeError):
    """Key Vault could not be reached, or the SDK / configuration is missing."""


@dataclass(frozen=True)
class KeyVaultConfig:
    """Non-secret Key Vault connection settings. The client secret (when a service
    principal is used) is passed separately and never persisted here."""
    vault_uri: str = ""
    auth_mode: str = "default"        # default | managed_identity | service_principal
    tenant_id: str = ""
    client_id: str = ""
    client_secret: str = ""
    overrides: dict = field(default_factory=dict)   # local_key -> kv secret name

    @property
    def enabled(self) -> bool:
        return bool((self.vault_uri or "").strip())


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


def secret_name_for(local_key: str, cfg: KeyVaultConfig | None = None) -> str:
    """Map a local credential key to its Key Vault secret name (convention + override)."""
    key = (local_key or "").strip()
    if cfg and key in (cfg.overrides or {}):
        return str(cfg.overrides[key])
    return DEFAULT_SECRET_NAMES.get(key, _slug(key) or key)


def config_from_settings(cfg) -> KeyVaultConfig:
    """Build a :class:`KeyVaultConfig` from the ``config`` module + environment.

    The service-principal client secret is read from ``AZURE_CLIENT_SECRET`` (env
    only), never from a config file.
    """
    import os
    return KeyVaultConfig(
        vault_uri=getattr(cfg, "KEYVAULT_URI", "") or "",
        auth_mode=getattr(cfg, "AUTH_MODE", "default") or "default",
        tenant_id=getattr(cfg, "AZURE_TENANT_ID", "") or "",
        client_id=getattr(cfg, "AZURE_CLIENT_ID", "") or "",
        client_secret=os.environ.get("AZURE_CLIENT_SECRET", ""),
    )


def _is_not_found(exc: Exception) -> bool:
    return getattr(exc, "status_code", None) == 404 or exc.__class__.__name__ == "ResourceNotFoundError"


def _require_sdk() -> None:
    try:
        from azure.keyvault.secrets import SecretClient  # noqa: F401
    except ImportError as exc:  # pragma: no cover - only without the extra
        raise KeyVaultUnavailable(_INSTALL_HINT) from exc


class KeyVaultSecretSource:
    """Resolve secrets from Azure Key Vault.

    The ``SecretClient`` is built lazily from the shared azure-identity credential;
    a test may inject a fake ``client`` to avoid the Azure SDK entirely.
    """

    def __init__(self, cfg: KeyVaultConfig, *, client=None) -> None:
        self._cfg = cfg
        self._client = client

    @property
    def config(self) -> KeyVaultConfig:
        return self._cfg

    def _get_client(self):
        if self._client is None:
            if not self._cfg.enabled:
                raise KeyVaultUnavailable("no Key Vault URI configured")
            _require_sdk()
            from azure.keyvault.secrets import SecretClient
            credential = get_credential(
                self._cfg.auth_mode,
                tenant_id=self._cfg.tenant_id,
                client_id=self._cfg.client_id,
                client_secret=self._cfg.client_secret,
            )
            self._client = SecretClient(vault_url=self._cfg.vault_uri, credential=credential)
        return self._client

    def get_by_name(self, name: str) -> str | None:
        """Return a Key Vault secret's string value by its exact vault name.

        ``None`` when the secret does not exist; raises :class:`KeyVaultUnavailable`
        on any connectivity / auth failure.
        """
        client = self._get_client()
        try:
            secret = client.get_secret(name)
        except Exception as exc:  # noqa: BLE001 - normalize SDK/transport errors
            if _is_not_found(exc):
                return None
            raise KeyVaultUnavailable(f"Key Vault fetch failed for {name!r}: {exc}") from exc
        return getattr(secret, "value", None)

    def get_secret_value(self, local_key: str) -> str | None:
        """Resolve a local credential key via the name convention, then fetch it."""
        return self.get_by_name(secret_name_for(local_key, self._cfg))

    def probe(self) -> tuple[bool, str]:
        """Best-effort connectivity check for ``/readyz``. Returns ``(ok, detail)``."""
        try:
            self._get_client()
            return True, "ok"
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)


def read_through_for(source: KeyVaultSecretSource, cfg: KeyVaultConfig | None = None):
    """Build a ``CredentialStore.read_through`` callable backed by Key Vault.

    Maps the store's ``(kind, key)`` miss onto a Key Vault secret name: a DB URL
    for connection ``default`` resolves to ``db-url`` (``db-url-<id>`` for a named
    connection); a mount secret id resolves via :func:`secret_name_for`.
    """
    def _rt(kind: str, key: str) -> str | None:
        if kind == "url":
            k = (key or "").strip()
            name = "db-url" if (not k or k.lower() == "default") else f"db-url-{_slug(k)}"
        else:
            name = secret_name_for(key, cfg)
        return source.get_by_name(name)

    return _rt


# ---------------------------------------------------------------------------
# Startup hydration + background refresh (Phase 2)
# ---------------------------------------------------------------------------

# Local key -> environment variable it hydrates (single-string secrets). ``db_url``
# is handled separately because it also persists into the connections bucket.
_ENV_SECRETS = {
    "s3_secret_access_key": "S3_SECRET_ACCESS_KEY",
    "admin_token": "ADMIN_TOKEN",
    "manager_auth_password": "MANAGER_AUTH_PASSWORD",
}


def _hydrate_db_url(source, store, hydrated, *, require) -> None:
    import os
    import sys
    from security.credential_store import looks_masked
    if os.environ.get("DB_URL"):
        return  # an explicit env value always wins
    try:
        val = source.get_secret_value("db_url")
    except KeyVaultUnavailable as exc:
        # Fall back to the local cache (read-through is attached later, so this
        # read is local-only and cannot re-trigger Key Vault).
        val = store.get_url("default") if store is not None else None
        if not val:
            if require:
                raise
            print(f"[keyvault] db_url unavailable and no local cache: {exc}", file=sys.stderr)
            return
    if val and not looks_masked(val):
        if store is not None and store.available:
            try:
                store.set_url("default", val)
            except Exception:  # noqa: BLE001
                pass
        os.environ["DB_URL"] = val
        hydrated.append("DB_URL")


def _hydrate_env_secret(source, store, local_key, env_var, hydrated, *, require) -> None:
    import os
    import sys
    if os.environ.get(env_var):
        return  # an explicit env value always wins
    cache_id = f"env:{local_key}"
    try:
        val = source.get_secret_value(local_key)
    except KeyVaultUnavailable as exc:
        cached = store.get_secret(cache_id) if store is not None else None
        val = cached.get("value") if isinstance(cached, dict) else None
        if not val:
            if require:
                raise
            print(f"[keyvault] {local_key} unavailable and no local cache: {exc}", file=sys.stderr)
            return
    if val:
        if store is not None and store.available:
            try:
                store.set_secret(cache_id, {"value": val})
            except Exception:  # noqa: BLE001
                pass
        os.environ[env_var] = val
        hydrated.append(env_var)


def hydrate_from_keyvault(store=None, *, source=None) -> list[str]:
    """Resolve Key Vault-backed secrets into the environment + local cache.

    Runs BEFORE config binds env values (Manager import / ``main.py`` top). It is
    best-effort and never-fail unless ``require_keyvault`` is set (owner directive):
    a Key Vault outage falls back to the local encrypted cache. Also attaches the
    store's cache-first read-through so mount credentials resolve lazily on demand.
    Returns the hydrated environment-variable names.
    """
    import sys
    import system_config as sc
    if source is None:
        cfg = config_from_settings(sc)
        if not cfg.enabled:
            return []
        source = KeyVaultSecretSource(cfg)
    else:
        cfg = source.config
        if not cfg.enabled:
            return []
    require = bool(getattr(sc, "REQUIRE_KEYVAULT", False))
    if store is None:
        from security.credential_store import CredentialStore
        store = CredentialStore()
    hydrated: list[str] = []
    try:
        _hydrate_db_url(source, store, hydrated, require=require)
        for local_key, env_var in _ENV_SECRETS.items():
            _hydrate_env_secret(source, store, local_key, env_var, hydrated, require=require)
    except KeyVaultUnavailable:
        raise  # require_keyvault + cold start with no cache => fail-fast (AC6)
    except Exception as exc:  # noqa: BLE001 - never fail on an unexpected error
        print(f"[keyvault] hydration skipped: {exc}", file=sys.stderr)
    # On-demand cache-first read-through for mount credentials (resolved per mount).
    try:
        if store is not None and store.available:
            store.read_through = read_through_for(source, cfg)
    except Exception:  # noqa: BLE001
        pass
    return hydrated


def refresh_secrets_once(source, store) -> list[str]:
    """Re-pull the documented secrets from Key Vault, writing through to cache + env.

    Used by the background refresh loop. A Key Vault error for any single secret is
    skipped so the last-known-good cache is retained. Returns refreshed env names.
    """
    import os
    from security.credential_store import looks_masked
    refreshed: list[str] = []
    try:
        val = source.get_secret_value("db_url")
        if val and not looks_masked(val):
            if store is not None and store.available:
                store.set_url("default", val)
            os.environ["DB_URL"] = val
            refreshed.append("DB_URL")
    except KeyVaultUnavailable:
        pass
    for local_key, env_var in _ENV_SECRETS.items():
        try:
            val = source.get_secret_value(local_key)
        except KeyVaultUnavailable:
            continue
        if val:
            if store is not None and store.available:
                try:
                    store.set_secret(f"env:{local_key}", {"value": val})
                except Exception:  # noqa: BLE001
                    pass
            os.environ[env_var] = val
            refreshed.append(env_var)
    return refreshed
