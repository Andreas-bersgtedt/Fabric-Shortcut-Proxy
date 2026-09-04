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

import json
import re
import sys
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


def _kv_name(kind: str, key: str, cfg: KeyVaultConfig | None = None) -> str:
    """Map a store ``(kind, key)`` pair to its Key Vault secret name.

    Shared by read-through, write-back, and delete so the three never drift: a DB
    URL for connection ``default`` is ``db-url`` (``db-url-<id>`` for a named one),
    a proxy access key is ``access-key-<id>``, and any other secret resolves via
    :func:`secret_name_for`.
    """
    k = (key or "").strip()
    if kind == "url":
        return "db-url" if (not k or k.lower() == "default") else f"db-url-{_slug(k)}"
    if kind == "access_key":
        return f"access-key-{_slug(k)}"
    return secret_name_for(key, cfg)


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


def sdk_available() -> bool:
    """True if azure-keyvault-secrets is importable (the optional 'keyvault' extra)."""
    import importlib.util
    try:
        return importlib.util.find_spec("azure.keyvault.secrets") is not None
    except ModuleNotFoundError:
        return False


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
            if not sdk_available():
                raise KeyVaultUnavailable(_INSTALL_HINT)
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

    def set_by_name(self, name: str, value: str) -> None:
        """Create/update a Key Vault secret by its exact vault name (write-back).

        Raises :class:`KeyVaultUnavailable` on any failure so the caller can decide
        whether to swallow it (write-back is fail-soft; local persistence still wins).
        """
        client = self._get_client()
        try:
            client.set_secret(name, value)
        except Exception as exc:  # noqa: BLE001 - normalize SDK/transport errors
            raise KeyVaultUnavailable(f"Key Vault write failed for {name!r}: {exc}") from exc

    def set_secret_value(self, local_key: str, value: str) -> None:
        """Write a local credential key to Key Vault via the name convention."""
        self.set_by_name(secret_name_for(local_key, self._cfg), value)

    def delete_by_name(self, name: str) -> None:
        """Soft-delete a Key Vault secret by its exact vault name (write-back).

        A *not found* is a no-op; any other failure raises :class:`KeyVaultUnavailable`
        so the caller can swallow it (delete write-through is fail-soft).
        """
        client = self._get_client()
        try:
            poller = client.begin_delete_secret(name)
        except Exception as exc:  # noqa: BLE001 - normalize SDK/transport errors
            if _is_not_found(exc):
                return
            raise KeyVaultUnavailable(f"Key Vault delete failed for {name!r}: {exc}") from exc
        waiter = getattr(poller, "wait", None)
        if callable(waiter):
            try:
                waiter()
            except Exception:  # noqa: BLE001 - deletion accepted; awaiting completion is best-effort
                pass

    def probe(self) -> tuple[bool, str]:
        """Live connectivity + auth check (used by the 'Test Key Vault' button).

        Attempts to read the conventional ``db-url`` secret; a *not found* still
        proves connectivity + read permission. Any transport/auth error fails.
        Returns ``(ok, detail)``.
        """
        if not sdk_available():
            return False, _INSTALL_HINT
        try:
            self.get_by_name(secret_name_for("db_url", self._cfg))
            return True, "ok"
        except KeyVaultUnavailable as exc:
            return False, str(exc)
        except Exception as exc:  # noqa: BLE001
            return False, str(exc)


def read_through_for(source: KeyVaultSecretSource, cfg: KeyVaultConfig | None = None):
    """Build a ``CredentialStore.read_through`` callable backed by Key Vault.

    Maps the store's ``(kind, key)`` miss onto a Key Vault secret name via
    :func:`_kv_name` (shared with write-back / delete so the names never drift).
    """
    def _rt(kind: str, key: str) -> str | None:
        return source.get_by_name(_kv_name(kind, key, cfg))

    return _rt


def write_through_for(source: KeyVaultSecretSource, cfg: KeyVaultConfig | None = None):
    """Build a ``CredentialStore.write_through`` callable backed by Key Vault (Phase 4).

    Persists a saved credential to Key Vault under the shared naming convention
    (:func:`_kv_name`): a DB URL for connection ``default`` writes to ``db-url``
    (``db-url-<id>`` for a named connection), a mount secret writes its JSON blob,
    and a proxy access key writes its full record — **secret + ACL scope** (allowed
    buckets/prefixes, permissions, enabled) — as JSON under ``access-key-<id>``.
    Raises on failure — the caller (the store) swallows it so write-back is fail-soft.
    """
    def _wt(kind: str, key: str, value) -> None:
        name = _kv_name(kind, key, cfg)
        payload = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
        source.set_by_name(name, payload)

    return _wt


def delete_through_for(source: KeyVaultSecretSource, cfg: KeyVaultConfig | None = None):
    """Build a ``CredentialStore.delete_through`` callable backed by Key Vault (Phase 4).

    Soft-deletes the Key Vault secret for a removed credential under the same
    :func:`_kv_name` convention. Raises on failure — the store swallows it so a
    delete write-through never blocks the local removal.
    """
    def _dt(kind: str, key: str) -> None:
        source.delete_by_name(_kv_name(kind, key, cfg))

    return _dt


def attach_write_back(store) -> bool:
    """Attach Key Vault write-back + delete-through to a store when
    ``keyvault_write_back`` is on.

    No-op (returns False) unless write-back is enabled, a vault is configured, and
    the local store has an encryption backend. Only the Manager should enable this
    (it needs ``Key Vault Secrets Officer``); agents stay read-only.
    """
    import system_config as sc
    if not getattr(sc, "KEYVAULT_WRITE_BACK", False):
        return False
    cfg = config_from_settings(sc)
    if not cfg.enabled or store is None or not getattr(store, "available", False):
        return False
    source = KeyVaultSecretSource(cfg)
    store.write_through = write_through_for(source, cfg)
    store.delete_through = delete_through_for(source, cfg)
    return True


# Config-file secret SETTINGS (persisted to config files, not the credential store)
# that also mirror into Key Vault. Maps the config setting key -> the local key used
# by the DEFAULT_SECRET_NAMES convention.
CONFIG_SECRET_SETTINGS = {
    "secret_access_key": "s3_secret_access_key",
    "admin_token": "admin_token",
    "manager_auth_password": "manager_auth_password",
}


def write_back_config_secrets(changed: dict) -> list[str]:
    """Mirror changed config-file secret settings to Key Vault (Phase 4, fail-soft).

    The S3 secret key, admin token, and Manager password persist to config files
    rather than the credential store, so they bypass the store's write-through. When
    ``keyvault_write_back`` is on this pushes them to Key Vault too: a non-empty value
    is written, an explicitly empty value soft-deletes (clears) the secret, and a
    redacted ``***`` placeholder from the UI is ignored (never overwrites the vault).
    Returns the setting keys mirrored; never raises (the config save already won).
    """
    import system_config as sc
    if not getattr(sc, "KEYVAULT_WRITE_BACK", False):
        return []
    cfg = config_from_settings(sc)
    if not cfg.enabled or not changed:
        return []
    source = KeyVaultSecretSource(cfg)
    done: list[str] = []
    for skey, local_key in CONFIG_SECRET_SETTINGS.items():
        if skey not in changed:
            continue
        sval = "" if changed[skey] is None else str(changed[skey])
        if "***" in sval:
            continue  # redacted placeholder from the UI — never overwrite KV with a mask
        name = secret_name_for(local_key, cfg)
        try:
            if sval.strip() == "":
                source.delete_by_name(name)
            else:
                source.set_by_name(name, sval)
            done.append(skey)
        except Exception as exc:  # noqa: BLE001 - fail-soft; the config save already succeeded
            print(f"[keyvault] config-secret write-back failed for {skey!r}: {exc}", file=sys.stderr)
    return done


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

    Used by the background refresh loop. A not-found secret is skipped; a Key Vault
    connectivity/auth error aborts the pass (the last-known-good cache is retained)
    and marks the advisory status ``degraded``. Never raises. Returns refreshed env
    names and records the outcome for :func:`status_snapshot`.
    """
    import os
    from security.credential_store import looks_masked
    refreshed: list[str] = []
    error: Exception | None = None
    try:
        val = source.get_secret_value("db_url")
        if val and not looks_masked(val):
            if store is not None and store.available:
                store.set_url("default", val)
            os.environ["DB_URL"] = val
            refreshed.append("DB_URL")
        for local_key, env_var in _ENV_SECRETS.items():
            val = source.get_secret_value(local_key)
            if val:
                if store is not None and store.available:
                    try:
                        store.set_secret(f"env:{local_key}", {"value": val})
                    except Exception:  # noqa: BLE001
                        pass
                os.environ[env_var] = val
                refreshed.append(env_var)
    except KeyVaultUnavailable as exc:
        error = exc
    if error is not None:
        note_refresh_error(error)
    else:
        note_refresh_ok(refreshed)
    return refreshed


# ---------------------------------------------------------------------------
# Advisory status for /readyz + the monitor dashboard (non-secret, no live call)
# ---------------------------------------------------------------------------

_STATUS: dict = {"ok": None, "at": "", "secrets": [], "error": ""}


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def note_refresh_ok(names) -> None:
    _STATUS.update(ok=True, at=_now_iso(), secrets=list(names or []), error="")


def note_refresh_error(err) -> None:
    _STATUS.update(ok=False, at=_now_iso(), error=str(err)[:200])


def _vault_host(uri: str) -> str:
    from urllib.parse import urlparse
    try:
        return urlparse(uri or "").hostname or ""
    except Exception:  # noqa: BLE001
        return ""


def status_snapshot(cfg: KeyVaultConfig | None = None, store=None) -> dict:
    """Non-secret Key Vault / Entra ID status for health + monitoring.

    Reports the configured vault (host only), auth mode, refresh/TTL knobs, whether
    a local cache exists, and the last refresh outcome. It never contains secret
    values and never performs a live network call (it reflects the last refresh),
    so it is safe to call on every ``/readyz`` hit.
    """
    import system_config as sc
    if cfg is None:
        cfg = config_from_settings(sc)
    out = {
        "enabled": bool(cfg.enabled),
        "vault": _vault_host(cfg.vault_uri),
        "auth_mode": cfg.auth_mode,
        "require_keyvault": bool(getattr(sc, "REQUIRE_KEYVAULT", False)),
        "refresh_seconds": int(getattr(sc, "KEYVAULT_REFRESH_SECONDS", 0)),
        "cache_ttl": int(getattr(sc, "KEYVAULT_CACHE_TTL", 0)),
        "last_refresh": dict(_STATUS),
    }
    cached = False
    try:
        if store is None:
            from security.credential_store import CredentialStore
            store = CredentialStore()
        if store.available:
            cached = bool(store.list_ids() or store.list_secret_ids())
    except Exception:  # noqa: BLE001
        cached = False
    out["cached"] = cached
    if not out["enabled"]:
        out["status"] = "disabled"
    elif _STATUS.get("ok") is False:
        out["status"] = "degraded"
    else:
        out["status"] = "ok"
    return out
