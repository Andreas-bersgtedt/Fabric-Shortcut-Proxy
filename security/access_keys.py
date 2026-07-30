"""
Proxy access keys + per-key authorization (devplan/StorageProxy.md, Phase 4-A).

Moves the front door from a single global key pair to **many proxy access keys**,
each scoped to a set of buckets/prefixes, so one deployment can front several
mounts with tenant isolation. Each key is stored as an encrypted record in the
credential store (secret + ACL together); only the id index is listable.

Record shape (encrypted at rest):

    { "access_key_id": "FSPKEY…", "secret_key": "…", "label": "finance-reader",
      "allowed_buckets": ["secure-nfs", "s3vault"],
      "allowed_prefixes": { "s3vault": ["2026/"] },   # optional finer scope
      "permissions": "read",                           # v1: read-only
      "enabled": true }

Authorization model:
  * ``allowed_buckets`` may be ``["*"]`` to allow every bucket.
  * ``allowed_prefixes[bucket]`` (optional) confines a key to sub-paths of that
    bucket; absent => the whole bucket is allowed.
  * ``permissions`` is ``read`` in v1; write methods are always rejected.

The single legacy ``config.ACCESS_KEY_ID`` keeps working as an implicit wildcard
key until any ACL key is defined (see :func:`resolve_secret` /
:func:`authorize` callers), so enabling SigV4 never breaks an existing setup.
"""
from __future__ import annotations

import re
import secrets as _secrets
import threading
import time
from dataclasses import dataclass, field

from security.credential_store import CredentialStore

_READ_METHODS = frozenset({"GET", "HEAD"})
_VALID_KEY_ID = re.compile(r"^[A-Za-z0-9]{8,64}$")

# Short-TTL snapshot cache so per-request auth is a dict lookup, not a file read.
_cache_lock = threading.Lock()
_cache: dict[str, "AccessKey"] | None = None
_cache_ts = 0.0
_CACHE_TTL = 5.0


@dataclass
class AccessKey:
    """A proxy access key and its authorization scope."""
    access_key_id: str
    secret_key: str = ""
    label: str = ""
    allowed_buckets: list[str] = field(default_factory=list)
    allowed_prefixes: dict[str, list[str]] = field(default_factory=dict)
    permissions: str = "read"
    enabled: bool = True


def parse_access_key(d: dict) -> AccessKey:
    ab = d.get("allowed_buckets") or []
    if isinstance(ab, str):
        ab = [ab]
    ap_in = d.get("allowed_prefixes") or {}
    ap: dict[str, list[str]] = {}
    if isinstance(ap_in, dict):
        for b, prefixes in ap_in.items():
            if isinstance(prefixes, str):
                prefixes = [prefixes]
            ap[str(b)] = [str(p).replace("\\", "/").strip("/") for p in (prefixes or [])]
    return AccessKey(
        access_key_id=str(d.get("access_key_id") or "").strip(),
        secret_key=str(d.get("secret_key") or ""),
        label=str(d.get("label") or "").strip(),
        allowed_buckets=[str(b).strip() for b in ab if str(b).strip()],
        allowed_prefixes=ap,
        permissions=str(d.get("permissions") or "read").strip().lower(),
        enabled=bool(d.get("enabled", True)),
    )


def validate_access_key(ak: AccessKey) -> list[str]:
    problems: list[str] = []
    if not ak.access_key_id or not _VALID_KEY_ID.match(ak.access_key_id):
        problems.append("access_key_id must be 8-64 alphanumeric characters")
    if not ak.secret_key:
        problems.append("secret_key is required")
    if ak.permissions not in ("read", "read-write"):
        problems.append("permissions must be 'read' or 'read-write'")
    if not ak.allowed_buckets:
        problems.append("allowed_buckets must list at least one bucket (or '*')")
    return problems


def to_public(ak: AccessKey) -> dict:
    """Non-secret view for the UI (never includes ``secret_key``)."""
    return {
        "access_key_id": ak.access_key_id,
        "label": ak.label,
        "allowed_buckets": list(ak.allowed_buckets),
        "allowed_prefixes": {b: list(p) for b, p in ak.allowed_prefixes.items()},
        "permissions": ak.permissions,
        "enabled": ak.enabled,
    }


def generate_key() -> tuple[str, str]:
    """Return a fresh ``(access_key_id, secret_key)`` pair."""
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    body = "".join(_secrets.choice(alphabet) for _ in range(17))
    return f"FSP{body}", _secrets.token_urlsafe(32)


# ---------------------------------------------------------------------------
# Store-backed access + authorization
# ---------------------------------------------------------------------------

def _store(store: CredentialStore | None = None) -> CredentialStore:
    if store is not None:
        return store
    import config
    return CredentialStore(config.CREDENTIAL_STORE_PATH or None)


def _read_all(st: CredentialStore) -> dict[str, "AccessKey"]:
    keys: dict[str, AccessKey] = {}
    try:
        for kid in st.list_access_key_ids():
            rec = st.get_access_key(kid)
            if rec is not None:
                keys[kid] = parse_access_key(rec)
    except Exception:  # noqa: BLE001 - a broken store must not brick auth
        return {}
    return keys


def _all_keys(store: CredentialStore | None = None) -> dict[str, "AccessKey"]:
    """Return all access keys, cached for a few seconds on the default store."""
    if store is not None:
        return _read_all(store)          # explicit store (tests) => no caching
    global _cache, _cache_ts
    now = time.monotonic()
    with _cache_lock:
        if _cache is not None and (now - _cache_ts) < _CACHE_TTL:
            return _cache
    keys = _read_all(_store())
    with _cache_lock:
        _cache = keys
        _cache_ts = now
    return keys


def invalidate_cache() -> None:
    """Drop the cached key snapshot (called after a save/delete)."""
    global _cache
    with _cache_lock:
        _cache = None


def list_access_keys(*, store: CredentialStore | None = None) -> list[dict]:
    """Non-secret list of every stored access key."""
    return [to_public(ak) for ak in _all_keys(store).values()]


def get_access_key(access_key_id: str, *, store: CredentialStore | None = None) -> AccessKey | None:
    return _all_keys(store).get((access_key_id or "").strip())


def save_access_key(ak: AccessKey, *, store: CredentialStore | None = None) -> None:
    st = _store(store)
    blob = {
        "access_key_id": ak.access_key_id,
        "secret_key": ak.secret_key,
        "label": ak.label,
        "allowed_buckets": list(ak.allowed_buckets),
        "allowed_prefixes": {b: list(p) for b, p in ak.allowed_prefixes.items()},
        "permissions": ak.permissions,
        "enabled": ak.enabled,
    }
    st.set_access_key(ak.access_key_id, blob)
    invalidate_cache()


def delete_access_key(access_key_id: str, *, store: CredentialStore | None = None) -> bool:
    removed = _store(store).delete_access_key((access_key_id or "").strip())
    invalidate_cache()
    return removed


def any_keys_defined(*, store: CredentialStore | None = None) -> bool:
    """True when at least one ACL access key exists (=> ACL mode is active)."""
    return bool(_all_keys(store))


def resolve_secret(access_key_id: str, *, store: CredentialStore | None = None) -> str | None:
    """Return the SigV4 signing secret for an access key id, or ``None``.

    Looks up an enabled ACL key first, then falls back to the legacy single key
    from ``config`` so an existing deployment keeps working until ACL keys exist.
    """
    ak = get_access_key(access_key_id, store=store)
    if ak is not None:
        return ak.secret_key if ak.enabled else None
    import config
    # Legacy single key (matches even when both are empty, preserving the
    # pre-ACL behavior); only ever matches an exact configured id.
    if access_key_id == config.ACCESS_KEY_ID:
        return config.SECRET_ACCESS_KEY
    return None


def authorize(access_key_id: str, bucket: str, key: str, method: str,
              *, store: CredentialStore | None = None) -> str | None:
    """Return ``None`` if allowed, else a short denial reason.

    A legacy (non-ACL) identity is an implicit wildcard. Write methods are always
    denied in v1 (read-only facade).
    """
    if method.upper() not in _READ_METHODS:
        return "read-only: writes are not permitted"
    ak = get_access_key(access_key_id, store=store)
    if ak is None:
        # Legacy single-key identity => wildcard read access.
        return None
    if not ak.enabled:
        return "access key is disabled"
    if "*" not in ak.allowed_buckets and bucket not in ak.allowed_buckets:
        return f"key not authorized for bucket {bucket!r}"
    prefixes = ak.allowed_prefixes.get(bucket)
    if prefixes:
        norm = (key or "").replace("\\", "/").lstrip("/")
        if not any(norm.startswith(p + "/") or norm == p or p == "" for p in prefixes):
            return f"key not authorized for this prefix in bucket {bucket!r}"
    return None
