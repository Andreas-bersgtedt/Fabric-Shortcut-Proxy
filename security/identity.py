"""Local identity provider with hashed credentials and revocable sessions."""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import secrets
import threading
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from security.authorization import ROLE_PERMISSIONS, User

_ITERATIONS = 310_000
_SESSION_TTL = 8 * 60 * 60


@dataclass(frozen=True)
class Identity:
    user_id: str
    credential_hash: str
    enabled: bool = True


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    if len(password) < 12:
        raise ValueError("password must be at least 12 characters")
    salt = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _ITERATIONS)
    return "pbkdf2_sha256${}${}${}".format(
        _ITERATIONS,
        base64.urlsafe_b64encode(salt).decode(),
        base64.urlsafe_b64encode(digest).decode(),
    )


def verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, iterations, salt_b64, digest_b64 = encoded.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        salt = base64.urlsafe_b64decode(salt_b64.encode())
        expected = base64.urlsafe_b64decode(digest_b64.encode())
        actual = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), salt, int(iterations)
        )
        return hmac.compare_digest(actual, expected)
    except (TypeError, ValueError, binascii.Error):
        return False


@dataclass
class Session:
    user_id: str
    expires_at: float
    source: str = "local"


class IdentityProvider:
    """File-backed identity metadata plus in-memory, revocable sessions."""

    def __init__(self, path: str, *, ttl_seconds: int = _SESSION_TTL) -> None:
        self.path = path
        self.ttl_seconds = ttl_seconds
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()

    def _read(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except FileNotFoundError:
            return {"identities": {}}
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"unable to load identities: {exc}") from exc
        return raw if isinstance(raw, dict) else {"identities": {}}

    def _write(self, raw: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        temporary = f"{self.path}.{secrets.token_hex(8)}.tmp"
        try:
            with open(temporary, "w", encoding="utf-8") as handle:
                json.dump(raw, handle, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        finally:
            try:
                os.remove(temporary)
            except OSError:
                pass

    def create_or_replace(self, user: User, password: str) -> None:
        raw = self._read()
        identities = raw.setdefault("identities", {})
        identities[user.user_id] = {
            "credential_hash": hash_password(password),
            "roles": list(user.roles),
            "grants": [grant.to_dict() for grant in user.grants],
            "enabled": user.enabled,
        }
        self._write(raw)

    def disable(self, user_id: str) -> None:
        raw = self._read()
        identity = raw.get("identities", {}).get(user_id)
        if identity is None:
            return
        identity["enabled"] = False
        self._write(raw)
        with self._lock:
            for token, session in list(self._sessions.items()):
                if session.user_id == user_id:
                    self._sessions.pop(token, None)

    def authenticate(self, user_id: str, password: str) -> User | None:
        raw = self._read()
        record = raw.get("identities", {}).get(user_id)
        if not isinstance(record, dict) or not record.get("enabled", True):
            return None
        if not verify_password(password, str(record.get("credential_hash", ""))):
            return None
        return User.from_dict({"user_id": user_id, **record})

    def create_session(self, user: User, *, source: str = "local") -> str:
        if source not in {"local", "manager"}:
            raise ValueError("unknown identity source")
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions[token] = Session(
                user.user_id, time.time() + self.ttl_seconds, source
            )
        return token

    def resolve_session(self, token: str) -> User | None:
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return None
            if session.expires_at <= time.time():
                self._sessions.pop(token, None)
                return None
        if session.source == "manager":
            return manager_identity(session.user_id)
        return self._user(session.user_id)

    def revoke_session(self, token: str) -> None:
        with self._lock:
            self._sessions.pop(token, None)

    def revoke_user(self, user_id: str) -> None:
        with self._lock:
            for token, session in list(self._sessions.items()):
                if session.user_id == user_id:
                    self._sessions.pop(token, None)

    def _user(self, user_id: str) -> User | None:
        raw = self._read()
        record = raw.get("identities", {}).get(user_id)
        if not isinstance(record, dict) or not record.get("enabled", True):
            return None
        return User.from_dict({"user_id": user_id, **record, "credential_hash": None})


_provider: IdentityProvider | None = None
_provider_key: tuple[str, int] | None = None


def identity_provider() -> IdentityProvider:
    """Return the process-shared provider so sessions survive requests."""
    global _provider, _provider_key
    path = os.environ.get("FSP_IDENTITY_FILE", "identities.json")
    ttl = int(os.environ.get("FSP_SESSION_TTL_SECONDS", _SESSION_TTL))
    key = (path, ttl)
    if _provider is None or _provider_key != key:
        _provider = IdentityProvider(path, ttl_seconds=ttl)
        _provider_key = key
    return _provider


def authenticate_manager_identity(user_id: str, password: str) -> User | None:
    """Map configured Manager credentials to the bootstrap administrator."""
    import config

    if not config.MANAGER_AUTH_ENABLED or not config.MANAGER_AUTH_PASSWORD:
        return None
    user_ok = hmac.compare_digest(user_id, str(config.MANAGER_AUTH_USERNAME))
    password_ok = hmac.compare_digest(password, str(config.MANAGER_AUTH_PASSWORD))
    if not user_ok or not password_ok:
        return None
    return User(user_id, roles=("system_administrator",))


def manager_identity(user_id: str) -> User | None:
    """Resolve an established Manager session without retaining its password."""
    import config

    if not config.MANAGER_AUTH_ENABLED or not config.MANAGER_AUTH_PASSWORD:
        return None
    if not hmac.compare_digest(user_id, str(config.MANAGER_AUTH_USERNAME)):
        return None
    return User(user_id, roles=("system_administrator",))


def authenticate_oidc_token(token: str) -> User | None:
    """Validate an OIDC JWT and resolve its subject through the local rights directory."""
    import config

    issuer = os.environ.get("FSP_OIDC_ISSUER", config.OIDC_ISSUER).strip()
    audience = os.environ.get("FSP_OIDC_AUDIENCE", config.OIDC_AUDIENCE).strip()
    if not issuer or not audience or not token:
        return None
    try:
        import jwt
    except ImportError as exc:
        raise RuntimeError(
            "OIDC authentication requires the 'oidc' package extra"
        ) from exc

    jwks_url = os.environ.get("FSP_OIDC_JWKS_URL", config.OIDC_JWKS_URL).strip()
    if not jwks_url:
        jwks_url = f"{issuer.rstrip('/')}/.well-known/openid-configuration"
    if jwks_url.endswith("/.well-known/openid-configuration"):
        jwks_url = _oidc_jwks_uri(jwks_url)
    try:
        signing_key = _oidc_jwk_client(jwks_url).get_signing_key_from_jwt(token)
        claims: dict[str, Any] = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
            audience=audience,
            issuer=issuer,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except jwt.PyJWTError:
        return None

    claim_name = os.environ.get("FSP_OIDC_USER_CLAIM", config.OIDC_USER_CLAIM).strip() or "sub"
    user_id = str(claims.get(claim_name, "")).strip()
    if not user_id:
        return None
    from security.authorization import UserDirectory, default_user_directory_path

    try:
        user = UserDirectory.load(default_user_directory_path()).get(user_id)
    except (ValueError, PermissionError):
        return None
    return user if user.enabled and user.identity_source == "oidc" else None


def authenticate_entra_token(token: str) -> User | None:
    """Validate a delegated Entra API token and resolve its central user record."""
    import config

    tenant_id = str(config.ENTRA_TENANT_ID or "").strip()
    audience = str(config.ENTRA_API_AUDIENCE or config.ENTRA_API_CLIENT_ID or "").strip()
    required_scope = str(config.ENTRA_API_SCOPE or "").strip()
    if not config.ENTRA_ENABLED or not tenant_id or not audience or not required_scope or not token:
        return None
    try:
        import jwt
        from security.authorization import entra_subject_id
        tenant_uuid = str(uuid.UUID(tenant_id))
    except (ImportError, ValueError, AttributeError):
        return None

    issuer = f"https://login.microsoftonline.com/{tenant_uuid}/v2.0"
    jwks_url = str(config.OIDC_JWKS_URL or "").strip()
    if not jwks_url:
        jwks_url = f"https://login.microsoftonline.com/{tenant_uuid}/discovery/v2.0/keys"
    allowed_clients = {
        value.strip() for value in str(config.ENTRA_ALLOWED_CLIENT_IDS or "").split(",")
        if value.strip()
    }
    try:
        signing_key = _oidc_jwk_client(jwks_url).get_signing_key_from_jwt(token)
        claims: dict[str, Any] = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"],
            audience=audience,
            issuer=issuer,
            options={"require": ["exp", "iat", "iss", "aud", "tid", "oid"]},
        )
    except jwt.PyJWTError:
        return None

    if str(claims.get("tid", "")).lower() != tenant_uuid.lower():
        return None
    if str(claims.get("idtyp", "")).lower() == "app":
        return None
    scopes = set(str(claims.get("scp", "")).split())
    if required_scope not in scopes:
        return None
    client_id = str(claims.get("azp", "")).strip()
    if allowed_clients and client_id not in allowed_clients:
        return None
    try:
        user_id = entra_subject_id(tenant_uuid, str(claims["oid"]))
        from security.authorization import UserDirectory, default_user_directory_path
        user = UserDirectory.load(default_user_directory_path()).get(user_id)
    except (KeyError, ValueError, PermissionError):
        user = None
    if user is not None:
        return user if user.enabled and user.identity_source == "entra" else None

    try:
        from security.authorization import UserDirectory
        directory = UserDirectory.load(default_user_directory_path())
        from security.entra_directory import EntraDirectoryClient
        group_ids = EntraDirectoryClient().user_group_ids(str(claims["oid"]))
        permissions = directory.group_permissions(tenant_uuid, group_ids)
        if not permissions:
            return None
        roles = tuple(
            role for role, role_permissions in ROLE_PERMISSIONS.items()
            if role_permissions.intersection(permissions)
        )
        return User(
            user_id, roles=roles, identity_source="entra",
            tenant_id=tenant_uuid, object_id=str(claims["oid"]),
            display_name=str(claims.get("name", "")).strip() or None,
        )
    except (KeyError, ValueError, PermissionError, RuntimeError):
        return None


@lru_cache(maxsize=8)
def _oidc_jwk_client(jwks_url: str):
    from jwt import PyJWKClient

    return PyJWKClient(jwks_url)


@lru_cache(maxsize=8)
def _oidc_jwks_uri(discovery_url: str) -> str:
    """Resolve the signing-key endpoint from OIDC discovery metadata."""
    import urllib.request

    try:
        with urllib.request.urlopen(discovery_url, timeout=5) as response:
            metadata = json.load(response)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError("unable to load OIDC discovery metadata") from exc
    jwks_uri = metadata.get("jwks_uri") if isinstance(metadata, dict) else None
    if not isinstance(jwks_uri, str) or not jwks_uri.startswith("https://"):
        raise RuntimeError("OIDC discovery metadata has no secure jwks_uri")
    return jwks_uri
