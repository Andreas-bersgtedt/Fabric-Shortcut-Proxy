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
from dataclasses import dataclass

from security.authorization import User

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

    def create_session(self, user: User) -> str:
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._sessions[token] = Session(user.user_id, time.time() + self.ttl_seconds)
        return token

    def resolve_session(self, token: str) -> User | None:
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return None
            if session.expires_at <= time.time():
                self._sessions.pop(token, None)
                return None
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
