"""Function- and context-based authorization primitives.

This module is intentionally independent of FastAPI and identity providers. It
provides the policy decision core that route adapters can reuse while the
transitional ADMIN_TOKEN provider remains in place.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import tempfile
from typing import Mapping


PERMISSIONS = frozenset({
    "monitor.read",
    "troubleshoot.read",
    "config.read",
    "config.write",
    "tokenization.assign",
    "tokenization.policy.read",
    "tokenization.policy.admin",
    "security.metadata.read",
    "security.credentials.admin",
    "users.admin",
    "system.admin",
})

ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "viewer": frozenset({"monitor.read"}),
    "monitor_troubleshooter": frozenset({"monitor.read", "troubleshoot.read"}),
    "config_operator": frozenset({
        "monitor.read", "troubleshoot.read", "config.read", "config.write",
        "tokenization.assign",
    }),
    "tokenization_administrator": frozenset({
        "monitor.read", "troubleshoot.read", "tokenization.assign",
        "tokenization.policy.read", "tokenization.policy.admin",
    }),
    "security_administrator": frozenset({
        "monitor.read", "troubleshoot.read", "security.metadata.read",
        "security.credentials.admin",
    }),
    "user_administrator": frozenset({"monitor.read", "troubleshoot.read", "users.admin"}),
    "system_administrator": frozenset(PERMISSIONS),
}


class AuthorizationError(PermissionError):
    """Raised when a user cannot perform a function in the requested context."""


@dataclass(frozen=True)
class PermissionGrant:
    permission: str
    context: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.permission not in PERMISSIONS:
            raise ValueError(f"unknown permission: {self.permission!r}")
        if any(not str(key).strip() or not str(value).strip() for key, value in self.context.items()):
            raise ValueError("permission context keys and values must be non-empty")

    def to_dict(self) -> dict:
        return {"permission": self.permission, "context": dict(self.context)}

    @classmethod
    def from_dict(cls, raw: Mapping) -> "PermissionGrant":
        if not isinstance(raw, Mapping):
            raise ValueError("permission grant must be an object")
        return cls(str(raw.get("permission", "")), dict(raw.get("context") or {}))


@dataclass(frozen=True)
class User:
    user_id: str
    roles: tuple[str, ...] = ()
    grants: tuple[PermissionGrant, ...] = ()
    enabled: bool = True

    def __post_init__(self) -> None:
        if not self.user_id.strip() or any(ch.isspace() for ch in self.user_id):
            raise ValueError("user_id must be non-empty and contain no whitespace")
        unknown = set(self.roles) - set(ROLE_PERMISSIONS)
        if unknown:
            raise ValueError(f"unknown roles: {sorted(unknown)}")

    def to_public(self) -> dict:
        """Return persisted identity metadata without credentials or tokens."""
        return {
            "user_id": self.user_id,
            "roles": list(self.roles),
            "grants": [grant.to_dict() for grant in self.grants],
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, raw: Mapping) -> "User":
        if not isinstance(raw, Mapping):
            raise ValueError("user must be an object")
        forbidden = {"password", "password_hash", "secret", "token", "access_token"}
        if forbidden.intersection(raw):
            raise ValueError("user records must not contain credentials or tokens")
        return cls(
            user_id=str(raw.get("user_id", "")).strip(),
            roles=tuple(str(role).strip() for role in (raw.get("roles") or [])),
            grants=tuple(PermissionGrant.from_dict(grant) for grant in (raw.get("grants") or [])),
            enabled=bool(raw.get("enabled", True)),
        )

    def permissions(self) -> frozenset[str]:
        result: set[str] = set()
        for role in self.roles:
            result.update(ROLE_PERMISSIONS[role])
        result.update(grant.permission for grant in self.grants if not grant.context)
        return frozenset(result)

    def can(self, permission: str, context: Mapping[str, str] | None = None) -> bool:
        if not self.enabled or permission not in PERMISSIONS:
            return False
        if permission in self.permissions():
            return True
        requested = context or {}
        return any(
            grant.permission == permission
            and all(requested.get(key) == value or value == "*" for key, value in grant.context.items())
            for grant in self.grants
        )


@dataclass(frozen=True)
class AuthorizationDecision:
    user_id: str
    permission: str
    context: Mapping[str, str]
    allowed: bool
    reason: str


def authorize(
    user: User,
    permission: str,
    context: Mapping[str, str] | None = None,
) -> AuthorizationDecision:
    """Evaluate one function permission against a user and resource context."""
    requested = dict(context or {})
    if permission not in PERMISSIONS:
        return AuthorizationDecision(user.user_id, permission, requested, False, "unknown permission")
    if not user.enabled:
        return AuthorizationDecision(user.user_id, permission, requested, False, "user is disabled")
    allowed = user.can(permission, requested)
    return AuthorizationDecision(
        user.user_id, permission, requested, allowed,
        "allowed" if allowed else "permission denied",
    )


def require(
    user: User,
    permission: str,
    context: Mapping[str, str] | None = None,
) -> AuthorizationDecision:
    """Evaluate and raise a generic authorization error when denied."""
    decision = authorize(user, permission, context)
    if not decision.allowed:
        raise AuthorizationError("permission denied")
    return decision


class UserDirectory:
    """Secret-free user/role directory for the future identity-provider adapter."""

    def __init__(self, users: list[User] | None = None) -> None:
        self._users: dict[str, User] = {}
        for user in users or []:
            self.replace(user)

    def replace(self, user: User) -> None:
        self._users[user.user_id] = user

    def disable(self, user_id: str) -> None:
        """Disable a user without allowing the last enabled admin to be removed."""
        user = self.get(user_id)
        if not user.enabled:
            return
        enabled_admins = sum(
            other.enabled and "system_administrator" in other.roles
            for other in self._users.values()
        )
        if "system_administrator" in user.roles and enabled_admins <= 1:
            raise AuthorizationError("cannot disable the last enabled system administrator")
        self._users[user_id] = User(
            user.user_id, roles=user.roles, grants=user.grants, enabled=False
        )

    def get(self, user_id: str) -> User:
        try:
            return self._users[user_id]
        except KeyError:
            raise AuthorizationError("user not found") from None

    def list_public(self) -> list[dict]:
        return [self._users[key].to_public() for key in sorted(self._users)]

    def to_dict(self) -> dict:
        return {"users": self.list_public()}

    @classmethod
    def from_dict(cls, raw: Mapping) -> "UserDirectory":
        if not isinstance(raw, Mapping) or not isinstance(raw.get("users"), list):
            raise ValueError("user directory must contain a users list")
        return cls([User.from_dict(item) for item in raw["users"]])

    @classmethod
    def load(cls, path: str) -> "UserDirectory":
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return cls.from_dict(json.load(handle))
        except FileNotFoundError:
            return cls()
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"unable to load users: {exc}") from exc

    def save(self, path: str) -> None:
        directory = os.path.dirname(os.path.abspath(path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".users-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.to_dict(), handle, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except OSError as exc:
            raise ValueError(f"unable to save users: {exc}") from exc
        finally:
            if os.path.exists(temporary):
                try:
                    os.remove(temporary)
                except OSError:
                    pass


def default_user_directory_path() -> str:
    """Return the operator-selected user directory path."""
    return os.environ.get("FSP_USER_DIRECTORY_FILE", "users.json")


def authenticate_admin_token(supplied_token: str) -> User | None:
    """Map the transitional admin token to a non-persisted system admin user."""
    import hmac

    expected = os.environ.get("ADMIN_TOKEN", "")
    if expected and supplied_token and hmac.compare_digest(supplied_token, expected):
        return User("admin-token", roles=("system_administrator",))
    return None


def authenticate_request(supplied_token: str) -> User | None:
    """Authenticate a request through the current transitional provider.

    A future identity/session provider should replace this adapter without
    changing permission checks in route handlers.
    """
    return authenticate_admin_token(supplied_token)


def require_request_permission(
    supplied_token: str,
    permission: str,
    context: Mapping[str, str] | None = None,
) -> AuthorizationDecision:
    """Authenticate and require one named function permission."""
    user = authenticate_request(supplied_token)
    if user is None:
        raise AuthorizationError("authentication required")
    return require(user, permission, context)
