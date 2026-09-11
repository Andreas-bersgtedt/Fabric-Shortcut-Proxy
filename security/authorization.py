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
import uuid
from typing import Mapping


PERMISSIONS = frozenset({
    "monitor.read",
    "troubleshoot.read",
    "config.read",
    "config.write",
    "storage.mount.inspect",
    "tokenization.assign",
    "tokenization.policy.read",
    "tokenization.policy.admin",
    "tokenization.reidentify",
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
        "storage.mount.inspect", "tokenization.assign",
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
    "auditor": frozenset({"tokenization.reidentify"}),
    "system_administrator": frozenset(PERMISSIONS - {"tokenization.reidentify"}),
}


class AuthorizationError(PermissionError):
    """Raised when a user cannot perform a function in the requested context."""


def entra_subject_id(tenant_id: str, object_id: str) -> str:
    """Return the stable application key for an Entra directory object."""
    try:
        tenant = str(uuid.UUID(str(tenant_id).strip()))
        object_value = str(uuid.UUID(str(object_id).strip()))
    except (ValueError, AttributeError):
        raise ValueError("Entra tenant_id and object_id must be UUIDs") from None
    return f"entra:{tenant}:{object_value}"


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
    identity_source: str = "local"
    tenant_id: str | None = None
    object_id: str | None = None
    display_name: str | None = None

    def __post_init__(self) -> None:
        if not self.user_id.strip() or any(ch.isspace() for ch in self.user_id):
            raise ValueError("user_id must be non-empty and contain no whitespace")
        unknown = set(self.roles) - set(ROLE_PERMISSIONS)
        if unknown:
            raise ValueError(f"unknown roles: {sorted(unknown)}")
        if self.identity_source not in {"local", "oidc", "entra"}:
            raise ValueError("identity_source must be 'local', 'oidc', or 'entra'")
        if self.identity_source == "entra":
            if not self.tenant_id or not self.object_id:
                raise ValueError("Entra users require tenant_id and object_id")
            if self.user_id != entra_subject_id(self.tenant_id, self.object_id):
                raise ValueError("Entra user_id must match tenant_id and object_id")
        elif self.tenant_id is not None or self.object_id is not None:
            raise ValueError("tenant_id and object_id are only valid for Entra users")

    def to_public(self) -> dict:
        """Return persisted identity metadata without credentials or tokens."""
        public = {
            "user_id": self.user_id,
            "roles": list(self.roles),
            "grants": [grant.to_dict() for grant in self.grants],
            "enabled": self.enabled,
            "identity_source": self.identity_source,
        }
        if self.identity_source == "entra":
            public.update({
                "tenant_id": self.tenant_id,
                "object_id": self.object_id,
                "display_name": self.display_name,
            })
        return public

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
            identity_source=str(raw.get("identity_source", "local")).strip().lower(),
            tenant_id=(str(raw["tenant_id"]).strip() if raw.get("tenant_id") else None),
            object_id=(str(raw["object_id"]).strip() if raw.get("object_id") else None),
            display_name=(str(raw["display_name"]).strip() if raw.get("display_name") else None),
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

    def __init__(self, users: list[User] | None = None, groups: list[dict] | None = None) -> None:
        self._users: dict[str, User] = {}
        self._groups: dict[str, dict] = {}
        for user in users or []:
            self.replace(user)
        for group in groups or []:
            self.replace_group(group)

    def replace(self, user: User) -> None:
        self._users[user.user_id] = user

    def replace_group(self, group: Mapping) -> None:
        group_id = str(group.get("group_id", "")).strip().lower()
        if not group_id:
            raise ValueError("group_id must be non-empty")
        self._groups[group_id] = {
            "group_id": group_id,
            "tenant_id": str(group.get("tenant_id", "")).strip(),
            "display_name": str(group.get("display_name", "")).strip(),
            "roles": [str(role).strip() for role in (group.get("roles") or [])],
            "enabled": bool(group.get("enabled", True)),
        }
        unknown = set(self._groups[group_id]["roles"]) - set(ROLE_PERMISSIONS)
        if unknown:
            raise ValueError(f"unknown roles: {sorted(unknown)}")

    def groups_for(self, tenant_id: str, group_ids: set[str]) -> list[dict]:
        tenant = str(tenant_id).strip().lower()
        return [
            group for group_id, group in self._groups.items()
            if group_id in {value.lower() for value in group_ids}
            and group["enabled"] and group["tenant_id"].lower() == tenant
        ]

    def authorize_groups(self, tenant_id: str, group_ids: set[str], permission: str) -> bool:
        return any(
            permission in {
                granted for role in group["roles"] for granted in ROLE_PERMISSIONS[role]
            }
            for group in self.groups_for(tenant_id, group_ids)
        )

    def group_permissions(self, tenant_id: str, group_ids: set[str]) -> frozenset[str]:
        permissions: set[str] = set()
        for group in self.groups_for(tenant_id, group_ids):
            for role in group["roles"]:
                permissions.update(ROLE_PERMISSIONS[role])
        return frozenset(permissions)

    def list_groups_public(self) -> list[dict]:
        return [dict(self._groups[key]) for key in sorted(self._groups)]

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
            user.user_id, roles=user.roles, grants=user.grants, enabled=False,
            identity_source=user.identity_source, tenant_id=user.tenant_id,
            object_id=user.object_id, display_name=user.display_name,
        )

    def get(self, user_id: str) -> User:
        try:
            return self._users[user_id]
        except KeyError:
            raise AuthorizationError("user not found") from None

    def list_public(self) -> list[dict]:
        return [self._users[key].to_public() for key in sorted(self._users)]

    def to_dict(self) -> dict:
        return {"users": self.list_public(), "groups": self.list_groups_public()}

    @classmethod
    def from_dict(cls, raw: Mapping) -> "UserDirectory":
        if not isinstance(raw, Mapping) or not isinstance(raw.get("users"), list):
            raise ValueError("user directory must contain a users list")
        directory = cls([User.from_dict(item) for item in raw["users"]])
        for group in raw.get("groups", []):
            directory.replace_group(group)
        return directory

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


def authenticate_request(
    supplied_token: str,
    session_token: str = "",
    bearer_token: str = "",
) -> User | None:
    """Authenticate a request through the current transitional provider.

    A future identity/session provider should replace this adapter without
    changing permission checks in route handlers.
    """
    user = authenticate_admin_token(supplied_token)
    if user is not None:
        return user
    if session_token:
        from security.identity import identity_provider
        user = identity_provider().resolve_session(session_token)
        if user is not None:
            return user
    if bearer_token:
        from security.identity import authenticate_entra_token, authenticate_oidc_token
        user = authenticate_entra_token(bearer_token)
        if user is not None:
            return user
        return authenticate_oidc_token(bearer_token)
    return None


def bearer_token(authorization_header: str) -> str:
    """Return a bearer credential only for a well-formed Authorization header."""
    scheme, separator, token = authorization_header.partition(" ")
    if separator and scheme.lower() == "bearer":
        return token.strip()
    return ""


def require_request_permission(
    supplied_token: str,
    permission: str,
    context: Mapping[str, str] | None = None,
    session_token: str = "",
    bearer_credential: str = "",
) -> AuthorizationDecision:
    """Authenticate and require one named function permission."""
    user = authenticate_request(supplied_token, session_token, bearer_credential)
    if user is None:
        raise AuthorizationError("authentication required")
    return require(user, permission, context)
