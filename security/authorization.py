"""Function- and context-based authorization primitives.

This module is intentionally independent of FastAPI and identity providers. It
provides the policy decision core that route adapters can reuse while the
transitional ADMIN_TOKEN provider remains in place.
"""
from __future__ import annotations

from dataclasses import dataclass, field
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
