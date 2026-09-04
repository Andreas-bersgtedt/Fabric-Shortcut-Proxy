"""Central, secret-free tokenization policy definitions.

The registry is intentionally runtime-light in this first slice. It gives the
planner, Arrow tokenizer, and future policy API one stable contract while the
existing inline ColumnTransform format remains compatible.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import ColumnTransform


class TokenizationPolicyError(ValueError):
    """Raised when a tokenization policy is invalid or unavailable."""


@dataclass(frozen=True)
class TokenizationPolicy:
    """Secret-free description of one approved tokenization policy."""

    policy_id: str
    kind: str
    algorithm: str = "sha256"
    key_ref: str | None = None
    domain: str | None = None
    normalization: str = "none"
    digest_size: int = 32
    framing_version: int = 1
    enabled: bool = True

    def __post_init__(self) -> None:
        policy_id = self.policy_id.strip()
        if not policy_id or any(ch.isspace() for ch in policy_id):
            raise TokenizationPolicyError("policy_id must be non-empty and contain no whitespace")
        if self.kind not in {"durable_token", "random_token"}:
            raise TokenizationPolicyError(
                "kind must be 'durable_token' or 'random_token'"
            )
        if self.algorithm not in {"sha256"}:
            raise TokenizationPolicyError(
                f"unsupported tokenization algorithm: {self.algorithm!r}"
            )
        if self.normalization not in {"none", "trim", "trim_lower"}:
            raise TokenizationPolicyError(
                f"unsupported tokenization normalization: {self.normalization!r}"
            )
        if self.digest_size != 32:
            raise TokenizationPolicyError("sha256 policies must use a 32-byte digest")
        if self.framing_version != 1:
            raise TokenizationPolicyError("unsupported tokenization framing version")
        if self.kind == "durable_token" and not self.key_ref:
            raise TokenizationPolicyError("durable_token requires a key_ref")
        if self.kind == "random_token" and self.key_ref:
            raise TokenizationPolicyError("random_token must not have a key_ref")

    @property
    def transform_kind(self) -> str:
        """Return the legacy transform kind used by current execution paths."""
        return "deterministic_hash" if self.kind == "durable_token" else "random_token"

    def to_public(self) -> dict:
        """Return policy metadata without secret material."""
        return {
            "policy_id": self.policy_id,
            "kind": self.kind,
            "algorithm": self.algorithm,
            "key_ref": self.key_ref,
            "domain": self.domain,
            "normalization": self.normalization,
            "digest_size": self.digest_size,
            "framing_version": self.framing_version,
            "enabled": self.enabled,
        }

    def deterministic_token(self, value: str) -> str:
        """Build the canonical durable token without exposing the key."""
        if self.kind != "durable_token":
            raise TokenizationPolicyError("only durable_token policies produce deterministic tokens")
        import config

        normalized = value
        if self.normalization in {"trim", "trim_lower"}:
            normalized = normalized.strip()
        if self.normalization == "trim_lower":
            normalized = normalized.lower()
        key = config.resolve_tokenization_key(self.key_ref)
        domain = self.domain or self.policy_id
        return hashlib.sha256(
            f"{key}|{domain}|{normalized}".encode("utf-8")
        ).hexdigest().upper()


class TokenizationPolicyRegistry:
    """In-memory registry for validated policies."""

    def __init__(self, policies: list[TokenizationPolicy] | None = None) -> None:
        self._policies: dict[str, TokenizationPolicy] = {}
        for policy in policies or []:
            self.register(policy)

    def register(self, policy: TokenizationPolicy) -> None:
        if policy.policy_id in self._policies:
            raise TokenizationPolicyError(
                f"tokenization policy already exists: {policy.policy_id!r}"
            )
        self._policies[policy.policy_id] = policy

    def get(self, policy_id: str) -> TokenizationPolicy:
        try:
            policy = self._policies[policy_id]
        except KeyError:
            raise TokenizationPolicyError(
                f"unknown tokenization policy: {policy_id!r}"
            ) from None
        if not policy.enabled:
            raise TokenizationPolicyError(
                f"tokenization policy is disabled: {policy_id!r}"
            )
        return policy

    def list_public(self) -> list[dict]:
        return [self._policies[key].to_public() for key in sorted(self._policies)]


def legacy_policy(
    transform: "ColumnTransform",
    *,
    policy_id: str = "legacy-inline",
    default_domain: str | None = None,
) -> TokenizationPolicy:
    """Normalize the current inline transform into a central policy object."""
    if transform.kind == "deterministic_hash":
        kind = "durable_token"
    elif transform.kind == "random_token":
        kind = "random_token"
    else:
        raise TokenizationPolicyError(f"unsupported legacy transform: {transform.kind!r}")
    return TokenizationPolicy(
        policy_id=policy_id,
        kind=kind,
        key_ref=transform.key_ref,
        domain=transform.domain or default_domain,
        normalization=transform.normalization,
    )


def policy_fingerprint(policy: TokenizationPolicy) -> str:
    """Return a stable, secret-free identity for cache and snapshot inputs."""
    payload = json.dumps(policy.to_public(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
