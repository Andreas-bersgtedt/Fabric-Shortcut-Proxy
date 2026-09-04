"""Central, secret-free tokenization policy definitions.

The registry is intentionally runtime-light in this first slice. It gives the
planner, Arrow tokenizer, and future policy API one stable contract while the
existing inline ColumnTransform format remains compatible.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import ColumnTransform


class TokenizationPolicyError(ValueError):
    """Raised when a tokenization policy is invalid or unavailable."""


@dataclass(frozen=True)
class TokenizationSelection:
    """Safe table-side choice that contains no algorithm or secret settings."""

    action: str
    policy_id: str | None = None

    def __post_init__(self) -> None:
        if self.action not in {"keep", "remove", "durable_token", "random_token"}:
            raise TokenizationPolicyError(
                "action must be 'keep', 'remove', 'durable_token', or 'random_token'"
            )
        if self.action in {"keep", "remove"} and self.policy_id is not None:
            raise TokenizationPolicyError(
                f"{self.action} selection must not reference a policy"
            )
        if self.action in {"durable_token", "random_token"}:
            if not self.policy_id or any(ch.isspace() for ch in self.policy_id):
                raise TokenizationPolicyError(
                    f"{self.action} selection requires a policy_id without whitespace"
                )

    def to_dict(self) -> dict:
        result = {"action": self.action}
        if self.policy_id is not None:
            result["policy_id"] = self.policy_id
        return result


@dataclass(frozen=True)
class AlgorithmSpec:
    """Approved algorithm metadata used by policy validation and Arrow."""

    name: str
    min_digest_size: int
    max_digest_size: int
    native_dialects: tuple[str, ...] = ()


_ALGORITHMS = {
    "sha256": AlgorithmSpec("sha256", 32, 32),
    "blake2b": AlgorithmSpec("blake2b", 1, 64),
}


def algorithm_specs() -> tuple[AlgorithmSpec, ...]:
    """Return approved algorithms without exposing any key material."""
    return tuple(_ALGORITHMS[name] for name in sorted(_ALGORITHMS))


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
        spec = _ALGORITHMS.get(self.algorithm)
        if spec is None:
            raise TokenizationPolicyError(
                f"unsupported tokenization algorithm: {self.algorithm!r}"
            )
        if self.normalization not in {"none", "trim", "trim_lower"}:
            raise TokenizationPolicyError(
                f"unsupported tokenization normalization: {self.normalization!r}"
            )
        if not spec.min_digest_size <= self.digest_size <= spec.max_digest_size:
            raise TokenizationPolicyError(
                f"{self.algorithm} digest_size must be between "
                f"{spec.min_digest_size} and {spec.max_digest_size} bytes"
            )
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

    def to_legacy_transform(self) -> "ColumnTransform":
        """Adapt a central policy for current planner/tokenizer consumers."""
        if self.kind == "durable_token":
            kind = "deterministic_hash"
        else:
            kind = "random_token"
        from config import ColumnTransform

        return ColumnTransform(
            kind=kind,
            key_ref=self.key_ref,
            domain=self.domain,
            normalization=self.normalization,
        )

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

    @classmethod
    def from_dict(cls, raw: dict) -> "TokenizationPolicy":
        """Parse one persisted policy without accepting secret values."""
        if not isinstance(raw, dict):
            raise TokenizationPolicyError("tokenization policy must be an object")
        forbidden = {"key", "secret", "secret_value", "token_key"}
        if forbidden.intersection(raw):
            raise TokenizationPolicyError("tokenization policy must contain key_ref, not key material")
        try:
            return cls(
                policy_id=str(raw["policy_id"]).strip(),
                kind=str(raw["kind"]).strip().lower(),
                algorithm=str(raw.get("algorithm", "sha256")).strip().lower(),
                key_ref=(str(raw["key_ref"]).strip() if raw.get("key_ref") else None),
                domain=(str(raw["domain"]) if raw.get("domain") is not None else None),
                normalization=str(raw.get("normalization", "none")).strip().lower(),
                digest_size=int(raw.get("digest_size", 32)),
                framing_version=int(raw.get("framing_version", 1)),
                enabled=bool(raw.get("enabled", True)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TokenizationPolicyError(f"invalid tokenization policy: {exc}") from exc

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
        payload = f"{key}|{domain}|{normalized}".encode("utf-8")
        if self.algorithm == "sha256":
            digest = hashlib.sha256(payload).digest()
        else:
            digest = hashlib.blake2b(payload, digest_size=self.digest_size).digest()
        return digest.hex().upper()


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

    def replace(self, policy: TokenizationPolicy) -> None:
        """Insert or replace a policy for controlled administrative updates."""
        self._policies[policy.policy_id] = policy

    def has(self, policy_id: str) -> bool:
        """Return whether a policy ID is registered, including disabled policies."""
        return policy_id in self._policies

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

    @classmethod
    def from_dict(cls, raw: dict) -> "TokenizationPolicyRegistry":
        """Load a registry from ``{"policies": [...]}`` JSON data."""
        if not isinstance(raw, dict) or not isinstance(raw.get("policies"), list):
            raise TokenizationPolicyError("tokenization config must contain a policies list")
        return cls([TokenizationPolicy.from_dict(item) for item in raw["policies"]])

    def to_dict(self) -> dict:
        """Return the secret-free persisted registry shape."""
        return {"policies": [self._policies[key].to_public() for key in sorted(self._policies)]}

    def resolve_selection(self, selection: TokenizationSelection) -> TokenizationPolicy:
        """Resolve a table selection and enforce action/policy-kind agreement."""
        if selection.action in {"keep", "remove"}:
            raise TokenizationPolicyError(
                f"{selection.action} selection does not resolve a token policy"
            )
        policy = self.get(selection.policy_id)
        if policy.kind != selection.action:
            raise TokenizationPolicyError(
                f"policy {selection.policy_id!r} is {policy.kind!r}, "
                f"not {selection.action!r}"
            )
        return policy

    def selection(self, policy_id: str) -> TokenizationSelection:
        """Return the safe table-side selector for an enabled policy."""
        policy = self.get(policy_id)
        return TokenizationSelection(policy.kind, policy_id=policy.policy_id)


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
        algorithm="sha256",
        key_ref=transform.key_ref,
        domain=transform.domain or default_domain,
        normalization=transform.normalization,
    )


def selection_from_transform(transform: "ColumnTransform") -> TokenizationSelection:
    """Convert the current inline transform to a table-side selector."""
    if transform.kind == "deterministic_hash":
        return TokenizationSelection("durable_token", policy_id="legacy-inline")
    if transform.kind == "random_token":
        return TokenizationSelection("random_token", policy_id="legacy-inline")
    raise TokenizationPolicyError(f"unsupported legacy transform: {transform.kind!r}")


def policy_fingerprint(policy: TokenizationPolicy) -> str:
    """Return a stable, secret-free identity for cache and snapshot inputs."""
    payload = json.dumps(policy.to_public(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def load_registry(path: str) -> TokenizationPolicyRegistry:
    """Load a policy registry from disk, failing closed on malformed JSON."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except FileNotFoundError:
        return TokenizationPolicyRegistry()
    except (OSError, json.JSONDecodeError) as exc:
        raise TokenizationPolicyError(f"unable to load tokenization policies: {exc}") from exc
    return TokenizationPolicyRegistry.from_dict(raw)


def save_registry(path: str, registry: TokenizationPolicyRegistry) -> None:
    """Atomically save a secret-free policy registry document."""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".tokenization-", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(registry.to_dict(), fh, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temporary, path)
    except OSError as exc:
        raise TokenizationPolicyError(f"unable to save tokenization policies: {exc}") from exc
    finally:
        if os.path.exists(temporary):
            try:
                os.remove(temporary)
            except OSError:
                pass


def default_registry_path() -> str:
    """Return the operator-selected policy file without importing application config."""
    return os.environ.get("TOKENIZATION_POLICY_FILE", "config.tokenization.json")


def load_default_registry() -> TokenizationPolicyRegistry:
    """Load the configured central policy registry."""
    return load_registry(default_registry_path())
