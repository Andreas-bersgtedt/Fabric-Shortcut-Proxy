"""Operator-owned materializer pools and dataset placement policies."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any
from urllib.parse import urlsplit


class ResidencyPolicyViolation(ValueError):
    code = "residency_policy_violation"

    def __init__(self, detail: str) -> None:
        super().__init__(f"{self.code}: {detail}")


@dataclass(frozen=True)
class StoreProfile:
    location: str
    provider: str


@dataclass(frozen=True)
class ServingEndpoint:
    location: str
    storage_profile: str
    url: str


@dataclass(frozen=True)
class MaterializerPool:
    pool_id: str
    identities: frozenset[str]
    location: str
    storage_profile: str
    allowed_connection_ids: frozenset[str]
    allowed_table_patterns: tuple[str, ...]
    max_concurrency: int | None
    heartbeat_ms: int | None = None
    heartbeat_miss_limit: int | None = None
    claim_lease_seconds: int | None = None

    def allows(self, connection_id: str, source_table: str) -> bool:
        if not connection_id or connection_id not in self.allowed_connection_ids:
            return False
        return not self.allowed_table_patterns or any(
            fnmatchcase(source_table, pattern)
            for pattern in self.allowed_table_patterns
        )


@dataclass(frozen=True)
class MaterializerPolicy:
    required_pool: str = ""
    required_location: str = ""
    required_storage_profile: str = ""
    fallback_pools: tuple[str, ...] = ()
    max_concurrency: int | None = None
    residency_locations: tuple[str, ...] = ()
    staging_storage_profile: str = ""
    serving_endpoints: tuple[str, ...] = ()
    replica_storage_profiles: tuple[str, ...] = ()
    cache_storage_profiles: tuple[str, ...] = ()
    freshness_target_ms: int | None = None


class PlacementConfig:
    """Validated, immutable placement settings supplied by the operator."""

    def __init__(
        self,
        pools: dict[str, MaterializerPool] | None = None,
        connections: dict[str, MaterializerPolicy] | None = None,
        tables: dict[tuple[str, str], MaterializerPolicy] | None = None,
        stores: dict[str, StoreProfile] | None = None,
        serving_endpoints: dict[str, ServingEndpoint] | None = None,
    ) -> None:
        self.pools = pools or {}
        self.connections = connections or {}
        self.tables = tables or {}
        self.stores = stores or {}
        self.serving_endpoints = serving_endpoints or {}
        self.active_storage_profile = ""
        self.identity_pools = {
            identity: pool
            for pool in self.pools.values()
            for identity in pool.identities
        }

    @property
    def enabled(self) -> bool:
        return bool(self.pools or self.connections or self.tables)

    @classmethod
    def from_environment(cls) -> "PlacementConfig":
        raw = os.environ.get("AGENT_PLACEMENT_CONFIG", "").strip()
        if not raw:
            return cls()
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("AGENT_PLACEMENT_CONFIG must be valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("AGENT_PLACEMENT_CONFIG must be a JSON object")
        return cls.from_dict(value)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "PlacementConfig":
        allowed_keys = {"pools", "connections", "tables", "stores", "serving_endpoints"}
        if set(value) - allowed_keys:
            unknown = sorted(set(value) - allowed_keys)
            raise ValueError(f"unknown placement configuration keys: {unknown}")
        pools: dict[str, MaterializerPool] = {}
        identities: set[str] = set()
        raw_pools = value.get("pools", [])
        if not isinstance(raw_pools, list):
            raise ValueError("placement pools must be a list")
        for item in raw_pools:
            if not isinstance(item, dict):
                raise ValueError("each placement pool must be an object")
            allowed_fields = {
                "pool_id",
                "identities",
                "location",
                "storage_profile",
                "allowed_connection_ids",
                "allowed_table_patterns",
                "max_concurrency",
                "heartbeat_ms",
                "heartbeat_miss_limit",
                "claim_lease_seconds",
            }
            unknown_fields = sorted(set(item) - allowed_fields)
            if unknown_fields:
                raise ValueError(
                    f"pool definition has unknown fields: {unknown_fields}"
                )
            pool_id = _required_text(item, "pool_id")
            if pool_id in pools:
                raise ValueError(f"duplicate materializer pool {pool_id!r}")
            pool_identities = _text_list(item, "identities")
            if not pool_identities:
                raise ValueError(f"pool {pool_id!r} must define identities")
            duplicates = identities.intersection(pool_identities)
            if duplicates:
                raise ValueError(
                    f"agent identities are assigned to multiple pools: "
                    f"{sorted(duplicates)}"
                )
            identities.update(pool_identities)
            allowed_connections = _text_list(item, "allowed_connection_ids")
            table_patterns = _text_list(item, "allowed_table_patterns", required=False)
            max_concurrency = _positive_limit(item.get("max_concurrency"), pool_id)
            pools[pool_id] = MaterializerPool(
                pool_id=pool_id,
                identities=frozenset(pool_identities),
                location=_optional_text(item, "location"),
                storage_profile=_optional_text(item, "storage_profile"),
                allowed_connection_ids=frozenset(allowed_connections),
                allowed_table_patterns=tuple(table_patterns),
                max_concurrency=max_concurrency,
                heartbeat_ms=_positive_limit(item.get("heartbeat_ms"), pool_id, "heartbeat_ms"),
                heartbeat_miss_limit=_positive_limit(item.get("heartbeat_miss_limit"), pool_id, "heartbeat_miss_limit"),
                claim_lease_seconds=_positive_limit(item.get("claim_lease_seconds"), pool_id, "claim_lease_seconds"),
            )

        connections: dict[str, MaterializerPolicy] = {}
        connection_definitions: dict[str, Any] = {}
        raw_connections = value.get("connections", {})
        if not isinstance(raw_connections, dict):
            raise ValueError("placement connections must be an object")
        for connection_id, raw_policy in raw_connections.items():
            key = str(connection_id).strip()
            if not key:
                raise ValueError("connection policy keys must be non-empty")
            connections[key] = _policy(raw_policy, pools, f"connection {key!r}")
            connection_definitions[key] = raw_policy

        tables: dict[tuple[str, str], MaterializerPolicy] = {}
        raw_tables = value.get("tables", {})
        if not isinstance(raw_tables, dict):
            raise ValueError("placement tables must be an object")
        for table_id, raw_policy in raw_tables.items():
            connection_id, separator, source_table = str(table_id).partition("::")
            if not separator or not connection_id.strip() or not source_table.strip():
                raise ValueError(
                    "table policy keys must use 'connection_id::source_table'"
                )
            key = (connection_id.strip(), source_table.strip())
            parent = connections.get(key[0])
            if parent is not None and parent.residency_locations:
                if not isinstance(raw_policy, dict):
                    raise ValueError("table policy must be an object")
                raw_policy = {**connection_definitions[key[0]], **raw_policy}
                locations = _text_list(raw_policy, "residency_locations", required=False)
                if not locations or not set(locations).issubset(parent.residency_locations):
                    raise ResidencyPolicyViolation("table override cannot widen connection residency")
            tables[key] = _policy(
                raw_policy,
                pools,
                f"table {key[0]!r}::{key[1]!r}",
            )
        stores: dict[str, StoreProfile] = {}
        raw_stores = value.get("stores", {})
        if not isinstance(raw_stores, dict):
            raise ValueError("placement stores must be an object")
        for name, definition in raw_stores.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("store profile names must be non-empty strings")
            if not isinstance(definition, dict) or set(definition) != {"location", "provider"}:
                raise ValueError("store profiles require only location and provider")
            provider = _required_text(definition, "provider")
            if provider not in {"azure", "s3", "gcs", "local"}:
                raise ValueError(f"unsupported store provider: {provider}")
            stores[name] = StoreProfile(_required_text(definition, "location"), provider)
        endpoints: dict[str, ServingEndpoint] = {}
        raw_endpoints = value.get("serving_endpoints", {})
        if not isinstance(raw_endpoints, dict):
            raise ValueError("placement serving_endpoints must be an object")
        for name, definition in raw_endpoints.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("serving endpoint names must be non-empty strings")
            if not isinstance(definition, dict) or set(definition) != {
                "location", "storage_profile", "url"
            }:
                raise ValueError("serving endpoints require location, storage_profile and url")
            url = _required_text(definition, "url")
            parsed = urlsplit(url)
            if (
                parsed.scheme != "https" or not parsed.hostname
                or parsed.username or parsed.password or parsed.query or parsed.fragment
            ):
                raise ValueError("serving endpoint URL must use HTTPS without credentials")
            endpoints[name] = ServingEndpoint(
                _required_text(definition, "location"),
                _required_text(definition, "storage_profile"),
                url,
            )
        result = cls(pools, connections, tables, stores, endpoints)
        for policy in (*connections.values(), *tables.values()):
            result.validate_residency(policy)
        return result

    def pool_for_identity(self, identity: str) -> MaterializerPool | None:
        return self.identity_pools.get(identity)

    def validate_identity_tokens(self, raw_tokens: str, shared_tokens: tuple[str, ...]) -> None:
        try:
            tokens = json.loads(raw_tokens or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("AGENT_IDENTITY_TOKENS must be valid JSON") from exc
        if not isinstance(tokens, dict) or any(
            not isinstance(identity, str)
            or not identity
            or not isinstance(token, str)
            or not token.isascii()
            or len(token) < 32
            or any(ord(char) < 0x21 or ord(char) > 0x7E for char in token)
            for identity, token in tokens.items()
        ):
            raise ValueError(
                "AGENT_IDENTITY_TOKENS must map agent IDs to tokens of at least 32 bytes"
            )
        missing = sorted(set(self.identity_pools) - set(tokens)) if self.enabled else []
        if missing:
            raise ValueError(
                f"AGENT_IDENTITY_TOKENS is missing pool identities: {missing}"
            )
        unexpected = sorted(set(tokens) - set(self.identity_pools)) if self.enabled else []
        if unexpected:
            raise ValueError(
                f"AGENT_IDENTITY_TOKENS contains unmapped identities: {unexpected}"
            )
        token_pools: dict[str, set[str]] = {}
        for identity, token in tokens.items():
            pool = self.identity_pools.get(identity)
            if pool is not None:
                token_pools.setdefault(token, set()).add(pool.pool_id)
        if any(len(pool_ids) > 1 for pool_ids in token_pools.values()):
            raise ValueError(
                "identity tokens must be unique across materializer pools"
            )
        if set(tokens.values()).intersection(token for token in shared_tokens if token):
            raise ValueError(
                "identity tokens must differ from AGENT_TOKEN and AGENT_TOKEN_PREVIOUS"
            )

    def validate_entra_identities(self, identities: dict[str, dict[str, str]]) -> None:
        missing = sorted(set(self.identity_pools) - set(identities))
        if missing:
            raise ValueError(f"Entra bindings are missing pool identities: {missing}")
        for claim in ("client_id", "principal_id"):
            identity_pools: dict[str, set[str]] = {}
            for agent_id, pool in self.identity_pools.items():
                identity_pools.setdefault(identities[agent_id][claim], set()).add(pool.pool_id)
            if any(len(pools) > 1 for pools in identity_pools.values()):
                raise ValueError(f"Entra {claim} bindings must be unique across materializer pools")

    def policy_for(self, connection_id: str, source_table: str) -> MaterializerPolicy:
        policy = self.tables.get(
            (connection_id, source_table),
            self.connections.get(connection_id, MaterializerPolicy()),
        )
        return policy

    def validate_residency(self, policy: MaterializerPolicy, *, connection_id: str = "") -> None:
        parent = self.connections.get(connection_id)
        if parent is not None and parent.residency_locations:
            if not policy.residency_locations or not set(policy.residency_locations).issubset(parent.residency_locations):
                raise ResidencyPolicyViolation("table override cannot widen connection residency")
        if not policy.residency_locations:
            if policy.staging_storage_profile or policy.serving_endpoints or policy.replica_storage_profiles or policy.cache_storage_profiles:
                raise ResidencyPolicyViolation("federated chain requires residency_locations")
            return
        allowed = set(policy.residency_locations)
        if not policy.required_pool or not policy.required_storage_profile or not policy.serving_endpoints:
            raise ResidencyPolicyViolation("constrained policy requires pool, published store and serving endpoint")
        if policy.required_location and policy.required_location not in allowed:
            raise ResidencyPolicyViolation("required location is outside the boundary")
        profiles = {policy.required_storage_profile}
        profiles.add(policy.staging_storage_profile or policy.required_storage_profile)
        profiles.update(policy.replica_storage_profiles)
        profiles.update(policy.cache_storage_profiles)
        for pool_id in (policy.required_pool, *policy.fallback_pools):
            pool = self.pools.get(pool_id)
            if pool is None or pool.location not in allowed or not pool.storage_profile:
                raise ResidencyPolicyViolation(f"pool {pool_id!r} is missing or outside the boundary")
            profiles.add(pool.storage_profile)
        for endpoint_id in policy.serving_endpoints:
            endpoint = self.serving_endpoints.get(endpoint_id)
            if endpoint is None or endpoint.location not in allowed:
                raise ResidencyPolicyViolation(f"serving endpoint {endpoint_id!r} is missing or outside the boundary")
            store = self.stores.get(endpoint.storage_profile)
            if store is None or store.location != endpoint.location:
                raise ResidencyPolicyViolation("serving endpoint must read a store in its own location")
            if endpoint.storage_profile not in {
                policy.required_storage_profile, *policy.replica_storage_profiles
            }:
                raise ResidencyPolicyViolation("serving endpoint must read the published store or a configured replica")
            profiles.add(endpoint.storage_profile)
        for profile_id in profiles:
            profile = self.stores.get(profile_id)
            if profile is None or profile.location not in allowed:
                raise ResidencyPolicyViolation(f"store {profile_id!r} is missing or outside the boundary")

    def bind_runtime_store(self, profile_id: str, provider: str) -> None:
        self.active_storage_profile = profile_id
        if profile_id:
            profile = self.stores.get(profile_id)
            if profile is None or profile.provider != provider:
                raise ResidencyPolicyViolation("active artifact store does not match its declared provider")
        for policy in (*self.connections.values(), *self.tables.values()):
            self.validate_dispatch(policy)

    def validate_dispatch(self, policy: MaterializerPolicy, *, connection_id: str = "") -> None:
        self.validate_residency(policy, connection_id=connection_id)
        if not policy.residency_locations:
            return
        if not self.active_storage_profile:
            raise ResidencyPolicyViolation("FSP_ARTIFACT_STORE_PROFILE is required for constrained dispatch")
        if (
            policy.required_storage_profile != self.active_storage_profile
            or (policy.staging_storage_profile or policy.required_storage_profile) != self.active_storage_profile
            or any(
                self.pools[pool].storage_profile != self.active_storage_profile
                for pool in (policy.required_pool, *policy.fallback_pools)
            )
        ):
            raise ResidencyPolicyViolation("current work queue supports one staging/published store; cross-store dispatch is not enabled")


def _required_text(item: dict[str, Any], key: str) -> str:
    value = _optional_text(item, key)
    if not value:
        raise ValueError(f"placement field {key!r} must be non-empty")
    return value


def _optional_text(item: dict[str, Any], key: str) -> str:
    value = item.get(key, "")
    if not isinstance(value, str):
        raise ValueError(f"placement field {key!r} must be a string")
    return value.strip()


def _text_list(
    item: dict[str, Any], key: str, *, required: bool = True
) -> list[str]:
    value = item.get(key, [] if not required else None)
    if not isinstance(value, list) or any(
        not isinstance(entry, str) or not entry.strip() for entry in value
    ):
        raise ValueError(f"placement field {key!r} must be a list of strings")
    return list(dict.fromkeys(entry.strip() for entry in value))


def _positive_limit(value: Any, scope: str, field: str = "max_concurrency") -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{scope} {field} must be a positive integer")
    return value


def _policy(
    raw: Any, pools: dict[str, MaterializerPool], scope: str
) -> MaterializerPolicy:
    if not isinstance(raw, dict):
        raise ValueError(f"{scope} policy must be an object")
    allowed = {
        "required_pool",
        "required_location",
        "required_storage_profile",
        "fallback_pools",
        "max_concurrency",
        "residency_locations",
        "staging_storage_profile",
        "serving_endpoints",
        "replica_storage_profiles",
        "cache_storage_profiles",
        "freshness_target_ms",
    }
    if set(raw) - allowed:
        unknown = sorted(set(raw) - allowed)
        raise ValueError(f"{scope} policy has unknown keys: {unknown}")
    required_pool = _optional_text(raw, "required_pool")
    required_location = _optional_text(raw, "required_location")
    required_storage_profile = _optional_text(raw, "required_storage_profile")
    fallback_pools = _text_list(raw, "fallback_pools", required=False)
    if fallback_pools and not required_pool:
        raise ValueError(f"{scope} fallback_pools requires required_pool")
    named_pools = ([required_pool] if required_pool else []) + fallback_pools
    unknown_pools = [pool for pool in named_pools if pool not in pools]
    if unknown_pools:
        raise ValueError(f"{scope} references unknown pools: {unknown_pools}")
    if len(set(named_pools)) != len(named_pools):
        raise ValueError(f"{scope} policy repeats a pool")
    return MaterializerPolicy(
        required_pool=required_pool,
        required_location=required_location,
        required_storage_profile=required_storage_profile,
        fallback_pools=tuple(fallback_pools),
        max_concurrency=_positive_limit(raw.get("max_concurrency"), scope),
        residency_locations=tuple(_text_list(raw, "residency_locations", required=False)),
        staging_storage_profile=_optional_text(raw, "staging_storage_profile"),
        serving_endpoints=tuple(_text_list(raw, "serving_endpoints", required=False)),
        replica_storage_profiles=tuple(_text_list(raw, "replica_storage_profiles", required=False)),
        cache_storage_profiles=tuple(_text_list(raw, "cache_storage_profiles", required=False)),
        freshness_target_ms=_positive_limit(raw.get("freshness_target_ms"), scope, "freshness_target_ms"),
    )
