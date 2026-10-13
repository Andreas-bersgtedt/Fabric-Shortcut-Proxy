"""Operator-owned materializer pools and dataset placement policies."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any
from urllib.parse import urlsplit


class PlacementPolicyViolation(ValueError):
    code = "placement_policy_violation"

    def __init__(self, detail: str) -> None:
        super().__init__(f"{self.code}: {detail}")


class ResidencyPolicyViolation(PlacementPolicyViolation):
    code = "residency_policy_violation"


class TenantIsolationViolation(PlacementPolicyViolation):
    code = "tenant_isolation_violation"


class AgentVersionUnsupported(ValueError):
    code = "agent_version_unsupported"

    def __init__(self, detail: str) -> None:
        super().__init__(f"{self.code}: {detail}")


@dataclass(frozen=True)
class AgentVersionPolicy:
    current: str
    previous: str
    rollout_order: tuple[str, ...]
    rings: dict[str, frozenset[str]]


@dataclass(frozen=True)
class StoreProfile:
    location: str
    provider: str
    tenant_id: str = ""


@dataclass(frozen=True)
class ServingEndpoint:
    location: str
    storage_profile: str
    url: str
    tenant_id: str = ""


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
    tenant_id: str = ""
    upgrade_ring: str = ""

    def allows(self, connection_id: str, source_table: str) -> bool:
        if not connection_id or connection_id not in self.allowed_connection_ids:
            return False
        return not self.allowed_table_patterns or any(
            fnmatchcase(source_table, pattern)
            for pattern in self.allowed_table_patterns
        )


@dataclass(frozen=True)
class MaterializerPolicy:
    tenant_id: str = ""
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
        tenants: frozenset[str] | None = None,
        agent_versions: AgentVersionPolicy | None = None,
    ) -> None:
        self.pools = pools or {}
        self.connections = connections or {}
        self.tables = tables or {}
        self.stores = stores or {}
        self.serving_endpoints = serving_endpoints or {}
        self.tenants = tenants or frozenset()
        self.agent_versions = agent_versions
        self.active_storage_profile = ""
        self.policy_bundle_revision: int | None = None
        self.policy_bundle_key_id = ""
        self.policy_bundle_from_cache = False
        self.identity_pools = {
            identity: pool
            for pool in self.pools.values()
            for identity in pool.identities
        }

    @property
    def enabled(self) -> bool:
        return bool(
            self.tenants or self.pools or self.connections or self.tables
        )

    @classmethod
    def from_environment(cls) -> "PlacementConfig":
        bundle_url = os.environ.get("FSP_POLICY_BUNDLE_URL", "").strip()
        raw = os.environ.get("AGENT_PLACEMENT_CONFIG", "").strip()
        if bundle_url:
            if raw and raw != "{}":
                raise ValueError(
                    "FSP_POLICY_BUNDLE_URL and AGENT_PLACEMENT_CONFIG "
                    "cannot both define placement policy"
                )
            from enterprise.control.policy_bundle import load_policy_bundle

            timeout_text = os.environ.get(
                "FSP_POLICY_BUNDLE_TIMEOUT_SECONDS",
                "5",
            ).strip()
            try:
                timeout_seconds = float(timeout_text)
            except ValueError as exc:
                raise ValueError(
                    "FSP_POLICY_BUNDLE_TIMEOUT_SECONDS must be numeric"
                ) from exc
            cache_file = os.environ.get(
                "FSP_POLICY_BUNDLE_CACHE_FILE",
                os.path.join(
                    os.environ.get("FSP_CONFIG_DIR", "/config"),
                    "policy-bundle.json",
                ),
            ).strip()
            loaded = load_policy_bundle(
                source_url=bundle_url,
                public_key_file=os.environ.get(
                    "FSP_POLICY_BUNDLE_PUBLIC_KEY_FILE",
                    "",
                ).strip(),
                cache_file=cache_file,
                expected_key_id=os.environ.get(
                    "FSP_POLICY_BUNDLE_KEY_ID",
                    "",
                ).strip(),
                timeout_seconds=timeout_seconds,
                value_loader=cls.from_dict,
            )
            placement = loaded.value
            placement.policy_bundle_revision = loaded.revision
            placement.policy_bundle_key_id = loaded.key_id
            placement.policy_bundle_from_cache = loaded.from_cache
            return placement
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
        allowed_keys = {
            "tenants",
            "pools",
            "connections",
            "tables",
            "stores",
            "serving_endpoints",
            "agent_versions",
        }
        if set(value) - allowed_keys:
            unknown = sorted(set(value) - allowed_keys)
            raise ValueError(f"unknown placement configuration keys: {unknown}")
        raw_tenants = value.get("tenants", [])
        if not isinstance(raw_tenants, list):
            raise ValueError("placement tenants must be a list")
        tenants = frozenset(
            _tenant_id(item, required=True, scope="tenant")
            for item in raw_tenants
        )
        if len(tenants) != len(raw_tenants):
            raise ValueError("placement tenants must be unique")
        agent_versions = _agent_version_policy(
            value.get("agent_versions")
        )
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
                "tenant_id",
                "upgrade_ring",
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
                tenant_id=_tenant_id(
                    item,
                    required=bool(tenants),
                    scope=f"pool {pool_id!r}",
                    tenants=tenants,
                ),
                upgrade_ring=_upgrade_ring(
                    item,
                    agent_versions,
                    scope=f"pool {pool_id!r}",
                ),
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
            connections[key] = _policy(
                raw_policy,
                pools,
                f"connection {key!r}",
                tenants,
            )
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
            if not isinstance(raw_policy, dict):
                raise ValueError("table policy must be an object")
            if (
                parent is not None
                and parent.tenant_id
                and "tenant_id" not in raw_policy
            ):
                raw_policy = {
                    **raw_policy,
                    "tenant_id": parent.tenant_id,
                }
            if (
                parent is not None
                and parent.tenant_id
                and raw_policy.get("tenant_id") != parent.tenant_id
            ):
                raise TenantIsolationViolation(
                    "table override cannot change connection tenant"
                )
            if parent is not None and parent.residency_locations:
                raw_policy = {**connection_definitions[key[0]], **raw_policy}
                locations = _text_list(raw_policy, "residency_locations", required=False)
                if not locations or not set(locations).issubset(parent.residency_locations):
                    raise ResidencyPolicyViolation("table override cannot widen connection residency")
            tables[key] = _policy(
                raw_policy,
                pools,
                f"table {key[0]!r}::{key[1]!r}",
                tenants,
            )
        stores: dict[str, StoreProfile] = {}
        raw_stores = value.get("stores", {})
        if not isinstance(raw_stores, dict):
            raise ValueError("placement stores must be an object")
        for name, definition in raw_stores.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("store profile names must be non-empty strings")
            allowed_store_fields = {"location", "provider", "tenant_id"}
            if (
                not isinstance(definition, dict)
                or set(definition) - allowed_store_fields
            ):
                raise ValueError(
                    "store profiles require location, provider and optional tenant_id"
                )
            provider = _required_text(definition, "provider")
            if provider not in {"azure", "s3", "gcs", "local"}:
                raise ValueError(f"unsupported store provider: {provider}")
            stores[name] = StoreProfile(
                _required_text(definition, "location"),
                provider,
                _tenant_id(
                    definition,
                    required=bool(tenants),
                    scope=f"store {name!r}",
                    tenants=tenants,
                ),
            )
        endpoints: dict[str, ServingEndpoint] = {}
        raw_endpoints = value.get("serving_endpoints", {})
        if not isinstance(raw_endpoints, dict):
            raise ValueError("placement serving_endpoints must be an object")
        for name, definition in raw_endpoints.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError("serving endpoint names must be non-empty strings")
            allowed_endpoint_fields = {
                "location",
                "storage_profile",
                "url",
                "tenant_id",
            }
            if (
                not isinstance(definition, dict)
                or set(definition) - allowed_endpoint_fields
            ):
                raise ValueError(
                    "serving endpoints require location, storage_profile, "
                    "url and optional tenant_id"
                )
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
                _tenant_id(
                    definition,
                    required=bool(tenants),
                    scope=f"serving endpoint {name!r}",
                    tenants=tenants,
                ),
            )
        result = cls(
            pools,
            connections,
            tables,
            stores,
            endpoints,
            tenants,
            agent_versions,
        )
        for policy in (*connections.values(), *tables.values()):
            result.validate_tenant(policy)
            result.validate_residency(policy)
        if tenants:
            for pool in pools.values():
                for connection_id in pool.allowed_connection_ids:
                    connection = connections.get(connection_id)
                    if connection is None:
                        raise ValueError(
                            f"pool {pool.pool_id!r} references unknown "
                            f"connection {connection_id!r}"
                        )
                    if connection.tenant_id != pool.tenant_id:
                        raise TenantIsolationViolation(
                            f"pool {pool.pool_id!r} cannot access connection "
                            f"{connection_id!r} from another tenant"
                        )
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

    def validate_mtls_identities(self, identities: dict[str, str]) -> None:
        unexpected = sorted(set(identities.values()) - set(self.identity_pools))
        if unexpected:
            raise ValueError(
                f"mTLS bindings contain unmapped Agent identities: {unexpected}"
            )

    def validate_agent_version(
        self,
        identity: str,
        version: str,
    ) -> None:
        policy = self.agent_versions
        if policy is None:
            return
        pool = self.pool_for_identity(identity)
        if pool is None:
            raise AgentVersionUnsupported(
                f"Agent {identity!r} is not assigned to a version ring"
            )
        match = re.fullmatch(
            r"(\d+)\.(\d+)(?:\.\d+)?"
            r"(?:[-+][0-9A-Za-z.-]+)?",
            str(version).strip(),
        )
        if match is None:
            raise AgentVersionUnsupported(
                f"Agent {identity!r} did not report a semantic version"
            )
        release = f"{int(match.group(1))}.{int(match.group(2))}"
        if release not in {policy.current, policy.previous}:
            raise AgentVersionUnsupported(
                f"Agent {identity!r} release {release} is outside "
                f"N/N-1 ({policy.current}, {policy.previous})"
            )
        allowed = policy.rings[pool.upgrade_ring]
        if release not in allowed:
            raise AgentVersionUnsupported(
                f"Agent {identity!r} release {release} is not enabled "
                f"for ring {pool.upgrade_ring!r}"
            )

    def policy_for(self, connection_id: str, source_table: str) -> MaterializerPolicy:
        policy = self.tables.get(
            (connection_id, source_table),
            self.connections.get(connection_id, MaterializerPolicy()),
        )
        return policy

    def validate_task_tenant(
        self,
        tenant_id: str,
        policy: MaterializerPolicy,
    ) -> None:
        if not self.tenants:
            if tenant_id:
                raise TenantIsolationViolation(
                    "task declares a tenant but tenant isolation is disabled"
                )
            return
        if not tenant_id or tenant_id != policy.tenant_id:
            raise TenantIsolationViolation(
                "task tenant does not match dataset tenant"
            )

    def validate_tenant(
        self,
        policy: MaterializerPolicy,
        *,
        connection_id: str = "",
    ) -> None:
        if not self.tenants:
            return
        if policy.tenant_id not in self.tenants:
            raise TenantIsolationViolation(
                "dataset policy must belong to one configured tenant"
            )
        parent = self.connections.get(connection_id)
        if parent is not None and parent.tenant_id != policy.tenant_id:
            raise TenantIsolationViolation(
                "table override cannot change connection tenant"
            )
        profiles = {
            policy.required_storage_profile,
            policy.staging_storage_profile,
            *policy.replica_storage_profiles,
            *policy.cache_storage_profiles,
        }
        for profile_id in profiles - {""}:
            profile = self.stores.get(profile_id)
            if profile is None or profile.tenant_id != policy.tenant_id:
                raise TenantIsolationViolation(
                    f"store {profile_id!r} belongs to another tenant"
                )
        for pool_id in (
            policy.required_pool,
            *policy.fallback_pools,
        ):
            if not pool_id:
                continue
            pool = self.pools.get(pool_id)
            if pool is None or pool.tenant_id != policy.tenant_id:
                raise TenantIsolationViolation(
                    f"pool {pool_id!r} belongs to another tenant"
                )
        for endpoint_id in policy.serving_endpoints:
            endpoint = self.serving_endpoints.get(endpoint_id)
            if (
                endpoint is None
                or endpoint.tenant_id != policy.tenant_id
            ):
                raise TenantIsolationViolation(
                    f"serving endpoint {endpoint_id!r} belongs to another tenant"
                )

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

    def bind_serving_store(self, profile_id: str, provider: str, endpoint_id: str) -> None:
        profile = self.stores.get(profile_id)
        endpoint = self.serving_endpoints.get(endpoint_id)
        if (
            profile is None or profile.provider != provider
            or endpoint is None or endpoint.storage_profile != profile_id
            or endpoint.location != profile.location
        ):
            raise ResidencyPolicyViolation("serving store must match its declared provider and endpoint")
        for connection_id, policy in self.connections.items():
            self.validate_tenant(policy, connection_id=connection_id)
            self.validate_residency(policy, connection_id=connection_id)
        self.active_storage_profile = profile_id

    def validate_dispatch(self, policy: MaterializerPolicy, *, connection_id: str = "") -> None:
        self.validate_tenant(policy, connection_id=connection_id)
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


def _tenant_id(
    item: Any,
    *,
    required: bool,
    scope: str,
    tenants: frozenset[str] = frozenset(),
) -> str:
    if isinstance(item, dict):
        value = item.get("tenant_id", "")
    else:
        value = item
    if not isinstance(value, str):
        raise ValueError(f"{scope} tenant_id must be a string")
    tenant_id = value.strip()
    if required and not tenant_id:
        raise TenantIsolationViolation(
            f"{scope} must belong to one configured tenant"
        )
    if tenant_id and not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}",
        tenant_id,
    ):
        raise ValueError(f"{scope} tenant_id has an invalid format")
    if tenant_id and tenants and tenant_id not in tenants:
        raise TenantIsolationViolation(
            f"{scope} references unknown tenant {tenant_id!r}"
        )
    if isinstance(item, dict) and tenant_id and not tenants:
        raise TenantIsolationViolation(
            f"{scope} requires a top-level tenants declaration"
        )
    return tenant_id


def _release_line(value: Any, field: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"\d+\.\d+",
        value.strip(),
    ):
        raise ValueError(
            f"agent_versions {field} must use major.minor"
        )
    major, minor = value.strip().split(".", 1)
    return f"{int(major)}.{int(minor)}"


def _agent_version_policy(raw: Any) -> AgentVersionPolicy | None:
    if raw in (None, {}):
        return None
    if not isinstance(raw, dict) or set(raw) != {
        "current",
        "previous",
        "rings",
        "rollout_order",
    }:
        raise ValueError(
            "agent_versions requires current, previous, rings and rollout_order"
        )
    current = _release_line(raw["current"], "current")
    previous = _release_line(raw["previous"], "previous")
    if current == previous:
        raise ValueError(
            "agent_versions current and previous must differ"
        )
    raw_rings = raw["rings"]
    raw_order = raw["rollout_order"]
    if (
        not isinstance(raw_rings, dict)
        or not raw_rings
        or not isinstance(raw_order, list)
        or any(
            not isinstance(item, str) or not item.strip()
            for item in raw_order
        )
    ):
        raise ValueError(
            "agent_versions requires non-empty rings and rollout_order"
        )
    order = tuple(item.strip() for item in raw_order)
    if len(set(order)) != len(order) or set(order) != set(raw_rings):
        raise ValueError(
            "agent_versions rollout_order must list every ring exactly once"
        )
    supported = {current, previous}
    rings = {}
    for ring_id, releases in raw_rings.items():
        if (
            not isinstance(ring_id, str)
            or not ring_id.strip()
            or not isinstance(releases, list)
            or not releases
        ):
            raise ValueError(
                "agent_versions ring definitions must be non-empty lists"
            )
        normalized = frozenset(
            _release_line(item, f"ring {ring_id!r}")
            for item in releases
        )
        if not normalized.issubset(supported):
            raise ValueError(
                f"agent_versions ring {ring_id!r} includes "
                "a release outside N/N-1"
            )
        rings[ring_id.strip()] = normalized
    return AgentVersionPolicy(
        current=current,
        previous=previous,
        rollout_order=order,
        rings=rings,
    )


def _upgrade_ring(
    item: dict[str, Any],
    policy: AgentVersionPolicy | None,
    *,
    scope: str,
) -> str:
    ring = _optional_text(item, "upgrade_ring")
    if policy is None:
        if ring:
            raise ValueError(
                f"{scope} upgrade_ring requires agent_versions"
            )
        return ""
    if not ring or ring not in policy.rings:
        raise ValueError(
            f"{scope} must reference one configured upgrade ring"
        )
    return ring


def _policy(
    raw: Any,
    pools: dict[str, MaterializerPool],
    scope: str,
    tenants: frozenset[str],
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
        "tenant_id",
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
        tenant_id=_tenant_id(
            raw,
            required=bool(tenants),
            scope=scope,
            tenants=tenants,
        ),
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
