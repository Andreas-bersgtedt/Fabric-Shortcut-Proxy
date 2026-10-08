"""Operator-owned materializer pools and dataset placement policies."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any


@dataclass(frozen=True)
class MaterializerPool:
    pool_id: str
    identities: frozenset[str]
    location: str
    storage_profile: str
    allowed_connection_ids: frozenset[str]
    allowed_table_patterns: tuple[str, ...]
    max_concurrency: int | None

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


class PlacementConfig:
    """Validated, immutable placement settings supplied by the operator."""

    def __init__(
        self,
        pools: dict[str, MaterializerPool] | None = None,
        connections: dict[str, MaterializerPolicy] | None = None,
        tables: dict[tuple[str, str], MaterializerPolicy] | None = None,
    ) -> None:
        self.pools = pools or {}
        self.connections = connections or {}
        self.tables = tables or {}
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
        if set(value) - {"pools", "connections", "tables"}:
            unknown = sorted(set(value) - {"pools", "connections", "tables"})
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
            )

        connections: dict[str, MaterializerPolicy] = {}
        raw_connections = value.get("connections", {})
        if not isinstance(raw_connections, dict):
            raise ValueError("placement connections must be an object")
        for connection_id, raw_policy in raw_connections.items():
            key = str(connection_id).strip()
            if not key:
                raise ValueError("connection policy keys must be non-empty")
            connections[key] = _policy(raw_policy, pools, f"connection {key!r}")

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
            tables[key] = _policy(
                raw_policy,
                pools,
                f"table {key[0]!r}::{key[1]!r}",
            )
        return cls(pools, connections, tables)

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

    def policy_for(self, connection_id: str, source_table: str) -> MaterializerPolicy:
        return self.tables.get(
            (connection_id, source_table),
            self.connections.get(connection_id, MaterializerPolicy()),
        )


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


def _positive_limit(value: Any, scope: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{scope} max_concurrency must be a positive integer")
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
    )
