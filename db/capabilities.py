"""Per-flavor capability matrix and fallback policy.

This module centralizes what each SQL flavor can do in this proxy so callers can
make explicit decisions (instead of implicit try/except behavior):
- connection prerequisites
- reflection coverage
- query execution mode (async-native vs sync-threadpool fallback)
- split-planning suitability
"""
from __future__ import annotations

from dataclasses import dataclass


TOKENIZATION_NATIVE = "native"
TOKENIZATION_ARROW = "arrow"
TOKENIZATION_NONE = "none"
TOKENIZATION_FALLBACKS = (TOKENIZATION_NONE, TOKENIZATION_ARROW)


@dataclass(frozen=True)
class FlavorCapabilities:
    flavor: str
    async_driver: bool
    supports_streaming_query: bool
    supports_view_listing: bool
    supports_primary_key_reflection: bool
    supports_range_key_bounds: bool
    supports_modulo_split: bool
    supports_fast_row_estimate: bool
    supports_deterministic_tokenization: bool = False
    supports_random_tokenization: bool = False
    supports_ntile: bool = True
    supports_stats_histogram: bool = False
    required_connection_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        execution_mode = "async-native" if self.async_driver else "sync-threadpool-fallback"
        return {
            "flavor": self.flavor,
            "execution_mode": execution_mode,
            "async_driver": self.async_driver,
            "supports_streaming_query": self.supports_streaming_query,
            "supports_view_listing": self.supports_view_listing,
            "supports_primary_key_reflection": self.supports_primary_key_reflection,
            "supports_range_key_bounds": self.supports_range_key_bounds,
            "supports_modulo_split": self.supports_modulo_split,
            "supports_fast_row_estimate": self.supports_fast_row_estimate,
            "supports_deterministic_tokenization": self.supports_deterministic_tokenization,
            "supports_random_tokenization": self.supports_random_tokenization,
            "supports_ntile": self.supports_ntile,
            "supports_stats_histogram": self.supports_stats_histogram,
            "required_connection_fields": list(self.required_connection_fields),
        }

    def tokenization_backend(self, kind: str, fallback: str = TOKENIZATION_NONE) -> str:
        """Return the selected backend without silently enabling fallback."""
        native = (
            self.supports_deterministic_tokenization
            if kind == "deterministic_hash"
            else self.supports_random_tokenization
        )
        if native:
            return TOKENIZATION_NATIVE
        if fallback == TOKENIZATION_ARROW:
            return TOKENIZATION_ARROW
        return TOKENIZATION_NONE

    def tokenization_warning(self, kind: str, fallback: str = TOKENIZATION_NONE) -> str | None:
        """Describe the operational cost when Arrow replaces native pushdown."""
        if self.tokenization_backend(kind, fallback) != TOKENIZATION_ARROW:
            return None
        return (
            f"{self.flavor} {kind} uses Arrow fallback: plaintext source values "
            "cross into the proxy, increasing proxy CPU/memory and source-to-proxy "
            "data transfer while reducing safe throughput."
        )


_CAPABILITIES: dict[str, FlavorCapabilities] = {
    "sqlite": FlavorCapabilities(
        flavor="sqlite",
        async_driver=True,
        supports_streaming_query=True,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
    ),
    "postgresql": FlavorCapabilities(
        flavor="postgresql",
        async_driver=True,
        supports_streaming_query=True,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=True,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
        supports_stats_histogram=True,
    ),
    "mssql": FlavorCapabilities(
        flavor="mssql",
        async_driver=True,
        supports_streaming_query=True,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=True,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
        supports_stats_histogram=True,
    ),
    "oracle": FlavorCapabilities(
        flavor="oracle",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=True,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
    ),
    "databricks": FlavorCapabilities(
        flavor="databricks",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=True,
        supports_primary_key_reflection=False,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
        required_connection_fields=("http_path",),
    ),
    # Issue #9 sources: conservative Phase-1 registration. Sync execution, no
    # tokenization / histogram / fast-estimate / freshness claim until the driver
    # gate and integration tests confirm each capability.
    "redshift": FlavorCapabilities(
        flavor="redshift",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
    ),
    "teradata": FlavorCapabilities(
        flavor="teradata",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=True,
        supports_primary_key_reflection=True,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
    ),
    "impala": FlavorCapabilities(
        flavor="impala",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=True,
        supports_primary_key_reflection=False,
        supports_range_key_bounds=True,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
    ),
    "generic": FlavorCapabilities(
        flavor="generic",
        async_driver=False,
        supports_streaming_query=False,
        supports_view_listing=False,
        supports_primary_key_reflection=False,
        supports_range_key_bounds=False,
        supports_modulo_split=True,
        supports_fast_row_estimate=False,
        supports_ntile=False,
    ),
}


_DIALECT_ALIASES = {
    "postgres": "postgresql",
    "sqlserver": "mssql",
    "oraclesql": "oracle",
}


def normalize_dialect(dialect: str | None) -> str:
    key = (dialect or "").strip().lower()
    return _DIALECT_ALIASES.get(key, key)


def flavor_from_db_url(db_url: str) -> str:
    scheme = (db_url or "").lower().split("://", 1)[0]
    # Match Redshift before PostgreSQL: it reuses Postgres query syntax but is a
    # distinct flavor (no pgcrypto / pg_stats / freshness assumptions).
    if "redshift" in scheme:
        return "redshift"
    if "postgres" in scheme:
        return "postgresql"
    if "mssql" in scheme:
        return "mssql"
    if "oracle" in scheme:
        return "oracle"
    if "databricks" in scheme:
        return "databricks"
    if "teradata" in scheme:
        return "teradata"
    if "impala" in scheme:
        return "impala"
    if "sqlite" in scheme:
        return "sqlite"
    return "generic"


def capabilities_for_dialect(dialect: str | None) -> FlavorCapabilities:
    key = normalize_dialect(dialect)
    return _CAPABILITIES.get(key, _CAPABILITIES["generic"])


def capabilities_for_db_url(db_url: str) -> FlavorCapabilities:
    return _CAPABILITIES.get(flavor_from_db_url(db_url), _CAPABILITIES["generic"])


def capability_matrix() -> dict[str, dict]:
    return {k: v.to_dict() for k, v in _CAPABILITIES.items()}


def missing_required_fields(dialect: str | None, query: dict | None) -> list[str]:
    caps = capabilities_for_dialect(dialect)
    query = query or {}
    missing: list[str] = []
    for field in caps.required_connection_fields:
        v = query.get(field)
        if not v:
            missing.append(field)
    return missing


def flavor_warnings(dialect: str | None) -> list[str]:
    caps = capabilities_for_dialect(dialect)
    out: list[str] = []
    if not caps.async_driver:
        out.append(
            "This flavor uses sync-threadpool query fallback; enable higher SOURCE_MAX_CONCURRENCY"
            " cautiously and validate throughput before production rollout."
        )
    if not caps.supports_primary_key_reflection:
        out.append(
            "Primary-key reflection may be unavailable; set key_column explicitly"
            " for deterministic split planning."
        )
    try:
        import config
        fallback = getattr(config, "TOKENIZATION_FALLBACK", TOKENIZATION_NONE)
        for kind in ("deterministic_hash", "random_token"):
            warning = caps.tokenization_warning(kind, fallback)
            if warning and warning not in out:
                out.append(warning)
    except (AttributeError, ImportError):
        pass
    return out
