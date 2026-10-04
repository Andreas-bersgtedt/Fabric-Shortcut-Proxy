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
SOURCE_SNAPSHOT_NONE = "none"


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
    support_status: str = "supported"
    requires_explicit_split_key: bool = False
    supports_freshness_probe: bool = False
    supports_deterministic_tokenization: bool = False
    supports_random_tokenization: bool = False
    supports_ntile: bool = True
    supports_stats_histogram: bool = False
    source_snapshot_provider: str = SOURCE_SNAPSHOT_NONE
    supports_distributed_snapshot: bool = False
    source_snapshot_reopenable: bool = False
    required_connection_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        execution_mode = "async-native" if self.async_driver else "sync-threadpool-fallback"
        return {
            "flavor": self.flavor,
            "support_status": self.support_status,
            "execution_mode": execution_mode,
            "async_driver": self.async_driver,
            "supports_streaming_query": self.supports_streaming_query,
            "supports_view_listing": self.supports_view_listing,
            "supports_primary_key_reflection": self.supports_primary_key_reflection,
            "requires_explicit_split_key": self.requires_explicit_split_key,
            "supports_range_key_bounds": self.supports_range_key_bounds,
            "supports_modulo_split": self.supports_modulo_split,
            "supports_fast_row_estimate": self.supports_fast_row_estimate,
            "supports_freshness_probe": self.supports_freshness_probe,
            "supports_deterministic_tokenization": self.supports_deterministic_tokenization,
            "supports_random_tokenization": self.supports_random_tokenization,
            "supports_ntile": self.supports_ntile,
            "supports_stats_histogram": self.supports_stats_histogram,
            "source_snapshot_provider": self.source_snapshot_provider,
            "supports_distributed_snapshot": self.supports_distributed_snapshot,
            "source_snapshot_reopenable": self.source_snapshot_reopenable,
            "required_connection_fields": list(self.required_connection_fields),
            "capability_gaps": _capability_gaps(self),
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
            "data transfer while reducing safe throughput. Arrow fallback is opt-in."
        )


def _capability_gaps(caps: FlavorCapabilities) -> dict[str, dict[str, str]]:
    """Return actionable reasons, fallbacks, and costs for unverified features."""
    gaps: dict[str, dict[str, str]] = {}
    if not caps.supports_streaming_query:
        gaps["streaming_query"] = {
            "reason": "The synchronous driver fetches the complete result before the proxy yields batches.",
            "fallback": "Use smaller splits or row caps; batch-shaped output remains available but is not server-side streaming.",
            "cost": "Proxy memory remains proportional to the full split result.",
        }
    if not caps.supports_primary_key_reflection:
        gaps["primary_key_reflection"] = {
            "reason": "The dialect does not reliably expose primary-key metadata through the configured inspector.",
            "fallback": "Select an explicit key_column from the reflected source columns.",
            "cost": "Table setup requires operator input; the selected key must be suitable for the configured split strategy.",
        }
    if not caps.supports_fast_row_estimate:
        gaps["fast_row_estimate"] = {
            "reason": "No verified low-cost catalog row estimate is implemented for this dialect.",
            "fallback": "Run an exact COUNT(*) when an estimate is requested.",
            "cost": "Counting may scan the source object and add source latency/load.",
        }
    if not caps.supports_freshness_probe:
        gaps["freshness_probe"] = {
            "reason": "No reliable, dialect-specific catalog change token is implemented.",
            "fallback": "Use manual/TTL refresh, or explicitly allow a full content read during auto refresh.",
            "cost": "Without a cheap probe, refresh detection may require rereading and hashing the full source.",
        }
    if not caps.supports_stats_histogram:
        gaps["stats_histogram"] = {
            "reason": "A source statistics histogram reader is not implemented for this dialect.",
            "fallback": "Use NTILE quantiles where supported, otherwise equal-span key ranges.",
            "cost": "NTILE may scan/sort source rows; equal-span ranges can be imbalanced for skewed keys.",
        }
    if not caps.supports_deterministic_tokenization or not caps.supports_random_tokenization:
        gaps["native_tokenization"] = {
            "reason": "Native tokenization SQL has not been verified for every token kind on this dialect.",
            "fallback": "Leave transforms disabled, or explicitly opt in to TOKENIZATION_FALLBACK=arrow.",
            "cost": "Arrow sends plaintext source values through the proxy and adds proxy CPU, memory, and network transfer.",
        }
    if caps.source_snapshot_provider == SOURCE_SNAPSHOT_NONE:
        gaps["source_snapshot"] = {
            "reason": "No transaction/snapshot provider is implemented for this dialect.",
            "fallback": "Use best-effort independent source reads.",
            "cost": "Rows read by separate splits may reflect different source commit points.",
        }
    return gaps


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
        support_status="development",
        supports_freshness_probe=True,
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
        supports_freshness_probe=True,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
        supports_stats_histogram=True,
        source_snapshot_provider="postgresql_exported",
        supports_distributed_snapshot=True,
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
        supports_freshness_probe=True,
        supports_deterministic_tokenization=True,
        supports_random_tokenization=True,
        supports_stats_histogram=True,
        source_snapshot_provider="mssql_transaction",
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
        support_status="supported",
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
        support_status="beta",
        requires_explicit_split_key=True,
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
        support_status="beta",
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
        support_status="beta",
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
        support_status="preview",
        requires_explicit_split_key=True,
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
        support_status="unsupported",
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
            "Primary-key reflection is unavailable; select an explicit key_column"
            " from the reflected source columns before planning splits."
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
