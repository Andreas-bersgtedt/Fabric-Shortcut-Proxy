"""
System configuration — infrastructure & cluster settings.

S3 bucket, server binding, artifact store, fleet coordination, control plane,
high availability, and admin UIs. These settings typically remain stable across
deployments and rarely change per-environment.

Settings resolve with this precedence (highest wins):
    1. environment variable
    2. external JSON config file (``config.system.json``)
    3. built-in default

Note: Monolithic config.json is no longer supported. Use config.system.json.
"""
from __future__ import annotations

import json
import os
import sys


# ---------------------------------------------------------------------------
# JSON Config loading — config.system.json only
# ---------------------------------------------------------------------------

def _load_config_file() -> dict:
    """Load system configuration from config.system.json.
    
    Precedence:
      1. config.system.json
      2. empty dict (no fallback to monolithic config.json)
    """
    section_path = "config.system.json"
    try:
        with open(section_path, "r", encoding="utf-8-sig") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            print(f"[system_config] {section_path}: top-level JSON must be an object; ignoring.", file=sys.stderr)
            return {}
        # Extract 'system' section if present
        return data.get("system", data) if "system" in data else data
    except FileNotFoundError:
        print(f"[system_config] {section_path}: file not found; using defaults only.", file=sys.stderr)
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[system_config] failed to read {section_path!r}: {exc}", file=sys.stderr)
        return {}


_FILE_CFG: dict = _load_config_file()
_SYSTEM_CFG: dict = _FILE_CFG


def _raw(env: str | None, key: str, default):
    """Return env var (if set), else JSON value (if present), else default."""
    if env and env in os.environ:
        return os.environ[env]
    if key in _SYSTEM_CFG:
        return _SYSTEM_CFG[key]
    return default


def _get_str(env: str | None, key: str, default: str) -> str:
    v = _raw(env, key, default)
    return default if v is None else str(v)


def _get_int(env: str | None, key: str, default: int) -> int:
    v = _raw(env, key, default)
    return int(str(v)) if not isinstance(v, bool) else default


def _get_bool(env: str | None, key: str, default: bool) -> bool:
    v = _raw(env, key, None)
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# S3 / bucket settings
# ---------------------------------------------------------------------------

BUCKET_NAME: str = _get_str("S3_BUCKET", "bucket", "fabric-iceberg-poc")
WAREHOUSE_PREFIX: str = _get_str(None, "warehouse_prefix", "db")

# Virtual object path layout: legacy (db/<table>) or canonical (db/<server>/<database>/<schema>/<object>)
OBJECT_PATH_LAYOUT: str = _get_str("OBJECT_PATH_LAYOUT", "object_path_layout", "canonical").strip().lower()
ENABLE_LEGACY_PATH_ALIASES: bool = _get_bool("ENABLE_LEGACY_PATH_ALIASES", "enable_legacy_path_aliases", False)

# S3 credentials
ACCESS_KEY_ID: str = _get_str("S3_ACCESS_KEY_ID", "access_key_id", "")
SECRET_ACCESS_KEY: str = _get_str("S3_SECRET_ACCESS_KEY", "secret_access_key", "")

# S3 request signature enforcement
REQUIRE_SIGV4: bool = _get_bool("REQUIRE_SIGV4", "require_sigv4", True)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

HOST: str = _get_str("HOST", "host", "0.0.0.0")
PORT: int = _get_int("PORT", "port", 9000)

# Trust X-Forwarded-For/Proto only from these proxy IPs/CIDRs (comma list, or "*")
# so the agent recovers the real client IP + scheme behind a load balancer (used
# by audit logging). Default trusts loopback only (uvicorn's default) = no change.
FORWARDED_ALLOW_IPS: str = _get_str("FORWARDED_ALLOW_IPS", "forwarded_allow_ips", "127.0.0.1").strip()

# Browser origins allowed to call the API. Empty means no cross-origin browser access.
CORS_ALLOWED_ORIGINS: str = _get_str("CORS_ALLOWED_ORIGINS", "cors_allowed_origins", "").strip()

# TLS termination at the proxy (Phase 4). Provide BOTH a cert and key to serve
# HTTPS; empty = plain HTTP (terminate TLS at a fronting LB instead).
TLS_CERT_FILE: str = _get_str("TLS_CERT_FILE", "tls_cert_file", "")
TLS_KEY_FILE: str = _get_str("TLS_KEY_FILE", "tls_key_file", "")

# ---------------------------------------------------------------------------
# Admin UIs / observability
# ---------------------------------------------------------------------------

# Optional config-builder admin SPA at /_config
ENABLE_CONFIG_BUILDER: bool = _get_bool("ENABLE_CONFIG_BUILDER", "enable_config_builder", False)

# Optional monitoring dashboard SPA at /_monitor
ENABLE_MONITOR: bool = _get_bool("ENABLE_MONITOR", "enable_monitor", False)

# Verbose S3 access diagnostics: structured per-response logs for _delta_log
# commits and ranged reads, so a Direct Lake reproduction captures the exact
# request/range/status chain. On by default; set to false to quiet the logs.
S3_ACCESS_LOG: bool = _get_bool("S3_ACCESS_LOG", "s3_access_log", True)

# Resilient startup: quarantine a table that can't be brought online (unreachable
# source, bad credential, missing column) instead of exiting EX_CONFIG (78). The
# agent serves every healthy table + mount and a background loop retries the
# quarantined ones. Set false for the legacy fail-fast behavior.
QUARANTINE_FAILED_TABLES: bool = _get_bool("QUARANTINE_FAILED_TABLES", "quarantine_failed_tables", True)
# Seconds between background retries of quarantined tables (0 disables retrying).
TABLE_RETRY_SECONDS: int = _get_int("TABLE_RETRY_SECONDS", "table_retry_seconds", 60)

# Storage proxy: serve mounted buckets (config.mounts.json) as byte passthrough
# from S3/NFS/SMB backends, alongside the relational->Iceberg path. Off by default.
ENABLE_STORAGE_PROXY: bool = _get_bool("ENABLE_STORAGE_PROXY", "enable_storage_proxy", False)

# Storage proxy security (Phase 4). Force SigV4 auth on mounted buckets even when
# the global REQUIRE_SIGV4 is off, and audit every mounted-object access.
ENFORCE_MOUNT_AUTH: bool = _get_bool("ENFORCE_MOUNT_AUTH", "enforce_mount_auth", True)
ENABLE_AUDIT_LOG: bool = _get_bool("ENABLE_AUDIT_LOG", "enable_audit_log", True)
AUDIT_LOG_FILE: str = _get_str("AUDIT_LOG_FILE", "audit_log_file", "")

# ---------------------------------------------------------------------------
# Credential store (Manager-owned; persists DB URLs across restarts)
# ---------------------------------------------------------------------------

# Allow operators to save encrypted DB credentials via the config builder so they
# survive a restart (hydrated into DB_URL / DB_URL_<ID> when the Manager starts).
ENABLE_CREDENTIAL_STORE: bool = _get_bool("ENABLE_CREDENTIAL_STORE", "enable_credential_store", True)

# Override the store file location ("" => <repo>/secrets/credentials.json).
CREDENTIAL_STORE_PATH: str = _get_str("CREDENTIAL_STORE_PATH", "credential_store_path", "")

# ---------------------------------------------------------------------------
# Open Mirroring publisher (config.open_mirror.json targets)
# ---------------------------------------------------------------------------

# Run the background publish loop on the Manager: periodically read each target's
# source tables and push initial/incremental batches into the Fabric landing zone.
# Off by default (publishing is also available on demand via the config builder).
OPEN_MIRROR_PUBLISH: bool = _get_bool("OPEN_MIRROR_PUBLISH", "open_mirror_publish", False)
# Seconds between publish cycles when the loop is enabled.
OPEN_MIRROR_INTERVAL_SECONDS: int = _get_int("OPEN_MIRROR_INTERVAL_SECONDS", "open_mirror_interval_seconds", 300)
# Publish mode: "incremental" (diff against the last snapshot via __rowMarker__) or
# "initial" (always a full insert batch, no markers).
OPEN_MIRROR_MODE: str = _get_str("OPEN_MIRROR_MODE", "open_mirror_mode", "incremental").strip().lower()
# Directory for the change-tracking cursor/state (one JSON file per table).
# Kept OUTSIDE the landing zone so Fabric never sees it. Gitignored.
# A packaged Linux install creates /var/lib/fabric-shortcut-proxy/open-mirror and
# grants it to the service user; that path is used only when it already exists and
# is writable, so unprivileged runs (tests, CI, source checkouts) stay local.
_OPEN_MIRROR_SERVICE_STATE_DIR = "/var/lib/fabric-shortcut-proxy/open-mirror"


def _open_mirror_state_default() -> str:
    if os.name != "posix":
        return "./.open_mirror_state"
    if os.path.isdir(_OPEN_MIRROR_SERVICE_STATE_DIR) and os.access(
        _OPEN_MIRROR_SERVICE_STATE_DIR, os.W_OK
    ):
        return _OPEN_MIRROR_SERVICE_STATE_DIR
    return "./.open_mirror_state"


_OPEN_MIRROR_STATE_DEFAULT = _open_mirror_state_default()
OPEN_MIRROR_STATE_DIR: str = _get_str(
    "OPEN_MIRROR_STATE_DIR", "open_mirror_state_dir", _OPEN_MIRROR_STATE_DEFAULT
)
# Per-table row cap per cycle (0 = the connection's query_max_rows default).
OPEN_MIRROR_MAX_ROWS: int = _get_int("OPEN_MIRROR_MAX_ROWS", "open_mirror_max_rows", 0)
# Optional safety bounds for draining watermark pages (0 = unlimited this cycle).
OPEN_MIRROR_MAX_PAGES_PER_CYCLE: int = _get_int(
    "OPEN_MIRROR_MAX_PAGES_PER_CYCLE", "open_mirror_max_pages_per_cycle", 0
)
OPEN_MIRROR_MAX_ROWS_PER_CYCLE: int = _get_int(
    "OPEN_MIRROR_MAX_ROWS_PER_CYCLE", "open_mirror_max_rows_per_cycle", 0
)
# Fabric mirroring preflight and bounded self-healing.
OPEN_MIRROR_SELF_HEALING: bool = _get_bool(
    "OPEN_MIRROR_SELF_HEALING", "open_mirror_self_healing", True
)
OPEN_MIRROR_PREFLIGHT_TIMEOUT_SECONDS: int = _get_int(
    "OPEN_MIRROR_PREFLIGHT_TIMEOUT_SECONDS", "open_mirror_preflight_timeout_seconds", 60
)
OPEN_MIRROR_START_COOLDOWN_SECONDS: int = _get_int(
    "OPEN_MIRROR_START_COOLDOWN_SECONDS", "open_mirror_start_cooldown_seconds", 300
)
OPEN_MIRROR_FABRIC_RETRY_ATTEMPTS: int = _get_int(
    "OPEN_MIRROR_FABRIC_RETRY_ATTEMPTS", "open_mirror_fabric_retry_attempts", 3
)


# ---------------------------------------------------------------------------
# Entra ID auth & Azure Key Vault (issue #16)
# ---------------------------------------------------------------------------

# Outbound identity mode for Key Vault / Azure access:
#   "default" (DefaultAzureCredential: MI/env/CLI), "managed_identity", "service_principal".
AUTH_MODE: str = _get_str("FSP_AUTH_MODE", "auth_mode", "default").strip().lower()

# Azure Key Vault URI, e.g. https://my-vault.vault.azure.net . Empty = KV disabled.
KEYVAULT_URI: str = _get_str("FSP_KEYVAULT_URI", "keyvault_uri", "").strip()

# Entra tenant / client for a service principal or user-assigned managed identity.
# A client secret, when needed, comes from AZURE_CLIENT_SECRET env — never config.
AZURE_TENANT_ID: str = _get_str("AZURE_TENANT_ID", "azure_tenant_id", "").strip()
AZURE_CLIENT_ID: str = _get_str("AZURE_CLIENT_ID", "azure_client_id", "").strip()

# Key Vault is NEVER a hard runtime dependency: an outage falls back to the local
# encrypted cache. Default off so the agent/heartbeat/manager never die on a KV
# outage (owner directive); when true, a cold start with no cache may fail-fast.
REQUIRE_KEYVAULT: bool = _get_bool("FSP_REQUIRE_KEYVAULT", "require_keyvault", False)

# Background KV refresh interval (seconds); positive = re-pull on this cadence.
KEYVAULT_REFRESH_SECONDS: int = _get_int("FSP_KEYVAULT_REFRESH_SECONDS", "keyvault_refresh_seconds", 300)

# Local-cache freshness TTL (seconds); <= 0 means NEVER expire so a fully offline /
# air-gapped deployment runs entirely from a pre-seeded local store.
KEYVAULT_CACHE_TTL: int = _get_int("FSP_KEYVAULT_CACHE_TTL", "keyvault_cache_ttl", 0)

# Phase 4 write-back: the Manager persists saved credentials INTO Key Vault too, making
# the vault the authoritative store. Needs Key Vault Secrets Officer on the Manager
# identity; fail-soft (the local encrypted save always wins). Default off.
KEYVAULT_WRITE_BACK: bool = _get_bool("FSP_KEYVAULT_WRITE_BACK", "keyvault_write_back", False)

# ---------------------------------------------------------------------------
# Artifact Store (cluster seam — Phase 0)
# ---------------------------------------------------------------------------

# Backends: "local" (filesystem/NFS/SMB) or "memory" (ephemeral/tests)
ARTIFACT_STORE_BACKEND: str = _get_str("ARTIFACT_STORE_BACKEND", "artifact_store_backend", "local").strip().lower()
ARTIFACT_STORE_DIR: str = _get_str("ARTIFACT_STORE_DIR", "artifact_store_dir", "./.artifacts")

# Serve Parquet from the shared artifact store (durable, shareable; zero regeneration)
ARTIFACT_STORE_SERVING: bool = _get_bool("ARTIFACT_STORE_SERVING", "artifact_store_serving", False)

# Publish a complete serving image (data + metadata) to the store at startup
PUBLISH_SERVING_IMAGE: bool = _get_bool("PUBLISH_SERVING_IMAGE", "publish_serving_image", False)

# ---------------------------------------------------------------------------
# Fleet / scale (Phase 3)
# ---------------------------------------------------------------------------

# Number of Agents to supervise (Manager)
AGENT_COUNT: int = _get_int("AGENT_COUNT", "agent_count", 1)

# This Agent's materialization shard (set by Manager)
AGENT_SHARD_INDEX: int = _get_int("AGENT_SHARD_INDEX", "agent_shard_index", 0)

# Total materialization shards (= AGENT_COUNT)
AGENT_SHARD_COUNT: int = _get_int("AGENT_SHARD_COUNT", "agent_shard_count", 1)

# Split-ownership strategy across shards: "modulo" (round-robin by split index) or
# "weighted" (size-weighted LPT using observed split sizes from the prior run).
SHARD_STRATEGY: str = _get_str("SHARD_STRATEGY", "shard_strategy", "modulo").strip().lower()

# Front the fleet with a built-in round-robin S3 gateway (Manager)
ENABLE_GATEWAY: bool = _get_bool("ENABLE_GATEWAY", "enable_gateway", False)

# Non-owner Agent: max wait for a sharded split to appear in the store
MATERIALIZE_WAIT_SECONDS: float = float(_get_int("MATERIALIZE_WAIT_SECONDS", "materialize_wait_seconds", 30))

# ---------------------------------------------------------------------------
# Control Plane (Phase 1)
# ---------------------------------------------------------------------------

# Agent: Manager control URL to register/heartbeat (blank = standalone)
MANAGER_URL: str = _get_str("MANAGER_URL", "manager_url", "").strip()

# Agent: stable id (blank = auto from host:port)
AGENT_ID: str = _get_str("AGENT_ID", "agent_id", "").strip()

# Agent: routable host/IP or DNS advertised to the Manager so the LB/gateway can
# dial this agent. Blank advertises the bind HOST (reachable same-box only when
# HOST is a wildcard like 0.0.0.0). Set to a real address for a multi-host fleet.
AGENT_ADVERTISE_HOST: str = _get_str("AGENT_ADVERTISE_HOST", "agent_advertise_host", "").strip()

# Manager: comma-separated exact hosts or IP networks accepted during registration.
AGENT_HOST_ALLOWLIST: str = _get_str(
    "AGENT_HOST_ALLOWLIST", "agent_host_allowlist", "127.0.0.1,0.0.0.0,::1,::,localhost"
).strip()

# Manager: control-plane REST bind address
CONTROL_HOST: str = _get_str("CONTROL_HOST", "control_host", "127.0.0.1")

# Manager: control-plane REST port
CONTROL_PORT: int = _get_int("CONTROL_PORT", "control_port", 9200)

# Agent heartbeat interval (ms)
HEARTBEAT_MS: int = _get_int("HEARTBEAT_MS", "heartbeat_ms", 2000)

# Manager marks Agent dead after this many missed heartbeats
HEARTBEAT_MISS_LIMIT: int = _get_int("HEARTBEAT_MISS_LIMIT", "heartbeat_miss_limit", 3)

# Manager: delay before respawning a crashed Agent (seconds)
AGENT_RESTART_BACKOFF_SECONDS: float = float(_get_int("AGENT_RESTART_BACKOFF", "agent_restart_backoff", 1))

# Manager: crash-loop guard — stop respawning after this many restarts
AGENT_MAX_RAPID_RESTARTS: int = _get_int("AGENT_MAX_RAPID_RESTARTS", "agent_max_rapid_restarts", 5)

# Agent: on drain, flip /readyz to 503 then wait this long before exiting so an
# external load balancer can deregister the backend and in-flight requests finish.
AGENT_DRAIN_GRACE_SECONDS: float = float(_get_int("AGENT_DRAIN_GRACE_SECONDS", "agent_drain_grace_seconds", 15))

# ---------------------------------------------------------------------------
# High Availability (Phase 5)
# ---------------------------------------------------------------------------

# Run a leader lease over the shared artifact store (HA mode)
MANAGER_HA: bool = _get_bool("MANAGER_HA", "manager_ha", False)

# Leader lease TTL (ms)
LEADER_LEASE_TTL_MS: int = _get_int("LEADER_LEASE_TTL_MS", "leader_lease_ttl_ms", 10_000)

# Leader lease renew interval (ms); must be < TTL
LEADER_LEASE_RENEW_MS: int = _get_int("LEADER_LEASE_RENEW_MS", "leader_lease_renew_ms", 3_000)

# Retention GC (Agent shard 0: periodically prune orphaned splits)
RETENTION_GC: bool = _get_bool("RETENTION_GC", "retention_gc", False)

# Retention GC sweep interval (seconds)
RETENTION_GC_INTERVAL_SECONDS: float = float(_get_int("RETENTION_GC_INTERVAL_SECONDS", "retention_gc_interval_seconds", 300))

# Rolling restart: max seconds to wait for each Agent to become healthy
ROLLING_RESTART_HEALTH_TIMEOUT: float = float(_get_int("ROLLING_RESTART_HEALTH_TIMEOUT", "rolling_restart_health_timeout", 30))

# ---------------------------------------------------------------------------
# Admin UI (Manager)
# ---------------------------------------------------------------------------

# Serve the /_manager operator console (fleet monitor + start/stop/restart/drain)
ENABLE_ADMIN_UI: bool = _get_bool("ENABLE_ADMIN_UI", "enable_admin_ui", False)

# Token required for mutating /_manager actions (X-Admin-Token header or ?token=)
ADMIN_TOKEN: str = _get_str("ADMIN_TOKEN", "admin_token", "").strip()

# ---------------------------------------------------------------------------
# Manager auth (standalone HTTP Basic gate over the whole control-plane surface)
# ---------------------------------------------------------------------------
# When enabled with a non-empty password, the Manager port requires HTTP Basic
# credentials for the operator surface (/_manager, /_config, /_monitor, /agents,
# root). Machine + liveness endpoints (/control, /healthz, /readyz) stay open so
# the fleet keeps registering and load balancers can probe.
MANAGER_AUTH_ENABLED: bool = _get_bool("MANAGER_AUTH_ENABLED", "manager_auth_enabled", True)
MANAGER_AUTH_USERNAME: str = _get_str("MANAGER_AUTH_USERNAME", "manager_auth_username", "admin").strip()
MANAGER_AUTH_PASSWORD: str = _get_str("MANAGER_AUTH_PASSWORD", "manager_auth_password", "")
