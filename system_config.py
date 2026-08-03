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
ACCESS_KEY_ID: str = _get_str("S3_ACCESS_KEY_ID", "access_key_id", "AKIAIOSFODNN7EXAMPLE")
SECRET_ACCESS_KEY: str = _get_str("S3_SECRET_ACCESS_KEY", "secret_access_key", "poc-secret-not-checked")

# S3 request signature enforcement
REQUIRE_SIGV4: bool = _get_bool("REQUIRE_SIGV4", "require_sigv4", False)

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

HOST: str = _get_str("HOST", "host", "0.0.0.0")
PORT: int = _get_int("PORT", "port", 9000)

# Trust X-Forwarded-For/Proto only from these proxy IPs/CIDRs (comma list, or "*")
# so the agent recovers the real client IP + scheme behind a load balancer (used
# by audit logging). Default trusts loopback only (uvicorn's default) = no change.
FORWARDED_ALLOW_IPS: str = _get_str("FORWARDED_ALLOW_IPS", "forwarded_allow_ips", "127.0.0.1").strip()

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
