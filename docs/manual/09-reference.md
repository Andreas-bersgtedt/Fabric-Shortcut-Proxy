# Chapter 9: Reference

Quick lookup for settings, dialects, paths, launcher flags, endpoints, and terms. The
settings registry in [config.py](../config.py) is the source of truth, and the config
builder's All settings panel lists every key with its default and help text. The complete
narrative settings reference is [CONFIGURATION.md](../CONFIGURATION.md).

## 9.1 Settings groups

Settings are organized into these categories, in the order the config builder shows them:

| Category | Covers |
|---|---|
| Connection | Connection string, source table, key column, query timeout, retries, schema validation |
| S3 endpoint | Bucket, access keys, SigV4 enforcement, storage proxy, mount auth, audit, TLS |
| Server | Host, port, forwarded IPs, config builder and monitor toggles |
| Splits & query | Split strategy and counts, target rows, row cap, streaming |
| Caching | Metadata and Parquet cache TTLs, disk cache, split pinning |
| Robustness | Retry counts and backoff, timestamp handling |
| Admin & observability | Request trace, trace buffer, admin token |
| Iceberg (advanced) | Manifest stats, snapshot history |
| Data freshness | Auto-refresh, strategy, poll interval, full-pull allowance |
| Entra ID & Key Vault | Outbound Azure identity, Key Vault source + write-back, refresh cadence, cache TTL |
| Cluster (scale) | Agent count, gateway, shard strategy, HA, control plane, retention GC |

## 9.2 Common settings

Each setting has an environment variable and a JSON key; the environment always wins.

| Env var | JSON key | Default | Purpose |
|---|---|---|---|
| `DB_URL` | `db_url` | `sqlite+aiosqlite:///./poc_source.db` | Source connection string (selects the dialect) |
| `DB_SOURCE_TABLE` | `source_table` | `sales` | Source table or view |
| `KEY_COLUMN` | `key_column` | *(unset)* | Split key; enables reflection |
| `TABLE_FORMAT` | `table_format` | `iceberg` | `iceberg` or `delta` |
| `NUM_SPLITS` | `num_splits` | `8` | Fixed split count (else dynamic) |
| `SPLIT_STRATEGY` | `split_strategy` | `modulo` | `modulo`, `range`, `date`, or `auto` (per-table overridable) |
| `SPLIT_TARGET_ROWS` | `split_target_rows` | `100000` | Target rows per split for dynamic count (0 = fixed `num_splits`) |
| `SPLIT_BALANCE` | `split_balance` | `span` | `span` (equal width) or `count` (equal rows via histogram/NTILE) |
| `SPLIT_SAMPLE_ROWS` | `split_sample_rows` | `0` | Cap rows fed into `count` quantile planning (0 = full scan) |
| `SPLIT_USE_STATS_HISTOGRAM` | `split_use_stats_histogram` | `1` | Use the source stats histogram for `count` boundaries (SQL Server/PostgreSQL) |
| `QUERY_MAX_ROWS` | `query_max_rows` | `500000` | Max rows per split query |
| `QUERY_TIMEOUT` | `query_timeout_seconds` | `30` | SQL query timeout (seconds) |
| `BUCKET_NAME` | `bucket` | `fabric-iceberg-poc` | Warehouse bucket name |
| `PORT` | `port` | `9000` | Agent data-plane port |
| `REQUIRE_SIGV4` | `require_sigv4` | `1` | Enforce SigV4 on all buckets |
| `ENABLE_STORAGE_PROXY` | `enable_storage_proxy` | `0` | Serve mounted buckets |
| `ENFORCE_MOUNT_AUTH` | `enforce_mount_auth` | `1` | Force auth on mounts even if SigV4 is off |
| `ENABLE_AUDIT_LOG` | `enable_audit_log` | `1` | Log mounted-object access |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | `tls_cert_file` / `tls_key_file` | *(unset)* | Terminate HTTPS at the proxy |
| `AUTH_MODE` | `auth_mode` | `default` | Outbound Azure identity: `default`, `managed_identity`, `service_principal` |
| `KEYVAULT_URI` | `keyvault_uri` | *(unset)* | Azure Key Vault URI; empty disables Key Vault |
| `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` | `azure_tenant_id` / `azure_client_id` | *(unset)* | Entra tenant / client for a service principal or user-assigned MI |
| `AZURE_CLIENT_SECRET` | *(env only)* | *(unset)* | Service-principal secret — **environment only**, never a config file |
| `REQUIRE_KEYVAULT` | `require_keyvault` | `0` | Fail-fast on a cold start with no local cache |
| `KEYVAULT_REFRESH_SECONDS` | `keyvault_refresh_seconds` | `300` | Background re-pull cadence (seconds) |
| `KEYVAULT_CACHE_TTL` | `keyvault_cache_ttl` | `0` | Local-cache TTL; `0` = never expire (offline-friendly) |
| `KEYVAULT_WRITE_BACK` | `keyvault_write_back` | `0` | Manager persists saved credentials into Key Vault (needs Secrets Officer) |
| `ENABLE_CONFIG_BUILDER` | `enable_config_builder` | `0` | Serve the config builder UI |
| `ENABLE_MONITOR` | `enable_monitor` | `0` | Serve the monitor dashboard |
| `AUTO_REFRESH` | `auto_refresh` | `0` | Re-read the source and publish new snapshots |
| `REFRESH_STRATEGY` | `refresh_strategy` | `auto` | `auto`, `dialect_probe`, `content_hash`, `ttl`, or `manual` |
| `REFRESH_POLL_SECONDS` | `refresh_poll_seconds` | `600` | Auto-refresh poll interval (seconds) |
| `PARQUET_DISK_CACHE` | `parquet_disk_cache` | `0` | Persist generated Parquet to disk |
| `PIN_MATERIALIZED_SPLITS` | `pin_materialized_splits` | `1` | Keep snapshot data files byte-identical |
| `MATERIALIZE_MODE` | `materialize_mode` | `eager` | `eager`, `lazy` (defer + pin per table), or `virtual` (defer, regenerate on demand, zero at rest). Restart-required |
| `AGENT_COUNT` | `agent_count` | `1` | Number of supervised agents (enterprise) |
| `ENABLE_GATEWAY` | `enable_gateway` | `0` | Built-in round-robin S3 gateway (enterprise) |
| `CONTROL_PORT` | `control_port` | `9200` | Manager control-plane port |

This is a curated subset. Every setting, with defaults and help, is in the config builder's
All settings panel and in [CONFIGURATION.md](../CONFIGURATION.md).

### Live vs restart

Some settings apply in the running process without a restart: split settings, cache TTLs and
sizes, streaming, timestamp handling, split pinning, table format, and several observability
toggles. Structural settings always require a restart: the connection string, port, bucket,
HA, and the control plane. The settings catalog marks which are live.

## 9.3 Dialect matrix

| Dialect | Scheme prefix | Quoting | Row limit | Async driver | PK reflection | Deterministic token | Random token |
|---|---|---|---|---|---|---|---|
| SQLite | `sqlite+aiosqlite` | `"id"` | `LIMIT` | yes | yes | no | no |
| PostgreSQL | `postgresql+asyncpg` | `"id"` | `LIMIT` | yes | yes | yes (`pgcrypto`) | yes |
| SQL Server | `mssql+aioodbc` | `[id]` | `TOP` | yes | yes | yes | yes |
| Oracle | `oracle+oracledb` | `"id"` | `FETCH FIRST` | no | yes | yes | yes |
| Databricks | `databricks` | `` `id` `` | `LIMIT` | no | no | yes | yes |

Non-async dialects use a sync threadpool fallback. Databricks needs an explicit `key_column`
because primary-key reflection is unavailable, and an `http_path` to a SQL warehouse. SQL
Server accepts a SQL login, Windows (Integrated Security), or an Entra ID service principal —
choose the method in the Config Builder or set it in the `DB_URL` (see
[CONFIGURATION.md](../CONFIGURATION.md) §6).

## 9.4 Path formats

Canonical object paths are namespaced by source identity:

```
db/<server>/<database>/<schema>/<object>/...
```

Fabric shortcut entry points:

- Iceberg: `db/<server>/<database>/<schema>/<object>/metadata/v1.metadata.json`
- Delta: `db/<server>/<database>/<schema>/<object>`

Canonical paths are the default; legacy path aliases exist but are disabled by default.

## 9.5 Launcher flags

`Manager.ps1` (Windows) and `Manager.sh` (Linux/macOS) accept the same options; the Bash
launcher uses `--kebab-case`.

| Flag | Purpose |
|---|---|
| `-SkipInstall` | Skip dependency install |
| `-Recreate` | Recreate the `.venv` |
| `-NoPull` | Do not `git pull` before starting |
| `-Reinstall` | Reinstall dependencies |
| `-AgentCount <n>` | Number of supervised agents |
| `-Gateway` | Enable the built-in S3 gateway |
| `-AdminUi` | Serve the admin console `/_manager` |
| `-ConfigUi` | Serve the config builder `/_config` |
| `-Ha` | Manager leader-lease HA |
| `-RetentionGc` | Enable retention GC |
| `-DbUrl <url>` | Source connection string |
| `-TableFormat <iceberg\|delta>` | Output format |
| `-ControlPort <n>` / `-AgentPort <n>` | Control and data ports |
| `-ObjectPathLayout <layout>` / `-DisableLegacyAliases` | Path layout controls |
| `-BuildCppAgent` / `-RunCppAgent` / `-Cpp*` | Optional C++ serving agent |

## 9.6 Endpoints

| Endpoint | Purpose |
|---|---|
| `/healthz`, `/readyz` | Liveness and readiness |
| `/metrics`, `/_admin/stats` | Metrics (Prometheus text and JSON) |
| `/_admin/timeline`, `/_admin/trace`, `/_admin/objects`, `/_admin/schemas` | Diagnostics |
| `/_admin/refresh`, `/_admin/gc`, `/_admin/publish-image` | Snapshot, GC, and image actions |
| `/_config/api/keyvault`, `/_config/api/keyvault/test` | Key Vault status and a live connectivity test |
| `/_config`, `/_monitor`, `/_manager` | Config builder, monitor, admin console; Manager Basic auth required |

Chapter 8 has the full table with methods and query parameters.

## 9.7 Optional dependency extras

| Extra | Installs | For |
|---|---|---|
| `postgres` | `asyncpg` | PostgreSQL sources |
| `oracle` | `oracledb` | Oracle sources |
| `s3proxy` | `boto3` | Native S3 / MinIO mounts |
| `azureblob` | `azure-storage-blob`, `azure-identity` | Azure Blob / ADLS mounts |
| `keyvault` | `azure-keyvault-secrets`, `azure-identity` | Entra ID identity + Azure Key Vault credential store (issue #16) |
| `credentials` | `cryptography` | Encrypted store on non-Windows hosts |
| `dev` | `pyiceberg`, `botocore`, `httpx` | Tests and reference-reader validation |

## 9.8 Glossary

- **Split** — A virtual Parquet data file; a table is served as several splits.
- **Snapshot** — A fixed set of split files plus the metadata pointing at them; a new
  version is published when content changes.
- **Canonical path** — The `db/<server>/<database>/<schema>/<object>` object path namespaced
  by source identity.
- **Warehouse bucket** — A bucket that resolves to Iceberg/Delta table objects backed by SQL.
- **Mounted bucket** — A bucket that streams object bytes from a storage backend.
- **Dialect** — The per-engine SQL adapter selected from the connection string scheme.
- **Data plane / control plane** — The agent that serves reads vs the Manager that supervises
  the fleet and hosts admin surfaces.
- **Credential mediation** — Holding source and upstream secrets inside the proxy so Fabric
  only ever presents SigV4 keys.
- **Tokenization** — A projection-time column policy that pushes hashing into the source
  engine so plaintext never leaves the database.

## 9.9 Related documents

- [CONFIGURATION.md](../CONFIGURATION.md) — complete settings and reflection reference
- [TechnicalArchitecture.md](../TechnicalArchitecture.md) — component flow diagrams
- [SECURITY.md](../SECURITY.md) — authentication, TLS, and audit policy
- [DELTA_FORMAT.md](../DELTA_FORMAT.md) — native Delta output
- [TOKENIZATION_PUSHDOWN.md](../TOKENIZATION_PUSHDOWN.md) — tokenization design
- [CONNECTIVITY_SETUP.md](../CONNECTIVITY_SETUP.md) — network patterns
- [EXTERNAL_LB_RUNBOOK.md](../EXTERNAL_LB_RUNBOOK.md) and [SCALE_ARCHITECTURE_PLAN.md](../SCALE_ARCHITECTURE_PLAN.md) — scaling
- [ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](../ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md) — Oracle/Databricks operations

This is the last reference chapter. For end-to-end worked examples, see
[Chapter 10: Tutorials](10-tutorials.md), or return to the [manual index](README.md) for the
full table of contents.
