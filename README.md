# Fabric Shortcut Proxy

S3 table gateway for Microsoft Fabric. It exposes relational source data as shortcut-readable
table objects and generates Parquet files on demand from SQL pushdown queries.

> The proxy supports two output modes from the same backend data path:
> `TABLE_FORMAT=iceberg` and `TABLE_FORMAT=delta`.
> In Fabric environments, Delta mode is often preferred because Fabric reads `_delta_log`
> directly with no conversion layer. See [DELTA_FORMAT.md](DELTA_FORMAT.md).

## Architecture recap

```mermaid
flowchart LR
  Fabric[Microsoft Fabric / S3 clients]

  subgraph Proxy["Fabric Shortcut Proxy (FastAPI)"]
    AUTH["Auth middleware<br/>SigV4 (multi-key) + per-key ACL<br/>+ forced mount auth"]
    RT["s3/router.py<br/>GET / HEAD / ListObjectsV2 (+ range)"]
    subgraph WH["Warehouse bucket — DB to table"]
      RES["Iceberg / Delta resolver"]
      GEN["SQL pushdown to Parquet<br/>planner + db + parquet"]
    end
    subgraph MNT["Mounted buckets — storage proxy"]
      PT["passthrough<br/>byte streaming + range"]
      LOC["local — NFS / SMB"]
      S3B["s3 / MinIO"]
      AZ["azure Blob / ADLS"]
    end
    AUD["audit log"]
  end

  SRC[(Source RDBMS)]
  UP[(Upstream S3 / Azure / file share)]

  Fabric -->|S3 + SigV4| AUTH --> RT
  RT -->|warehouse bucket| RES --> GEN -->|SQL| SRC
  RT -->|mounted bucket| PT --> LOC & S3B & AZ --> UP
  AUTH -. denials .-> AUD
  PT -. access .-> AUD
```

A bucket with **no mount** resolves through the Iceberg/Delta path exactly as before;
a bucket **with a mount** streams bytes straight from its backend. Both share the same
SigV4 front door. See [Storage Proxy](#storage-proxy--secured-file--object-passthrough).

> For component-level detail — per-process flow diagrams for auth, the warehouse read
> path, the storage proxy, credential mediation, config, and the Manager/Agent control
> plane — see [TechnicalArchitecture.md](TechnicalArchitecture.md).

## Storage Proxy — secured file / object passthrough

Alongside the DB→table virtualization, the same S3 endpoint can serve **existing files**
from a storage backend as **read-only byte passthrough**. It is **additive**: a bucket with
a **mount** streams bytes from its backend; every other bucket resolves through the
Iceberg/Delta path unchanged, so a single deployment exposes the DB warehouse *and* file
shares/object stores at once.

**Backends** (`config.mounts.json`, gitignored — see [config.mounts.example.json](config.mounts.example.json)):

| Backend | Serves | Notes |
|---|---|---|
| `local` | a filesystem path | an OS-mounted **NFS/SMB** share (UNC or mount point); zero extra deps |
| `s3` | a native **S3 / MinIO / S3-compatible** bucket | ranged streaming + list pagination (`pip install '.[s3proxy]'`) |
| `azure` | a native **Azure Blob / ADLS Gen2** container | flat blob + hierarchical namespace (`pip install '.[azureblob]'`) |

**Security (Phase 4):**
- **Per-key authorization** — issue scoped S3 **access keys** (allowed buckets/prefixes, read-only), stored encrypted; SigV4 is verified against them. The legacy single key stays a wildcard until you add a key.
- **Forced auth on mounts** — `ENFORCE_MOUNT_AUTH` (default on) requires SigV4 on mounted buckets even when `REQUIRE_SIGV4` is off.
- **Upstream credential mediation** — clients never see upstream S3/Azure secrets; they're held encrypted (DPAPI/Fernet) and resolved by id. Outbound S3 covers static/session/assume-role/web-identity/profile/SSO/instance/process/anonymous; Azure covers connection-string/account-key/SAS/AAD/managed-identity/default/anonymous.
- **TLS** — terminate HTTPS at the proxy (`TLS_CERT_FILE` + `TLS_KEY_FILE`) or a fronting LB.
- **Audit** — every mounted-object access (identity, bucket, key, bytes) is logged when `ENABLE_AUDIT_LOG` is on; recent events at `GET /_config/api/audit`.

Enable it in the config-builder **Storage** tab, or set `ENABLE_STORAGE_PROXY=1` and drop a
`config.mounts.json`. Design, phasing, and diagrams: [devplan/StorageProxy.md](devplan/StorageProxy.md).

## Project layout

```
s3emulator/
├── main.py                  FastAPI app + lifespan + auth middleware (SigV4 multi-key + mount enforcement) + TLS
├── manager.py               Manager entrypoint (control plane + local Agent supervision)
├── config.py                All tunables (env / config.json / defaults) + validation
├── config.example.json      Template for the optional config.json
├── Manager.ps1              Bootstrap: venv + deps + launch Manager/Agent cluster
├── Manager.sh               Linux/macOS bootstrap for Manager/Agent cluster
├── validate_pyiceberg.py    Reference-reader validation (pyiceberg)
├── PLANNING.md              Hardening & enhancement roadmap
├── SCALE_ARCHITECTURE_PLAN.md  Manager/Agent cluster rewrite for scale (10⁸+ rows)
├── CONFIGURATION.md         Full configuration manual (PostgreSQL / SQL Server)
├── DELTA_FORMAT.md          Native Delta output design (TABLE_FORMAT=delta)
├── ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md  Real Oracle/Databricks operations + smoke tests
├── s3/
│   ├── router.py            GET / HEAD / ListObjectsV2 endpoints (warehouse + mount routing)
│   ├── auth.py              AWS SigV4 verification (multi-key resolver; H3)
│   └── xml_responses.py     S3 XML body builders (+ ListObjectsV2 pagination)
├── iceberg/
│   ├── schema.py            Iceberg ↔ PyArrow type mapping
│   ├── metadata.py          Build metadata.json (+ snapshot history, F2)
│   ├── manifest.py          Manifest list + manifest file (Avro, + stats F3)
│   ├── stats.py             Column stats + Iceberg value encoding (F3)
│   ├── state_store.py       Snapshot/version registry + split descriptors
│   └── freshness.py         Content-addressed refresh + poller (AUTO_REFRESH)
├── delta/
│   └── log.py               Native Delta _delta_log emitter (TABLE_FORMAT=delta)
├── planner/
│   ├── split_planner.py     Virtual file path → parameterised SQL
│   └── dialects.py          Per-dialect SQL: SQLite/Postgres/SQL Server (F6)
├── db/
│   ├── executor.py          Async SQL execution (retry, schema validation)
│   └── reflect.py           Reflect an arbitrary DB (config builder)
├── parquet/
│   └── generator.py         SQL rows → Parquet bytes (PyArrow)
├── cache/
│   └── lru_cache.py         In-memory + optional disk Parquet cache (F5), split pinning
├── observability/
│   ├── logging.py           structlog setup
│   ├── metrics.py           stdlib Prometheus-style metrics (H1)
│   ├── endpoints.py         /healthz /readyz /metrics /_admin (H1/H2)
│   ├── trace.py             Fabric request-timeline ring buffer
│   ├── querystats.py        Per-request SQL vs Parquet query-lag
│   └── audit.py             Storage-proxy access audit log (Phase 4)
├── security/
│   ├── access_keys.py       Scoped proxy access keys + per-key ACL (Phase 4)
│   ├── credential_store.py  Encrypted store (DPAPI/Fernet): DB URLs + upstream S3/Azure creds + access keys
│   └── credentials.py       Secret scrubbing helpers
├── storage/
│   ├── mounts.py            Mount registry (local | s3 | azure) + validation
│   ├── passthrough.py       Read-only S3 passthrough (list / head / ranged get) + audit
│   ├── s3_store.py          Native S3 / MinIO backend (ranged streaming + pagination)
│   ├── s3_auth.py           Outbound S3 auth (static / session / assume_role / web_identity / …)
│   ├── azure_store.py       Native Azure Blob / ADLS Gen2 backend
│   └── azure_auth.py        Outbound Azure auth (account_key / SAS / AAD / managed_identity / …)
├── demo/
│   └── seed_db.py           Seed SQLite with 50k demo sales rows
├── configbuilder/           Optional config-builder UI (ENABLE_CONFIG_BUILDER)
│   ├── router.py            /_config API (connect / inspect)
│   └── index.html           the single-page app
├── monitor/                 Optional monitoring dashboard (ENABLE_MONITOR)
│   ├── router.py            /_monitor API (summary / reset)
│   └── index.html           the dashboard SPA
└── tests/                   pytest suite (S3 API, storage proxy, auth/ACL, credential store, …)
    ├── test_s3_api.py           S3 API + Parquet round-trip
    ├── test_metadata.py         metadata.json / manifest structure
    ├── test_parquet_gen.py      Parquet generation
    ├── test_autoschema.py       Reflected-schema type mapping
    ├── test_config_file.py      config.json precedence
    ├── test_config_builder.py   Config-builder API
    ├── test_hardening.py        Phase 1/2: range reads, config, snapshot
    ├── test_metrics_health.py   H1/H2 metrics + health
    ├── test_phase2.py           Robustness: 416, retry→503, schema drift
    ├── test_auth.py             H3 SigV4 (botocore-signed)
    ├── test_dialects.py         F6 SQL dialects
    ├── test_multitable.py       F1 multi-table isolation
    ├── test_phase5.py           F5 disk cache / F3 stats / F2 time-travel
    ├── test_freshness.py        AUTO_REFRESH content-addressed snapshots
    ├── test_pinning.py          Split pinning (no size drift)
    ├── test_trace.py            Request-timeline classification
    ├── test_monitor.py          Monitor dashboard API
    ├── test_delta.py            Native Delta output + refresh diffs
    ├── test_capabilities.py     Per-flavor capability matrix tests
    ├── test_executor_sync_fallback.py  Sync-driver query fallback coverage
    ├── test_integration_oracle_databricks.py  Real Oracle/Databricks smoke (env-gated)
    ├── test_storage_proxy.py       Storage proxy P1: local mount + passthrough + coexistence
    ├── test_storage_proxy_s3.py    Storage proxy P2: native S3/MinIO (auth, streaming, pagination)
    ├── test_storage_proxy_azure.py Storage proxy P3: native Azure Blob/ADLS
    ├── test_access_control.py      Storage proxy P4: access keys + ACL + audit + mount enforcement
    └── test_credential_store.py    Encrypted credential store (DPAPI/Fernet)
```

  ## Oracle + Databricks Integration Coverage

  Real-environment integration smoke tests are available in
  [tests/test_integration_oracle_databricks.py](tests/test_integration_oracle_databricks.py).
  These tests are opt-in and skipped unless the required environment variables
  are present.

  ```powershell
  .\.venv\Scripts\python.exe -m pytest tests/test_integration_oracle_databricks.py -q
  ```

  For setup, day-2 operations, troubleshooting, and rollback, use
  [ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md).

## Quick start

### One command (recommended)

```powershell
# Bootstraps the venv, installs dependencies, and launches Manager + Agent
.\Manager.ps1
# Agent data plane (Fabric endpoint):  http://0.0.0.0:9000
# Manager control plane (admin/ops):   http://127.0.0.1:9200

# Options
.\Manager.ps1 -AgentPort 9100          # override Fabric-facing S3 port
.\Manager.ps1 -DbUrl "postgresql+asyncpg://user:pass@host/db"
.\Manager.ps1 -Gateway                 # route Fabric through Manager control port
.\Manager.ps1 -Reinstall               # refresh dependencies
.\Manager.ps1 -Recreate                # rebuild the venv from scratch
.\Manager.ps1 -SkipInstall             # fastest start (venv already prepared)
```

```bash
# Linux/macOS equivalent
bash ./Manager.sh

# Options
bash ./Manager.sh --agent-port 9100
bash ./Manager.sh --db-url "postgresql+asyncpg://user:pass@host/db"
bash ./Manager.sh --gateway
bash ./Manager.sh --reinstall
bash ./Manager.sh --recreate
bash ./Manager.sh --skip-install
```

### Manual

```powershell
# 1. Create and activate venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install dependencies (editable install uses pyproject.toml)
pip install -e .
# For pyiceberg validation + SigV4 auth tests:
pip install -e ".[dev]"

# 3. Run Manager + Agent (recommended)
python manager.py
# Agent data plane (Fabric endpoint):  http://0.0.0.0:9000
# Manager control plane:               http://127.0.0.1:9200

# Optional standalone Agent mode:
# python main.py

# 4. Run tests
pytest tests/ -v
```

## C++ serving Agent (Windows + Linux)

Phase 6 ships a stateless C++ serving Agent that reads objects directly from the
published artifact-store image.

### Build

Windows (MSVC):

```powershell
.\agent-cpp\build.ps1
```

Linux (g++):

```bash
bash agent-cpp/build.sh
```

### Run (standalone serving)

Publish a complete serving image first (data + metadata):

```powershell
$env:ARTIFACT_STORE_SERVING = "1"
$env:PUBLISH_SERVING_IMAGE = "1"
$env:ARTIFACT_STORE_DIR = ".artifacts_cpp"
$env:PORT = "9011"
python main.py
# then: POST http://127.0.0.1:9011/_admin/publish-image
```

Run C++ Agent from that store:

Windows:

```powershell
$env:STORE_DIR = ".artifacts_cpp"
$env:PORT = "9012"
.\agent-cpp\agent.exe
```

Linux:

```bash
STORE_DIR=.artifacts_cpp PORT=9012 ./agent-cpp/agent
```

Quick conformance smoke:

```powershell
$env:S3EMU_SERVER = "http://127.0.0.1:9012"
python validate_pyiceberg.py
```

Optional fleet mode: set `MANAGER_URL=http://<manager-host>:<control-port>` before
starting the C++ agent so it registers and heartbeats into the Manager gateway
rotation.

## Configuring Fabric shortcut

In Microsoft Fabric, create an S3 shortcut pointing at:

| Setting            | Value                                    |
|--------------------|------------------------------------------|
| URL                | `http://<proxy-host>:9000`               |
| Bucket             | `fabric-iceberg-poc`                     |
| Path               | See mode-specific paths below |
| Access Key ID      | `AKIAIOSFODNN7EXAMPLE`                   |
| Secret Access Key  | `poc-secret-not-checked`                 |

Use a subpath that matches your selected `TABLE_FORMAT`:

| Mode | Fabric shortcut subpath |
|---|---|
| `iceberg` | `db/<server>/<database>/<schema>/<object>/metadata/v1.metadata.json` |
| `delta` | `db/<server>/<database>/<schema>/<object>` |

Fabric auto-discovers the remaining table objects from that entry point.

For strict canonical-only behavior (no legacy aliases), start Manager with:

```powershell
.\Manager.ps1 -ObjectPathLayout canonical -DisableLegacyAliases
```

> By default the proxy does **not** verify credentials (`REQUIRE_SIGV4=0`), so any
> Access Key ID / Secret works. If you set `REQUIRE_SIGV4=1`, the shortcut's
> credentials must match the proxy's `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`
> exactly, or requests are rejected with `403`.

> **Storage proxy note:** mounted buckets are always authenticated when
> `ENFORCE_MOUNT_AUTH=1` (the default), regardless of `REQUIRE_SIGV4`. Issue scoped
> per-tenant **access keys** (config-builder → Storage → *Access keys*) so each
> Fabric shortcut can reach only its allowed buckets/prefixes; the legacy single
> key keeps working as a wildcard until the first access key is created. Put the
> proxy behind TLS (`TLS_CERT_FILE`/`TLS_KEY_FILE` or an LB) before enabling auth.

## Connecting a real SQL database

Point the proxy at a table/view and a **key column** — the table schema is
**reflected from the source database automatically**, no manual column list:

```powershell
$env:DB_URL          = "postgresql+asyncpg://user:pass@host:5432/dbname"  # or mssql+aioodbc://...
$env:DB_SOURCE_TABLE = "public.orders"   # table or view (schema-qualified ok)
$env:KEY_COLUMN      = "order_id"        # integer split key -> enables auto-schema
$env:TABLE_NAME      = "orders"          # canonical path derived from source identity
.\Manager.ps1 -SkipInstall
```

```
# PostgreSQL (pip install asyncpg)
DB_URL=postgresql+asyncpg://user:pass@host/dbname

# SQL Server (aioodbc + ODBC Driver 18)
DB_URL=mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

If `KEY_COLUMN` is unset, the source table's **primary key** is auto-detected.
You only edit `config.py` to expose **multiple** tables (still schema-free) or to
override a reflected type.

### Config file (`config.json`)

Instead of environment variables you can drop a `config.json` next to `main.py`
(copy [config.example.json](config.example.json)). It can define **everything,
including multiple tables**, with no Python:

```json
{
  "db_url": "postgresql+asyncpg://user:pass@host:5432/db",
  "tables": [
    { "name": "orders",    "source_table": "public.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "public.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

Precedence is **env var > config.json > default**. Point `CONFIG_FILE` at a
different path if you like. `config.json` is gitignored (it holds your
connection string).

### Config Builder UI

Prefer clicking to typing JSON? Enable the built-in **config builder** — a small
web page that connects to your database, lists tables, and downloads a
`config.json`:

```powershell
$env:ENABLE_CONFIG_BUILDER = "1"
.\Manager.ps1 -SkipInstall
# then open http://localhost:9200/_config/   # Manager control-plane surface
```

If you run standalone `python main.py` (no Manager), open `http://localhost:9000/_config/`.

Enter host / user / password, pick tables (the key column is auto-detected and
overridable), and click **Download config.json**. It's **off by default** and
accepts DB credentials, so run it locally only. PostgreSQL needs `asyncpg`
(`pip install asyncpg`); SQL Server needs the ODBC driver.

> For in-depth, working PostgreSQL and SQL Server examples — single-table **and**
> multi-table, with source DDL and troubleshooting — see the
> [Configuration Manual](CONFIGURATION.md).

The split-query SQL is generated per **dialect** (F6). `planner/dialects.py`
selects an adapter from the `DB_URL` scheme and handles the differences
automatically:

| Dialect | Identifier quoting | Integer CAST | Row limit |
|---|---|---|---|
| SQLite | `"col"` | `INTEGER` | `LIMIT :max_rows` |
| PostgreSQL | `"col"` | `BIGINT` | `LIMIT :max_rows` |
| SQL Server | `[col]` | `BIGINT` | `SELECT TOP (:max_rows)` |

## Serving multiple tables

The proxy can expose several virtual tables from one instance (F1).
Each is described by a `TableDef` in `config.py`'s `TABLES` list and served under
`db/<server>/<database>/<schema>/<object>`. Schemas are reflected from the source — just name the
table and its key column:

```python
# config.py
TABLES = [
    TableDef(name="orders",    source_table="public.orders",    key_column="order_id",    num_splits=8),
    TableDef(name="customers", source_table="public.customers", key_column="customer_id", num_splits=4),
]
```

By default `TABLES` contains just the single demo `sales` table. `key_column` is
optional when the source has a primary key (auto-detected); pass it for views.
Each table gets its own deterministic snapshot lineage, format-specific metadata/log,
and on-demand Parquet splits. Create one Fabric shortcut per table pointing at its
canonical table prefix.

## Key environment variables

| Variable              | Default                                  | Description                         |
|-----------------------|------------------------------------------|-------------------------------------|
| `S3_BUCKET`           | `fabric-iceberg-poc`                     | Bucket name Fabric connects to      |
| `TABLE_NAME`          | `sales`                                  | Source SQL table name               |
| `DB_URL`              | `sqlite+aiosqlite:///./poc_source.db`    | SQLAlchemy async DB URL             |
| `DB_SOURCE_TABLE`     | `sales`                                  | Table/view to query (schema-qualified ok) |
| `KEY_COLUMN`          | *(unset)*                                | Integer split key; set it to reflect the schema from source |
| `NUM_SPLITS`          | `8`                                      | Number of virtual Parquet files     |
| `TABLE_FORMAT`        | `iceberg`                                | Output format: `iceberg` or `delta` (native `_delta_log` in Fabric). See [DELTA_FORMAT.md](DELTA_FORMAT.md) |
| `QUERY_TIMEOUT`       | `30`                                     | SQL query timeout (seconds)         |
| `PORT`                | `9000`                                   | HTTP listen port                    |
| `QUIET_404_LOGS`      | `1`                                      | Suppress expected-probe 404 access logs |
| `MAX_CONCURRENT_GENERATIONS` | `4`                               | Max simultaneous on-demand Parquet builds |
| `DB_MAX_RETRIES`      | `2`                                      | Retries on transient source-DB errors |
| `DB_RETRY_BACKOFF`    | `0.5`                                    | Linear backoff base (seconds) between retries |
| `VALIDATE_SOURCE_SCHEMA` | `1`                                   | Fail fast at startup if a declared column is missing |
| `REQUIRE_SIGV4`       | `0`                                      | Enforce AWS SigV4 request signatures (403 on mismatch) |
| `ENABLE_STORAGE_PROXY` | `0`                                     | Serve mounted buckets (`config.mounts.json`) as read-only byte passthrough (local / s3 / azure) |
| `ENFORCE_MOUNT_AUTH`  | `1`                                      | Require SigV4 on mounted buckets even when `REQUIRE_SIGV4=0` (a secured mount is never anonymous) |
| `ENABLE_AUDIT_LOG`    | `1`                                      | Emit a structured audit event per mounted-object access (identity, bucket, key, bytes) |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | *(unset)*                     | Serve HTTPS from the proxy when both are set (else terminate TLS at a fronting LB) |
| `ENABLE_CONFIG_BUILDER` | `0`                                    | Serve the config-builder UI (typically via Manager at `http://localhost:9200/_config/`) |
| `PARQUET_DISK_CACHE`  | `0`                                      | Persist generated Parquet to disk; warm restarts skip regeneration (F5) |
| `PARQUET_DISK_CACHE_DIR` | `./.parquet_cache`                    | Directory for the persistent Parquet cache |
| `CONCURRENT_STARTUP_MATERIALIZATION` | `1`                       | Materialize splits in parallel at startup (F4) |
| `ICEBERG_MANIFEST_STATS` | `0`                                   | Emit per-column stats (bounds/counts) in the manifest for reader pruning (F3) |
| `ICEBERG_SNAPSHOT_HISTORY` | `0`                                 | Retain prior snapshots for time-travel; `/_admin/refresh` advances the version (F2) |
| `SNAPSHOT_HISTORY_LIMIT` | `10`                                  | Max historical snapshots to retain |
| `PIN_MATERIALIZED_SPLITS` | `1`                                  | Serve snapshot data files byte-identical (prevents size-drift 404s). Keep on |
| `TIMESTAMP_ASSUME_UTC` | `1`                                     | Map naive datetimes to `timestamptz` (Fabric SQL endpoint rejects `TIMESTAMP_NTZ`) |
| `AUTO_REFRESH`        | `0`                                      | Re-read the source and publish a new snapshot when data changes (see [CONFIGURATION.md](CONFIGURATION.md) §12) |
| `ENABLE_MONITOR`      | `0`                                      | Serve the read-only monitoring dashboard at `/_monitor/` (local admin only) |
| `REQUEST_TRACE`       | `1`                                      | Record the Fabric request timeline (powers `/_admin/timeline` + the monitor) |

> This is a curated subset. **Every** setting — with defaults, categories, and help
> text — is documented in [CONFIGURATION.md](CONFIGURATION.md) and browsable in the
> config-builder UI's "All settings" panel.

## Documentation freshness

Use this order when docs appear to conflict:

1. **Runtime source of truth**: [config.py](config.py) (defaults, validation, effective-setting sources) and API/router modules.
2. **Operator docs (kept current)**:
  - [README.md](README.md) for setup and quick operations.
  - [CONFIGURATION.md](CONFIGURATION.md) for complete settings and behavior.
  - [DELTA_FORMAT.md](DELTA_FORMAT.md) for `TABLE_FORMAT=delta` semantics.
3. **Design/history docs (may describe prior states)**:
  - [PLANNING.md](PLANNING.md)
  - [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md)
  - [CONFIG_BUILDER_PLAN.md](CONFIG_BUILDER_PLAN.md)

Current defaults and behavior to assume unless overridden:

- Canonical object paths are enabled by default: `db/<server>/<database>/<schema>/<object>`.
- Legacy path aliases are disabled by default.
- Split planning is row-targeted by default (`split_target_rows=100000`) and prefers range-based slices.
- In Manager mode, the config UI is served from the control plane (`http://localhost:9200/_config/`).

## Operational endpoints

| Endpoint | Purpose |
|---|---|
| `GET /healthz`      | Liveness — 200 while the process is up |
| `GET /readyz`       | Readiness — 200 when the snapshot is built **and** the source DB is reachable, else 503 |
| `GET /metrics`      | Prometheus text exposition (request counts, bytes served, cache hit/miss, SQL latency histogram) |
| `GET /_admin/stats` | JSON snapshot of the same metrics plus cache occupancy |
| `GET /_admin/timeline?table=` | Per-table Fabric read timeline: proxy time vs Fabric-side gaps, per-kind counts, errors |
| `GET /_admin/trace?table=&kind=&status=` | Raw request log, newest first (e.g. `?status=404` lists blobs Fabric asked for) |
| `POST /_admin/trace/reset` | Clear the trace buffer before a fresh Fabric run |
| `GET /_admin/objects?table=` | Declared vs cached size per served object (pinpoints size-drift 404s) |
| `GET /_admin/schemas?table=` | Resolved logical types per column + risky-type flags |
| `GET /_monitor/`    | Monitoring dashboard SPA (when `ENABLE_MONITOR=1`) — per-table read/query stats, output format, query lag |

```powershell
curl http://localhost:9000/healthz          # {"status":"ok"}
curl http://localhost:9000/readyz           # {"status":"ready","checks":{...}}
curl http://localhost:9000/metrics          # Prometheus text
curl http://localhost:9000/_admin/stats     # JSON metrics + cache stats
```

## Admin API

```
POST /_admin/refresh
```
Rebuilds every table snapshot and clears all caches. Call after schema or data changes. With `ICEBERG_SNAPSHOT_HISTORY=1`, it instead advances each table to a new snapshot version (retaining prior versions for time-travel) and reuses the materialized data files. With `AUTO_REFRESH=1`, it forces an immediate content-addressed materialize + publish and reports which tables changed.

## Development & CI

```powershell
# Run the test suite
pytest tests/ -v

# Validate the served table with the reference reader (server must be running)
.\Manager.ps1 -SkipInstall      # in one terminal
$env:S3EMU_SERVER = "http://127.0.0.1:9000"
.\.venv\Scripts\python.exe validate_pyiceberg.py
```

GitHub Actions ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs the
`pytest` suite on Python 3.11/3.12 and a `fabric-compat` job that boots the proxy
and validates the table with `pyiceberg`.

Config is validated at startup (`config.validate_config()`); invalid settings fail
fast with an actionable message. Any password embedded in `DB_URL` is redacted in
logs.

## Acceptance criteria status

| Criterion | Status |
|---|---|
| S3 ListObjectsV2 returns metadata + data keys | ✅ |
| HEAD object returns correct size | ✅ |
| Format entry objects resolve correctly (`metadata.json` for Iceberg or `_delta_log` for Delta) | ✅ |
| Format metadata objects are valid and readable by reference clients | ✅ |
| GET data/*.parquet triggers SQL and returns real rows | ✅ |
| Range requests (partial reads) work | ✅ |
| Logs trace Fabric request → SQL → Parquet response | ✅ |
| Parameterized SQL only (no injection risk) | ✅ |

For the full hardening/feature roadmap and per-item status (H1–H9, F1–F6), see
[PLANNING.md](PLANNING.md). All planned items are complete; the format-risky
features (F3 manifest stats, F2 time-travel) ship behind default-off flags and
are validated with the `pyiceberg` reference reader.
