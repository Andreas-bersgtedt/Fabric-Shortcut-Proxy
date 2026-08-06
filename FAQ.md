# Fabric Shortcut Proxy — Complete FAQ

A single, consolidated FAQ compiled from **all** the project documentation: the
[README](README.md), [SSL_Deployment.md](SSL_Deployment.md), the installation guides, and
everything under [docs/](docs/) (configuration, security, delta format, freshness, scale,
usecases, troubleshooting, roadmap). It answers the common "what / how / why" questions and
the operational gotchas in one place.

> This is an additive summary document — it does not replace the focused guides. Each answer
> links to the authoritative source. The shorter positioning FAQ lives at
> [docs/FAQ.md](docs/FAQ.md); this file is the exhaustive version.

## Contents
1. [About & positioning](#1-about--positioning)
2. [Core concepts](#2-core-concepts)
3. [Sources & drivers](#3-sources--drivers)
4. [Configuration](#4-configuration)
5. [Deployment](#5-deployment)
6. [Fabric connectivity (OPDG, public TLS, Spark)](#6-fabric-connectivity)
7. [Security](#7-security)
8. [Data freshness / AUTO_REFRESH](#8-data-freshness--auto_refresh)
9. [Performance & splits](#9-performance--splits)
10. [Scale & cluster](#10-scale--cluster)
11. [Troubleshooting](#11-troubleshooting)
12. [Roadmap & known limitations](#12-roadmap--known-limitations)
13. [Where to go next](#13-where-to-go-next)

---

## 1. About & positioning

**What does the Fabric Shortcut Proxy do?**
It presents relational database tables to Microsoft Fabric through an **S3‑compatible endpoint
with AWS SigV4 auth**. Fabric sees a shortcut‑readable **Iceberg or Delta** table; the proxy
executes bounded SQL queries against the source and generates the required Parquet on demand.
The source database stays the system of record — this is a read‑path virtualization layer, not
a database, query engine, or full S3 implementation. See [README.md](README.md).

**Is it a Microsoft product?**
No. It is open‑source reference code / a proof of concept created to solve a specific customer
problem. Treat it as community‑supported unless a separate support and ownership model is
established. See [docs/FAQ.md](docs/FAQ.md).

**How does it compare with Open Mirroring?**
Different problems. Open Mirroring **replicates** source changes into OneLake (a durable copy,
incremental freshness, Fabric‑managed). The proxy **virtualizes** an existing source through a
shortcut with no copy pipeline — freshness is poll/probe/manual, and the source remains
authoritative. Use Open Mirroring when durable replication and near‑instant freshness are the
priority; use the proxy to expose an existing source as a shortcut without building ingestion
first. See [docs/FAQ.md](docs/FAQ.md).

**How is it different from a Data Factory connector?**
Data Factory is an ingestion/orchestration model (a pipeline copies/transforms into a
destination it owns). The proxy participates in Fabric's **read path**: a request for a virtual
Parquet object maps to a bounded SQL query returned over S3. For durable copies or complex
transforms, Data Factory is the conventional choice. See [docs/FAQ.md](docs/FAQ.md).

---

## 2. Core concepts

**Iceberg vs Delta output — which should I use?**
Both come from the same backend path, chosen by `TABLE_FORMAT`:
- `iceberg` (default): emits `metadata.json` + Avro manifest‑list + manifest files + versioned
  `v{N}.metadata.json`. Fabric converts Iceberg→Delta on the reader side (adds sync latency).
- `delta`: emits a native `_delta_log/` directory Fabric reads **directly** with no conversion
  layer — usually preferred in Fabric for lower lag.

See [docs/DELTA_FORMAT.md](docs/DELTA_FORMAT.md).

**What is the "warehouse bucket" vs "mounted bucket" distinction?**
A bucket with **no mount** resolves through the Iceberg/Delta virtualization path (DB→table). A
bucket **with a mount** (in `config.mounts.json`) streams bytes from a storage backend as
read‑only passthrough. Both share the same SigV4 front door; the proxy routes by bucket. See
[README.md](README.md).

**Can it serve files/objects, not just databases?**
Yes — the **storage proxy** (`ENABLE_STORAGE_PROXY=1`, off by default). A single deployment can
expose the relational warehouse *and* file shares/object stores at once. Backends:

| Backend | Serves | Extra install |
|---|---|---|
| `local` | a filesystem path (OS‑mounted NFS/SMB) | none |
| `s3` | native S3 / MinIO / S3‑compatible bucket | `pip install -e '.[s3proxy]'` |
| `azure` | Azure Blob / ADLS Gen2 container | `pip install -e '.[azureblob]'` |

See [README.md](README.md) and [docs/SECURITY.md](docs/SECURITY.md).

**What is credential mediation?**
Upstream secrets (DB URLs, S3/Azure creds) are held **encrypted** inside the proxy (DPAPI on
Windows, Fernet elsewhere) and referenced by id. Fabric clients only ever see SigV4 keys —
never the source password or cloud credential. See [docs/SECURITY.md](docs/SECURITY.md).

---

## 3. Sources & drivers

**Which sources are supported?**
SQLite (demo), **PostgreSQL**, **SQL Server**, **Oracle**, and **Databricks SQL Warehouse**.
See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) and
[docs/ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](docs/ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md).

**What driver / extra do I need per source?**

| Source | Driver | Install | Notes |
|---|---|---|---|
| SQLite | `aiosqlite` | in core | demo/dev only |
| SQL Server | `aioodbc` (in core) **+ OS ODBC Driver 18** | driver from Microsoft (no Python extra) | Windows Integrated auth supported |
| PostgreSQL | `asyncpg` | `pip install -e '.[postgres]'` | async‑native |
| Oracle | `oracledb` | `pip install -e '.[oracle]'` | sync fallback, capability‑gated |
| Databricks SQL | `databricks-sqlalchemy` (in core) | in core | needs `http_path` in the URL |

> The SQL Server ODBC driver is an **OS** package, not a pip extra. On Linux/macOS the
> encrypted credential store additionally needs `pip install -e '.[credentials]'` (Windows uses
> DPAPI, no extra).

**What connection URL do I use?**
- SQL Server: `mssql+aioodbc://user:pass@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes`
- PostgreSQL: `postgresql+asyncpg://user:pass@host:5432/db`
- Oracle: `oracle+oracledb://user:pass@host:1521/ORCLPDB1`
- Databricks: `databricks://token:<PAT>@<workspace>:443?http_path=/sql/1.0/warehouses/<id>`
- SQLite: `sqlite+aiosqlite:///./poc_source.db`

Always use a **read‑only** login scoped to the exposed tables. See
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

---

## 4. Configuration

**How does configuration precedence work?**
**Environment variable → JSON config file → built‑in default.** Every JSON key has a matching
uppercase env var (`db_url`→`DB_URL`, `table_format`→`TABLE_FORMAT`); the env var always wins.
See [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

**What are the config files?** (all **gitignored** — secrets never get committed; the tracked
`*.example.json` files are templates)

| File | Holds |
|---|---|
| `config.system.json` | ports, host binds, `table_format`, `require_sigv4`, feature flags, control plane, auth |
| `config.connection.json` | source `db_url` (default) + a `connections[]` array of named extra sources |
| `config.tables.json` | `tables[]`: `name`, `source_table`, `key_column`, optional `num_splits`, `split_strategy`, `split_target_rows`, `split_balance`, `split_sample_rows`, `schema` |
| `config.freshness.json` | `auto_refresh`, `refresh_poll_seconds`, `refresh_strategy`, `refresh_allow_full_pull`, `refresh_ttl_seconds` |
| `config.performance.json` | `num_splits`, `split_strategy`, `split_balance`, `split_sample_rows`, streaming, caching, memory thresholds |
| `config.mounts.json` | storage‑proxy mounts (`local`/`s3`/`azure`) — credentials by id, never inline |

**Single table with env vars only?** Set `DB_URL`, `DB_SOURCE_TABLE`, `KEY_COLUMN`,
`TABLE_NAME`, `NUM_SPLITS`; the schema auto‑reflects. **Multiple tables?** Use a `tables[]`
array in `config.tables.json`.

**How do I set credentials securely?**
- DB password: `DB_URL` env var, or the **encrypted credential store** (Manager; DPAPI/Fernet).
- Named sources: `DB_URL_<ID>` env var (id uppercased, non‑alphanumerics → `_`).
- SigV4 keys: `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`.
- Mount credentials: encrypted by id in the credential store, never in `config.mounts.json`.

**What is the config builder?** A web UI at `/_config` (enable with `ENABLE_CONFIG_BUILDER=1`)
that connects to your DB, lists tables, auto‑detects the PK, reflects schema, and edits/saves
config live. It accepts DB credentials, so keep it on a trusted network. See
[configbuilder/](configbuilder/) and [docs/CONFIG_BUILDER_PLAN.md](docs/CONFIG_BUILDER_PLAN.md).

---

## 5. Deployment

**How do I start a single node?**
```bash
# Linux/macOS
bash ./Manager.sh --skip-install
# Windows (PowerShell)
.\Manager.ps1 -SkipInstall
```
Data plane on **:9000** (Fabric connects here), control plane on **:9200** (admin only). See
[README.md](README.md).

**What are the startup modes?**

| Command | Runs | Ports |
|---|---|---|
| `python main.py` | Lite single‑node agent (no supervision) | 9000 |
| `Manager.sh` / `Manager.ps1` / `python -m enterprise.manager` | Manager + supervised agent(s) | 9000(+), 9200 |
| `Manager.ps1 -Gateway` / `--gateway` | Manager fronts the fleet with a built‑in S3 gateway | 9000 via Manager, 9200 |

**Manager vs Agent?** The **Manager** (control plane, :9200) owns config, the snapshot
registry, refresh orchestration, agent supervision, and the `/_manager` / `/_config` /
`/_monitor` UIs. Each **Agent** (data plane, :9000, :9001, …) is a stateless S3 endpoint that
runs SQL and serves Parquet. `AGENT_COUNT` (default 1) sets how many agents the Manager spawns,
each on `PORT + i`. See [docs/SCALE_ARCHITECTURE_PLAN.md](docs/SCALE_ARCHITECTURE_PLAN.md).

**Common launcher flags** (`Manager.sh` / `Manager.ps1`):

| Flag | Effect |
|---|---|
| `--skip-install` / `-SkipInstall` | Fast start (venv ready) |
| `--recreate` / `-Recreate` | Clean venv from scratch |
| `--reinstall` / `-Reinstall` | Refresh dependencies |
| `--no-pull` / `-NoPull` | Skip the git fast‑forward on start |
| `--db-url` / `-DbUrl` | Override `DB_URL` |
| `--table-format` / `-TableFormat` | `iceberg` \| `delta` |
| `--agent-count` / `-AgentCount` | Number of supervised agents |
| `--gateway` / `-Gateway` | Enable the built‑in S3 gateway |
| `--admin-ui` / `-AdminUi` | Serve `/_manager` |
| `--config-ui` / `-ConfigUi` | Serve `/_config` |
| `--admin-token` / `-AdminToken` | Set `ADMIN_TOKEN` |
| `--control-host` / `-ControlHost` | Control‑plane bind (default `127.0.0.1`) |

**How do I run it as a service?**
- **Linux (systemd):** a `fabric-shortcut-proxy.service` unit runs `Manager.sh` as a dedicated
  `fsp` user with an `EnvironmentFile=/etc/fabric-shortcut-proxy.env`. Full walkthrough:
  [docs/installation/Linux_Deployment.md](docs/installation/Linux_Deployment.md) and
  [docs/LINUX_MANAGER_TROUBLESHOOTING.md](docs/LINUX_MANAGER_TROUBLESHOOTING.md) §8.
- **Windows:** wrap `Manager.ps1` with **NSSM** (auto‑restart) or **Task Scheduler**. Full
  walkthrough: [docs/installation/Windows_Deployment.md](docs/installation/Windows_Deployment.md).

**How do I deploy a code update?** The service `git pull`s `origin/main` on start, so: push to
`origin/main`, then restart the service; verify with
`git -C /opt/fabric-shortcut-proxy log --oneline -1`. A **local, unpushed** commit never
deploys. See [docs/LINUX_MANAGER_TROUBLESHOOTING.md](docs/LINUX_MANAGER_TROUBLESHOOTING.md) §10.

---

## 6. Fabric connectivity

**How does Fabric reach the proxy?** Two patterns (see
[docs/UsecasesAndScenarios.md](docs/UsecasesAndScenarios.md) and the install guides):

| Pattern | Path | Public exposure | When |
|---|---|---|---|
| **Private (recommended)** | Fabric → **OPDG** → `http://<proxy-private-ip>:9000` | none | on‑prem / VNet / VPC |
| **Public internet** | Fabric → **directly** to `https://<fqdn>` (nginx TLS) — **no OPDG** | 443 only | cloud‑native, no private path |

**What is the OPDG and when is it needed?** The On‑Premises Data Gateway is a Microsoft Windows
service that bridges OneLake to a **private** endpoint. Install it on a Windows host that can
reach the proxy, register it in Fabric, then select it in the shortcut's *Data gateway*
dropdown. A **public** HTTPS endpoint needs **no** OPDG (set *Data gateway* = *None*). Ref:
<https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>.

**How do I create the shortcut?** In a Fabric Lakehouse: *New shortcut → Amazon S3 compatible*:
- **URL**: `http://<proxy-private-ip>:9000` (private) or `https://<fqdn>` (public).
- **Access key / Secret**: your `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`.
- **Data gateway**: your OPDG (private) or *None* (public).
- Browse to the bucket (`fabric-iceberg-poc`) and select the table folder(s).

**Can Spark read it, not just shortcuts?** Yes — Fabric Spark reads via `s3a://` / `boto3`
behind a **Managed Private Endpoint + Private Link Service** (on‑prem via a forwarding VM over
ExpressRoute/VPN). See [docs/UsecasesAndScenarios.md](docs/UsecasesAndScenarios.md).

---

## 7. Security

**How does SigV4 auth work?** The proxy verifies the AWS SigV4 signature against either the
legacy single key (`S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY`) or **scoped access keys** stored
encrypted. Gated by `REQUIRE_SIGV4` (default off for POC parity); when on, unsigned/mis‑signed
requests get `403`. See [docs/SECURITY.md](docs/SECURITY.md).

**What are scoped access keys?** Per‑key records with `allowed_buckets`, optional per‑bucket
`allowed_prefixes`, read‑only permissions, and an `enabled` flag — managed in the config
builder's **Storage → Access keys** panel or the `/_config/api/access-keys` API. The legacy key
stays a wildcard until you add the first scoped key.

**What is forced mount auth?** `ENFORCE_MOUNT_AUTH` (default on) requires SigV4 on mounted
buckets even when `REQUIRE_SIGV4` is off, so a secured mount is never served anonymously.

**What encryption backs the credential store?** DPAPI on Windows (no extra); Fernet elsewhere
(`pip install -e '.[credentials]'` + an `FSP_CRED_KEY`). It holds DB URLs, upstream S3/Azure
credentials, and access keys — encrypted at rest.

**How do I protect the operator console (:9200)?**
- **`MANAGER_AUTH_ENABLED` + `MANAGER_AUTH_USERNAME` + `MANAGER_AUTH_PASSWORD`** — an HTTP Basic
  gate over the whole control surface (`/_manager`, `/_config`, `/_monitor`, `/agents`). Active
  only when a non‑empty password is set; run it over TLS. Settable in the `/_config` Advanced
  tab or the env file.
- **`ADMIN_TOKEN`** — required (`X-Admin-Token` header or `?token=`) for *mutating* `/_manager`
  actions (start/stop/restart/drain/scale). Generate with `openssl rand -hex 24`.

Both are independent layers; the machine/liveness endpoints (`/control`, `/healthz`, `/readyz`)
stay open so agents keep registering. See [docs/SECURITY.md](docs/SECURITY.md).

**How do I add TLS?** Terminate at **nginx** in front (recommended for the fleet — see
[SSL_Deployment.md](SSL_Deployment.md)) or serve HTTPS in‑process by setting `TLS_CERT_FILE` +
`TLS_KEY_FILE` (best for a single standalone process; the clustered internal hops are HTTP by
design). Fabric requires a **CA‑trusted** cert on the data plane — self‑signed is rejected there.

**What does the audit log capture?** With `ENABLE_AUDIT_LOG` (default on), every mounted‑object
access (identity, client IP, bucket, key, method, status, bytes) plus auth denials — to
structlog, an optional `AUDIT_LOG_FILE`, and a recent ring at `GET /_config/api/audit`. Set
`FORWARDED_ALLOW_IPS` to your LB so the real client IP is logged.

**Hardening checklist:** credentials in env/credential store (never JSON) · `config.*.json`
gitignored · run under a low‑privilege user · firewall `9200` to admins · `REQUIRE_SIGV4=1` when
shared · `ADMIN_TOKEN` + `MANAGER_AUTH` over TLS · rotate SigV4 keys · ship audit logs. See
[docs/SECURITY.md](docs/SECURITY.md).

---

## 8. Data freshness / AUTO_REFRESH

**Why doesn't Fabric see my updates automatically?** With content‑independent snapshot ids, a
change alters a chunk's bytes but not its path, and Fabric caches by path. The proxy fixes this
with **content‑addressed identity**: each chunk file is named `split-{i}-{sha(rows)[:12]}.parquet`
— same content keeps the path (no churn), changed content gets a new path (Fabric re‑reads), and
the snapshot id is a root hash that changes iff some chunk changed. See
[docs/FRESHNESS_PLAN.md](docs/FRESHNESS_PLAN.md).

**How does the poller work?** With `AUTO_REFRESH=1`, every `REFRESH_POLL_SECONDS` (default 600 =
10 min) it runs the `REFRESH_STRATEGY` cascade and publishes a new snapshot only when content
actually changed.

**Freshness settings:**

| Key | Default | Meaning |
|---|---|---|
| `AUTO_REFRESH` | `0` | Enable the background poller |
| `REFRESH_POLL_SECONDS` | `600` | Poll interval |
| `REFRESH_STRATEGY` | `auto` | `auto` · `dialect_probe` · `content_hash` · `ttl` · `manual` |
| `REFRESH_ALLOW_FULL_PULL` | `0` | In `auto`, allow a full re‑read when the probe is unavailable |
| `REFRESH_TTL_SECONDS` | `1200` | Window for the `ttl` strategy |

**What are the strategies?** `auto` = probe → (skip if unchanged) → full read only if
`REFRESH_ALLOW_FULL_PULL=1`; `dialect_probe` = cheap change token from source system views
(SQL Server `sys.dm_db_index_usage_stats.last_user_update`, PostgreSQL `pg_stat_user_tables`);
`content_hash` = always re‑read + hash (safe, expensive); `ttl` = re‑read on a timer; `manual` =
only on `POST /_admin/refresh`.

**What does `refresh_probe_unavailable` mean?** In `auto`, the cheap probe returned nothing
(e.g. the SQL Server DMV is empty after a restart/failover, or the login lacks permission), and
`REFRESH_ALLOW_FULL_PULL=0`, so the poller **skips** that table to avoid hammering the DB. Fix by
setting `refresh_allow_full_pull: true`, switching to `content_hash`, or triggering a manual
refresh. See [docs/FRESHNESS_PLAN.md](docs/FRESHNESS_PLAN.md).

---

## 9. Performance & splits

**What is a split?** A deterministic slice of the table served as one virtual Parquet file;
Fabric reads splits in parallel. `NUM_SPLITS` (default 8) sets the count, or set
`split_target_rows` (default 100000) to derive the count from a target rows‑per‑file.

**Split strategies (`SPLIT_STRATEGY`):**

| Strategy | How | Trade‑off |
|---|---|---|
| `modulo` (default) | `WHERE (PK % :n) = :i` | simple, deterministic; but full‑table scan per split |
| `range` | `WHERE PK >= :lo AND PK < :hi` | index range scans (fast); needs an integer PK + MIN/MAX |
| `date` | `WHERE d >= :start AND d < :end` | natural for time series; needs a date/timestamp column |
| `auto` | range → date → modulo fallback | picks the best available |

> On SQL Server, use `range` only on a build with the T‑SQL `TOP` fix; older builds emit `LIMIT`
> and crash — the stopgap is `SPLIT_STRATEGY=modulo`. See [Troubleshooting](#11-troubleshooting).

**Split balance (`SPLIT_BALANCE`):** `span` (default) sizes `range`/`date` splits by equal
key/time width; `count` sizes them by equal rows per split, cutting at quantile boundaries so a
skewed key does not produce one giant split. Boundaries come from the source's statistics
histogram on SQL Server / PostgreSQL (a zero-scan metadata read, `SPLIT_USE_STATS_HISTOGRAM=1`)
or `NTILE`, optionally capped by `SPLIT_SAMPLE_ROWS`. Strategy, balance, and target rows are all
overridable per table.

**Caching & pinning:** in‑memory metadata/Parquet cache (always on); optional disk cache
(`PARQUET_DISK_CACHE=1`, dir `./.parquet_cache`) for warm restarts; **split pinning**
(`PIN_MATERIALIZED_SPLITS=1`, default on) keeps snapshot data files byte‑stable to prevent
size‑drift read errors. Streaming materialization (`STREAMING_PARQUET`, `STREAM_BATCH_ROWS`)
bounds memory for huge tables. See [README.md](README.md) and
[docs/PLANNING.md](docs/PLANNING.md).

---

## 10. Scale & cluster

**How does it scale out?** A **Manager** control plane supervises **N stateless Agents** behind
a load balancer, each registered via heartbeats to `MANAGER_URL`. Agents serve materialized
Parquet from a **shared artifact store**, so any agent can serve any request. See
[docs/SCALE_ARCHITECTURE_PLAN.md](docs/SCALE_ARCHITECTURE_PLAN.md).

**Gateway vs external LB?** The Manager can front the fleet itself (`ENABLE_GATEWAY=1`), or you
run **nginx** and keep the upstream in sync with the live fleet using the **LB renderer**
sidecar (`python -m enterprise.control.lb_renderer`), which polls `GET /agents`, probes
`/readyz`, renders the upstream, and reloads nginx. See
[docs/EXTERNAL_LB_RUNBOOK.md](docs/EXTERNAL_LB_RUNBOOK.md).

**How do I drain an agent?** `POST /_manager/api/agents/{id}/drain` (with the admin token): the
agent flips `/readyz` to 503, the LB drops it, and after `AGENT_DRAIN_GRACE_SECONDS` (default 15)
it exits with in‑flight requests finished. Rolling restarts recycle the fleet one at a time.

**Shard strategy?** `SHARD_STRATEGY` decides which agent materializes which split: `modulo`
(round‑robin by split index) or `weighted` (size‑weighted balancing using observed split sizes
from the prior run; needs a shared store). `AGENT_ADVERTISE_HOST` is the routable address the LB
dials for multi‑host fleets; `FORWARDED_ALLOW_IPS` makes audit log the real client IP behind the
LB. Manager HA (leader lease over the shared store) is available via `MANAGER_HA=1`.

---

## 11. Troubleshooting

**Agent exits with code 78.** Permanent source‑DB config error (bad credential, unreachable
host, missing table). The Manager deliberately does **not** restart it (avoids a crash loop);
the control plane + `/_config` stay up so you can fix the connection, then restart. See
[docs/LINUX_MANAGER_TROUBLESHOOTING.md](docs/LINUX_MANAGER_TROUBLESHOOTING.md).

**`libodbc.so.2: cannot open shared object file` (Linux).** Install the ODBC driver:
```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
curl -sSL https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null
sudo apt-get update && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
odbcinst -q -d      # should list: [ODBC Driver 18 for SQL Server]
```

**`no encryption backend available`.** The Linux credential store needs `cryptography`. Install
into the **venv the service actually runs** (confirm with `pgrep -af enterprise.manager`):
`pip install -e '.[credentials]'`. Windows uses DPAPI (no extra).

**SQL Server crash‑loop: `Incorrect syntax near 'LIMIT'`.** T‑SQL has no `LIMIT`; the `range`
strategy must emit `TOP`. Stopgap without a deploy: `SPLIT_STRATEGY=modulo` and restart.

**Agents show `registered:false` right after start.** Expected cold‑start: with `AUTO_REFRESH`,
each agent materializes every configured table **before** it registers with the Manager, so the
fleet briefly shows `agents_registered:0` and empty `serving_tables`. It flips to registered
once the cold pass finishes (watch for `agent_registered_ok`). Worry only if you see
`source_connect_failed` + exit 78, or repeated `agent_register_failed`.

**`refresh_probe_unavailable table=…` keeps repeating.** The cheap change probe returned nothing
and full‑pull is off. Set `refresh_allow_full_pull: true` (or `refresh_strategy: content_hash`)
and restart. See [Data freshness](#8-data-freshness--auto_refresh).

**Timezone / timestamp read errors.** Keep `TIMESTAMP_ASSUME_UTC=1` (default) so naive datetimes
map to `timestamptz` (Fabric's SQL endpoint rejects `TIMESTAMP_NTZ`). On Windows, `tzdata` ships
as a dependency — re‑run the installer if the venv build was interrupted.

**`no integer split key found`.** The proxy needs an integer split key. Set `KEY_COLUMN` to an
integer column, define a PK on the source, or provide an explicit `schema` with a `key_column`
(always set `KEY_COLUMN` for views).

**Parquet reads fail with size mismatch / `READ_EXCEPTION`.** Size drift — keep
`PIN_MATERIALIZED_SPLITS=1` (default) so materialized data files stay byte‑stable; content
changes get new split names rather than mutating an existing file.

**venv / pip errors on a minimal image.** `sudo apt-get install -y python3-venv python3-pip`,
then rebuild: `rm -rf .venv && bash Manager.sh --recreate --no-pull`.

**Deployed commit didn't advance.** The unit uses `--no-pull`, or local edits block the
fast‑forward. Push to `origin/main`, resolve local changes (or `--auto-stash`), and restart. A
restart that comes up with default POC config (sqlite / iceberg / 1 agent / `127.0.0.1`) means
the `EnvironmentFile` isn't carrying the production settings.

**Wrong client IP in audit logs.** Set `FORWARDED_ALLOW_IPS=<LB-IP-or-CIDR>`.

**Config API returned redacted values / how do I confirm secrets aren't leaking?** Secret
settings (DB URL, S3 secret, TLS key, Manager password) are redacted in `/_config/api/current`
and `/_config/api/settings` — both `value` and `default` come back blank/`***set***`, never the
real secret. Follow logs with `journalctl -u fabric-shortcut-proxy.service -f`.

---

## 12. Roadmap & known limitations

**Delivered:** single‑node Lite proxy; Manager/Agent cluster; Iceberg **and** Delta output;
storage proxy (local/S3/Azure) with per‑key ACLs, credential mediation, and audit; SigV4;
multi‑table; content‑addressed refresh (`AUTO_REFRESH`); disk cache; snapshot history;
config‑builder + monitor UIs; Oracle & Databricks (limited); TLS at the proxy or a fronting LB;
`MANAGER_AUTH` gate. See [docs/PLANNING.md](docs/PLANNING.md),
[docs/Roadmap.md](docs/Roadmap.md), [docs/CHANGELOG.md](docs/CHANGELOG.md).

**In progress / planned:** split‑planner enhancements (row‑target sizing, richer range/date/auto
cascades); a zero‑dependency **C++ serving agent** (`agent-cpp/`); further control‑plane
hardening and Manager HA.

**Known limitations:**

| Limitation | Note |
|---|---|
| `modulo` splits full‑scan the table | use `range`/`date`/`auto` on an integer/date key for index pruning |
| Freshness is poll‑bounded | changes appear after the poll interval + Fabric sync lag; not CDC |
| Oracle / Databricks are capability‑gated | sync‑driver fallback; some features limited |
| Read‑only | no `PUT`/`DELETE`; the proxy is a read‑path gateway by design |
| Single Manager by default | HA via `MANAGER_HA` leader lease; full multi‑Manager Raft is deferred |
| Native end‑to‑end TLS in the cluster | internal hops are HTTP by design — front with nginx TLS for production |

---

## 13. Where to go next

- **Overview & quick start:** [README.md](README.md)
- **All settings:** [docs/CONFIGURATION.md](docs/CONFIGURATION.md)
- **Security (auth/TLS/audit/credentials):** [docs/SECURITY.md](docs/SECURITY.md)
- **Delta output:** [docs/DELTA_FORMAT.md](docs/DELTA_FORMAT.md)
- **Freshness design:** [docs/FRESHNESS_PLAN.md](docs/FRESHNESS_PLAN.md)
- **Scale architecture:** [docs/SCALE_ARCHITECTURE_PLAN.md](docs/SCALE_ARCHITECTURE_PLAN.md)
- **Fabric connectivity patterns:** [docs/UsecasesAndScenarios.md](docs/UsecasesAndScenarios.md)
- **Component architecture:** [docs/TechnicalArchitecture.md](docs/TechnicalArchitecture.md)
- **Linux install:** [docs/installation/Linux_Deployment.md](docs/installation/Linux_Deployment.md)
- **Windows install:** [docs/installation/Windows_Deployment.md](docs/installation/Windows_Deployment.md)
- **Public TLS (nginx):** [SSL_Deployment.md](SSL_Deployment.md)
- **External LB runbook:** [docs/EXTERNAL_LB_RUNBOOK.md](docs/EXTERNAL_LB_RUNBOOK.md)
- **Linux troubleshooting:** [docs/LINUX_MANAGER_TROUBLESHOOTING.md](docs/LINUX_MANAGER_TROUBLESHOOTING.md)
- **Oracle / Databricks runbook:** [docs/ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](docs/ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md)
- **Positioning FAQ:** [docs/FAQ.md](docs/FAQ.md)
