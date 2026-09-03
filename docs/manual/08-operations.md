# Chapter 8: Operations

This chapter covers running the service day to day: starting it, the operational endpoints,
monitoring, managing freshness, scaling the fleet, and troubleshooting. It builds on the
Manager/Agent model from chapter 3.

## 8.1 Running the service

- **Lite:** `python main.py` runs a single agent on port 9000.
- **Enterprise:** `Manager.ps1` / `Manager.sh` start the Manager (control plane, port 9200)
  and one or more supervised agents (data plane, port 9000 and up). See chapter 4 for the
  launcher flags.

Run under a dedicated low-privilege account. On Linux, wrap the launcher in a systemd unit
(see [installation/Linux_Deployment.md](../installation/Linux_Deployment.md)); on Windows,
run it as a service under a gMSA or local service account
([installation/Windows_Deployment.md](../installation/Windows_Deployment.md)).

### AKS stop/start and endpoint durability

Stopping an AKS cluster stops the nodes and causes a service outage. Starting the same cluster
does not normally delete Kubernetes Services or their Azure LoadBalancer frontends, so the
internal Agent endpoint should return after the Agent pods become ready. During startup, wait
for `/readyz` and Service endpoints before testing Fabric.

Deleting and recreating the `LoadBalancer` Service is different. Azure can assign a new private
frontend IP, leaving a manually configured gateway or DNS record pointed at the old address.
Use a private DNS hostname for the Fabric endpoint, record the Service `EXTERNAL-IP` after every
Service change, and reserve or pin the frontend IP when the deployment requires a fixed address.
Never use a pod IP as the gateway endpoint.

After a cluster restart or Service change, verify:

```bash
kubectl -n fabric-shortcut-proxy get svc fsp-materializer-internal -o wide
kubectl -n fabric-shortcut-proxy get endpointslice \
  -l kubernetes.io/service-name=fsp-materializer-internal -o wide
curl -fsS http://<agent-private-fqdn>:9000/healthz
```

## 8.2 Operational endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/healthz` | GET | Liveness; 200 while the process is up |
| `/readyz` | GET | Readiness; 200 when the snapshot is built and the source DB is reachable, else 503 |
| `/metrics` | GET | Prometheus text exposition: request counts, bytes served, cache hit/miss, SQL latency |
| `/_admin/stats` | GET | JSON metrics snapshot plus cache occupancy |
| `/_admin/timeline?table=` | GET | Per-table Fabric read timeline: proxy time vs Fabric-side gaps |
| `/_admin/trace?table=&kind=&status=` | GET | Raw request log, newest first (for example `?status=404`) |
| `/_admin/trace/reset` | POST | Clear the trace buffer before a fresh Fabric run |
| `/_admin/objects?table=` | GET | Declared vs cached size per served object (pinpoints size-drift 404s) |
| `/_admin/schemas?table=` | GET | Resolved logical types per column, with risky-type flags |
| `/_admin/refresh` | POST | Re-read the source and publish a new snapshot |
| `/_admin/gc` | POST | Run retention garbage collection now |
| `/_admin/publish-image` | POST | Publish a complete serving image (data + metadata) |
| `/_config/api/open-mirror/health` | POST | Run read-only dependency checks for an edited Open Mirror target |
| `/_config/api/open-mirror/publish` | POST | Start an on-demand Open Mirroring background job |
| `/_config/api/open-mirror/publish/jobs/latest` | GET | Read the latest Open Mirroring job and per-target status |
| `/_config/api/backup` | POST | Download a password-encrypted portable backup |
| `/_config/api/restore` | POST | Restore a multipart backup upload; restart required |

The config builder (`/_config`) and the monitor (`/_monitor`) are served on the Manager
control plane, or on the agent's own port in Lite mode. In the enterprise edition the admin
console is at `/_manager`.

## 8.3 Monitoring

Enable the read-only monitor with `ENABLE_MONITOR=1`. In Enterprise, open the
authenticated `/_manager` console and select **Monitor**. Lite mode opens
`/_monitor/`. The dashboard shows
per-table read and request stats, Fabric-side gaps, per-request query lag (Fabric → SQL →
Parquet → out), current snapshot and version per table, cache occupancy including pinned
splits, process and SQL metrics, active cluster alerts, and health history over a
selectable 1-hour, 5-hour, or 24-hour window.

For a metrics pipeline, scrape `/metrics`. The request timeline (`REQUEST_TRACE=1`, on by
default) powers `/_admin/timeline` and the monitor. Use `/_admin/objects` to diagnose
size-drift 404s and `/_admin/schemas` to check resolved column types before wiring a
shortcut.

## 8.4 Managing freshness

Auto-refresh is off by default. Turn it on and pick a strategy (chapter 2):

```powershell
$env:AUTO_REFRESH = "1"
$env:REFRESH_STRATEGY = "auto"        # auto | dialect_probe | content_hash | ttl | manual
$env:REFRESH_POLL_SECONDS = "600"
```

- `auto` uses a cheap dialect change probe and falls back to a full content hash when
  `REFRESH_ALLOW_FULL_PULL=1`.
- `dialect_probe` uses only the dialect-specific change probe.
- `content_hash` publishes only when the materialized content hash changes.
- `ttl` refreshes on a fixed interval.
- `manual` publishes only on `POST /_admin/refresh`.

To publish on demand instead of on a timer, `POST /_admin/refresh`. Note that random
tokenization is incompatible with content-based refresh and the proxy rejects that
combination at startup.

## 8.5 Open Mirror publishing

The Manager publishes targets from `config.open_mirror.json` on a schedule when
`OPEN_MIRROR_PUBLISH=1`. The interval is controlled by `OPEN_MIRROR_INTERVAL_SECONDS`; a
target can also be published on demand from the Config Builder with **Publish now** or **Dry run**.
On-demand requests return a job id immediately. The UI polls that job and shows per-target and
per-table status, so navigation does not interrupt publishing. Only one on-demand job runs at a
time, and the latest status remains queryable while the Manager process remains up.

Use **Check health** before the first publish or after changing a source credential, schema, or
Fabric target. It checks the target currently shown in the editor, even before it is saved, and
reports configuration, Manager credential freshness, source query access, table key/watermark
columns, Fabric mirroring status, and landing-zone listing access independently. The check is
read-only: it does not start Fabric mirroring, read source rows, write OneLake files, or advance
publication state. A warning identifies a condition that may need attention, such as a saved
credential awaiting Manager restart, a nullable or string watermark, or a mirror that is not
`Running`; an error or blocked result must be resolved before publishing.

Before reading a OneLake target's source tables, the Manager checks the mirrored database status.
When `self_healing` is enabled, it calls Fabric's mirroring start operation for `Initialized`,
`Paused`, or `Stopped`, then waits for `Running` within
`OPEN_MIRROR_PREFLIGHT_TIMEOUT_SECONDS`. `OPEN_MIRROR_START_COOLDOWN_SECONDS` prevents repeated
start attempts for the same target. This operation does not start or resume Fabric capacity.

Use watermark tracking for source-incremental upserts. Use snapshot tracking when delete detection
is required; it is a full-source scan. The state directory contains the committed cursor and
pending-file recovery metadata. Back it up with the landing-zone file indexes.

To force an operator-approved full load for one table, call:

```bash
curl -X POST http://localhost:9200/_config/api/open-mirror/reset \
  -H 'Content-Type: application/json' \
  -d '{"target_id":"fabric-sales","table":"sales","confirm":true}'
```

The reset endpoint requires all three fields. It preserves invalid state until this explicit
action, so a corrupt or unreadable state file never becomes an accidental full load.

If no data reaches Fabric, check the cycle in this order:

1. `open_mirror_cycle` with `replication_unavailable=1` means Fabric preflight failed before a
  source query. Check the Manager identity's Fabric permissions and mirrored database status.
2. `open_mirror_table_failed` with `pages_read=0` means source reflection or the first query
  failed. The message includes the final database-driver error after retries.
3. A missing table state file means no batch committed. A state file whose
  `published_rows_total` increases confirms that OneLake writes completed and were verified.
4. Large first pages can exceed driver timeouts or delay shutdown. Set `OPEN_MIRROR_MAX_ROWS` to
  a bounded value and use a non-null, monotonic watermark supported by a source index.

Restart the Manager after changing a saved source credential. Restarting only supervised Agents
does not refresh the Manager process that owns scheduled and on-demand Open Mirror jobs.

For tokenized Open Mirror targets, use the dedicated
[Open Mirror tokenization UAT](../TOKENIZATION_OPEN_MIRROR_UAT.md) before production use.
It covers deterministic tokens, omitted watermark controls, encrypted state, random-token
pending recovery, and key rotation.

## 8.6 Caching

Generated Parquet is cached in memory, and optionally on disk for warm restarts:

```powershell
$env:PARQUET_DISK_CACHE = "1"
$env:PARQUET_DISK_CACHE_DIR = "./.parquet_cache"
```

Snapshot splits are pinned (`PIN_MATERIALIZED_SPLITS=1`, default) so their data files stay
byte-identical while the snapshot is current, which prevents size-drift read failures. Cache
sizing and TTLs (`parquet_cache_max_bytes`, `metadata_cache_ttl`, `parquet_cache_ttl`) are
live settings that can change without a restart.

## 8.7 Scaling the fleet

The enterprise edition scales horizontally by running more agents behind the Manager.

- Set the agent count with `-AgentCount <n>` (or `AGENT_COUNT`). Agents are interchangeable
  because they serve from the shared artifact store.
- Enable the built-in round-robin gateway with `-Gateway` to front the fleet behind one
  Fabric-facing endpoint. Point the shortcut at the gateway.
- For production, you can front the agents with an external L7 load balancer instead; see
  [EXTERNAL_LB_RUNBOOK.md](../EXTERNAL_LB_RUNBOOK.md).
- Size-weighted split assignment (`shard_strategy=weighted`) balances large tables across
  agents.

Each agent idles near 300 MB, alerts at 800 MB, and auto-restarts at 1200 MB by default. The
supervisor restarts crashed agents with backoff and will not crash-loop on a permanent
configuration error; it emits a clear, redacted message instead. The scale design is in
[SCALE_ARCHITECTURE_PLAN.md](../SCALE_ARCHITECTURE_PLAN.md).

## 8.8 Materialization and data size

Understanding when data is read matters most for large tables. By default
(`MATERIALIZE_MODE=eager`) the proxy materializes each split at startup: it runs the split's
SQL and encodes the result as Parquet before serving begins. Iceberg and Delta manifests must
declare accurate row counts and file sizes; readers, including OneLake's Iceberg-to-Delta
virtualization, use the declared size to locate the Parquet footer. The `lazy` and `virtual`
modes (below) defer this per table instead of doing it all at startup.

What this means for, say, a 1 TB source table:

- **The first cold start reads the whole table once** and writes it as Parquet to the durable
  store. It is not re-queried from the database on every Fabric read; Fabric streams the
  already-materialized bytes on demand (ranged reads).
- **The stored footprint is Parquet-encoded, not raw 1 TB.** Parquet is columnar and
  compressed, so the on-disk size is materially smaller than the raw source, though still
  proportional to it.
- **Memory stays bounded.** With `STREAMING_PARQUET=1` each split materializes in row batches
  (`STREAM_BATCH_ROWS`, default 50,000) instead of loading whole into RAM; concurrency is
  capped by `MAX_CONCURRENT_GENERATIONS`; the in-memory Parquet cache is capped by
  `PARQUET_CACHE_MAX_BYTES`. Durable bytes live on disk in the artifact store, not RAM.
- **Warm restarts skip regeneration.** With `PIN_MATERIALIZED_SPLITS=1` (default) and
  artifact-store serving, a restart serves the materialized splits from disk with no SQL and
  no re-read of the source.
- **A cluster divides the seed.** Each agent materializes only its shard of splits
  (`AGENT_SHARD_COUNT`); `shard_strategy=weighted` balances by bytes, not just count. So N
  agents each seed roughly `1 TB / N`, and non-owner agents wait for the owning shard to
  publish rather than re-reading the source.

Sizing guidance for large tables:

- Budget artifact-store or disk-cache space near the Parquet-encoded size, and keep
  `PIN_MATERIALIZED_SPLITS=1` with artifact-store serving so restarts do not re-read.
- Enable `STREAMING_PARQUET=1` to bound memory during the initial materialization.
- Keep range split planning (default) so each split scans only its key slice instead of the
  full table, and shard across agents to parallelize the seed.
- For a skewed key, set `split_balance=count` so each split holds ~equal rows (no straggler
  split). On SQL Server / PostgreSQL the boundaries come from the optimizer's statistics
  histogram with no data scan; elsewhere from `NTILE`, optionally capped by `split_sample_rows`.
- In `eager` mode expect a real cold-start cost. To avoid seeding tables no one reads, use
  `lazy` or `virtual` (below).
- Prefer a cheap change probe (`REFRESH_STRATEGY=auto` or `dialect_probe`) or `ttl` over
  `content_hash` for auto-refresh, because content hashing re-materializes the table to
  detect change.

### Materialization modes (`MATERIALIZE_MODE`)

`MATERIALIZE_MODE` chooses **when** splits are built. It is restart-required.

- **`eager`** (default): build and pin every split at startup. Lowest read latency; the cold
  start reads the whole table; best for live/mutating sources.
- **`lazy`**: build a table's splits on the first read of that table, then pin them. Unread
  tables cost nothing at startup, so it suits large catalogs where consumers read a subset.
  Works for Iceberg and Delta; a multi-agent fleet needs a shared artifact store so shards
  serve byte-identical splits. The C++ serving agent participates via the Manager: on a store
  miss it asks the Manager (`POST /control/materialize`) to materialize the table into the
  shared store, then serves it.
- **`virtual`**: build a table's splits on first read to learn their sizes, then keep **zero
  bytes at rest** — each split is regenerated deterministically on demand. It suits
  **immutable / append-only / snapshot-isolated** sources where the rows are stable between
  reads (Parquet output is byte-reproducible for a fixed library version). A determinism
  self-check fails closed if a split does not regenerate byte-identically, so a mutating
  source is caught rather than served with drift. Because regeneration is deterministic,
  multi-agent `virtual` is consistent without a shared store.

`lazy` and `virtual` are incompatible with auto-refresh and serving-image publishing. Set the
mode in the config builder (Global Settings) or `config.performance.json`.

## 8.9 High availability

Enable Manager HA with `-Ha` (leader lease over the shared artifact store). Only the primary
Manager supervises agents; a standby is a warm spare and reports as ready. The gateway on a
standby naturally returns 503 because no agents register to it. Roll agents with health-gated
restarts (`rolling_restart_health_timeout`).

## 8.10 Retention

Enable the retention garbage collector with `-RetentionGc` to prune orphaned Parquet splits
on a timer (`retention_gc_interval_seconds`), or trigger it once with `POST /_admin/gc`.

## 8.11 Troubleshooting

| Symptom | Likely cause | First check |
|---|---|---|
| `/readyz` returns 503 | Source unreachable or snapshot not built | Connection string, driver, DB reachability, startup logs |
| Fabric shows a 403 | SigV4 mismatch or unauthorized key | Access key, secret, per-key ACL for the bucket/prefix |
| Fabric read fails with a size mismatch | Split size drift | `/_admin/objects?table=`; keep `PIN_MATERIALIZED_SPLITS=1` |
| A table refuses to start | A `total_rows > num_splits × QUERY_MAX_ROWS` bound, or a validation error | Startup log; raise `num_splits` or `QUERY_MAX_ROWS` |
| Change-probe-unavailable warning | Source has no cheap probe | Set `REFRESH_ALLOW_FULL_PULL=1` or use `ttl` |
| A view fails to plan | No primary key detected | Set `key_column` for the view |
| Tokenized table fails at startup | Missing key, unsupported dialect, or refresh conflict | Read the redacted error; see chapter 7 |
| Open Mirror table stops after a restart | State is missing, corrupt, unreadable, or incompatible | Inspect `OPEN_MIRROR_STATE_DIR`; use the explicit reset endpoint only after review |
| Open Mirror preflight does not start mirroring | Target is not a OneLake target, self-healing is disabled, or Fabric identifiers are missing | Check `landing_zone_root`, `workspace_id`, `mirrored_database_id`, and `self_healing` |

## 8.12 Backup and restore

Create and restore portable `.fspbackup` archives in the Config Builder **Security** area. The
archive includes split configuration, local credential-store records, scoped access keys, and
Open Mirroring state. It does not include source data, generated artifacts or caches, logs,
environment-only secrets, external TLS files, or remote Key Vault contents.

Before restore, take a destination backup and stop Open Mirroring publishing. Upload the archive,
enter its password, and review the returned counts. Restore is transactional, but the running
process does not reload every restored setting. Restart the Manager, then verify sources, table
mappings, access-key scopes, and mirror status before resuming publishing. The complete procedure
and API examples are in [BACKUP_RESTORE.md](../BACKUP_RESTORE.md).

Launcher and host issues are covered in
[LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) and the deployment
guides. Oracle and Databricks operational specifics are in
[ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](../ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md).

## 8.13 Next

Continue to [Chapter 9: Reference](09-reference.md) for the settings groups, dialect matrix,
path formats, and launcher flags.
