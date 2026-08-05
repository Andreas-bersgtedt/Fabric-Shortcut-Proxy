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

The config builder (`/_config`) and the monitor (`/_monitor`) are served on the Manager
control plane, or on the agent's own port in Lite mode. In the enterprise edition the admin
console is at `/_manager`.

## 8.3 Monitoring

Enable the read-only monitor with `ENABLE_MONITOR=1` and open `/_monitor/`. It shows
per-table read and request stats, Fabric-side gaps, per-request query lag (Fabric → SQL →
Parquet → out), current snapshot and version per table, cache occupancy including pinned
splits, and process and SQL metrics.

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

## 8.5 Caching

Generated Parquet is cached in memory, and optionally on disk for warm restarts:

```powershell
$env:PARQUET_DISK_CACHE = "1"
$env:PARQUET_DISK_CACHE_DIR = "./.parquet_cache"
```

Snapshot splits are pinned (`PIN_MATERIALIZED_SPLITS=1`, default) so their data files stay
byte-identical while the snapshot is current, which prevents size-drift read failures. Cache
sizing and TTLs (`parquet_cache_max_bytes`, `metadata_cache_ttl`, `parquet_cache_ttl`) are
live settings that can change without a restart.

## 8.6 Scaling the fleet

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

## 8.7 High availability

Enable Manager HA with `-Ha` (leader lease over the shared artifact store). Only the primary
Manager supervises agents; a standby is a warm spare and reports as ready. The gateway on a
standby naturally returns 503 because no agents register to it. Roll agents with health-gated
restarts (`rolling_restart_health_timeout`).

## 8.8 Retention

Enable the retention garbage collector with `-RetentionGc` to prune orphaned Parquet splits
on a timer (`retention_gc_interval_seconds`), or trigger it once with `POST /_admin/gc`.

## 8.9 Troubleshooting

| Symptom | Likely cause | First check |
|---|---|---|
| `/readyz` returns 503 | Source unreachable or snapshot not built | Connection string, driver, DB reachability, startup logs |
| Fabric shows a 403 | SigV4 mismatch or unauthorized key | Access key, secret, per-key ACL for the bucket/prefix |
| Fabric read fails with a size mismatch | Split size drift | `/_admin/objects?table=`; keep `PIN_MATERIALIZED_SPLITS=1` |
| A table refuses to start | A `total_rows > num_splits × QUERY_MAX_ROWS` bound, or a validation error | Startup log; raise `num_splits` or `QUERY_MAX_ROWS` |
| Change-probe-unavailable warning | Source has no cheap probe | Set `REFRESH_ALLOW_FULL_PULL=1` or use `ttl` |
| A view fails to plan | No primary key detected | Set `key_column` for the view |
| Tokenized table fails at startup | Missing key, unsupported dialect, or refresh conflict | Read the redacted error; see chapter 7 |

Launcher and host issues are covered in
[LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) and the deployment
guides. Oracle and Databricks operational specifics are in
[ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md](../ORACLE_DATABRICKS_OPERATOR_RUNBOOK.md).

## 8.10 Next

Continue to [Chapter 9: Reference](09-reference.md) for the settings groups, dialect matrix,
path formats, and launcher flags.
