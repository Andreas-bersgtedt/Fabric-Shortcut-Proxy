# SQL Iceberg Proxy — Hardening & Enhancement Plan

Status: **POC complete and verified end-to-end against Microsoft Fabric** (S3‑compatible
shortcut → Iceberg → Delta virtualization; 50k rows served from SQL pushdown).
This document plans the work to take the POC from "works in a demo" toward
"robust and extensible."

Companion documents:
- Design & requirements: [s3virtulization.md](s3virtulization.md)
- Scale & robustness (Manager/Agent cluster rewrite): [SCALE_ARCHITECTURE_PLAN.md](SCALE_ARCHITECTURE_PLAN.md)
- Verified engineering notes: repo memory (`/memories/repo/s3-keycount-fix.md`)

---

## 1. Guiding principles

1. **Fabric is the source of truth.** Every change is validated against the real
   Fabric reader and the reference reader (`validate_pyiceberg.py`), because
   `pyiceberg` passing is necessary but *not sufficient* (OneLake uses Apache
   XTable = Iceberg **Java**, which is stricter).
2. **Determinism.** A given virtual file key maps to stable bytes for a snapshot,
   and identifiers are restart‑stable.
3. **Fail fast, loudly.** Broken metadata references and schema drift should
   surface as clear errors, not silent fallbacks.
4. **Feature‑flag risky behavior.** Anything that could break the working Fabric
   path (e.g. SigV4 enforcement) ships behind a config flag, defaulting to the
   known‑good POC behavior.
5. **Every change lands with a test.**

---

## 2. Current state snapshot

| Area | State |
|---|---|
| S3 API (GET/HEAD/List, range + suffix range) | ✅ |
| Iceberg metadata (metadata.json, manifest‑list, manifest) | ✅ spec‑compliant |
| SQL pushdown planner + async execution | ✅ |
| Parquet generation (field‑ids, accurate sizes) | ✅ |
| Deterministic, restart‑stable snapshot ids | ✅ |
| LRU cache (metadata + Parquet) | ✅ |
| Structured logging + quiet expected‑404s | ✅ |
| Metrics / health endpoints | ✅ (H1/H2 — `/metrics`, `/_admin/stats`, `/healthz`, `/readyz`) |
| Auth (SigV4 verification) | ✅ (H3 — behind `REQUIRE_SIGV4`, default off) |
| Resource guards for large scans | ✅ (H4 — concurrency semaphore + `QUERY_MAX_ROWS`) |
| Retry / resilience | ✅ (H5 — backoff retries → 503) |
| Schema‑drift handling | ✅ (H6 — startup source-column validation) |
| Pluggable SQL dialects (SQLite/Postgres/SQL Server) | ✅ (F6 — `planner/dialects.py`) |
| Multi‑table (N tables from one proxy) | ✅ (F1 — `TABLES` registry + per‑table snapshots) |
| Manifest column stats (prune-enabling) | ✅ (F3 — `ICEBERG_MANIFEST_STATS`, default off) |
| Persistent disk Parquet cache | ✅ (F5 — `PARQUET_DISK_CACHE`, warm-restart skip regen) |
| Concurrent startup materialization | ✅ (F4 — bounded parallel warm) |
| Snapshot history / time‑travel | ✅ (F2 — `ICEBERG_SNAPSHOT_HISTORY`, default off) |
| CI | ✅ (H9 — GitHub Actions: pytest matrix + pyiceberg smoke) |
| Config validation / secret redaction | ✅ (H7) |
| Split pinning (byte-stable data files) | ✅ (`PIN_MATERIALIZED_SPLITS`, default on — prevents size-drift 404s) |
| Data freshness (content-addressed refresh) | ✅ (`AUTO_REFRESH` + background poller, default off) |
| Native Delta output | ✅ (`TABLE_FORMAT=delta` — `_delta_log/`, no Iceberg→Delta conversion; see [DELTA_FORMAT.md](DELTA_FORMAT.md)) |
| Request tracing / Fabric timeline | ✅ (`REQUEST_TRACE`, default on — `/_admin/timeline`, `/trace`, `/objects`, `/schemas`) |
| Monitoring dashboard | ✅ (`ENABLE_MONITOR`, default off — `/_monitor/` read/query stats + query lag) |

---

## 3. Hardening workstream (production‑readiness gaps)

Effort key: **S** ≈ hours · **M** ≈ 1–2 days · **L** ≈ 3+ days. Risk = chance of
regressing the working Fabric path.

### H1 — Metrics & diagnostics  ·  effort M · risk low  ·  (§4.8)  ·  ✅ DONE
- **Goal:** expose metadata‑request count, data‑file‑request count, SQL latency,
  bytes served, and cache hit ratio.
- **Delivered:** [observability/metrics.py](../observability/metrics.py) registry;
  `GET /metrics` (Prometheus) + `GET /_admin/stats` (JSON) in
  [observability/endpoints.py](../observability/endpoints.py); instrumented
  [s3/router.py](../s3/router.py) (request counts + bytes served),
  [db/executor.py](../db/executor.py) (SQL latency histogram), and
  [cache/lru_cache.py](../cache/lru_cache.py) (hit/miss + occupancy).
- **Acceptance:** ✅ `/metrics` returns non‑zero counters after traffic; cache hit
  ratio rises on repeated reads. Covered by `tests/test_metrics_health.py`.

### H2 — Health & readiness endpoints  ·  effort S · risk low  ·  ✅ DONE
- **Goal:** `/healthz` (liveness) and `/readyz` (readiness = snapshot built **and**
  source DB reachable).
- **Delivered:** routes in [observability/endpoints.py](../observability/endpoints.py)
  mounted before the S3 catch‑all; readiness runs `SELECT 1` via
  [db/executor.py](../db/executor.py) `ping()`.
- **Acceptance:** ✅ `/healthz` → 200 while up; `/readyz` → 200 once ready, 503
  otherwise. Covered by `tests/test_metrics_health.py`.

### H3 — SigV4 authentication verification  ·  effort L · risk med  ·  ✅ DONE
- **Delivered:** [s3/auth.py](../s3/auth.py) verifies `AWS4-HMAC-SHA256` header auth
  (canonical request → string-to-sign → HMAC signing-key chain), and a middleware
  in [main.py](../main.py) enforces it when `REQUIRE_SIGV4=1` (default off); health/
  metrics/admin endpoints and CORS preflight are exempt. Failures return
  `403` with `AccessDenied` / `InvalidAccessKeyId` / `SignatureDoesNotMatch`.
- **Acceptance:** ✅ tests sign with **botocore** (the real S3 signer) and assert
  accept/reject; verified live: unsigned → 403, `/healthz` → 200, signed → 200.
  Default-off keeps the known-good Fabric path unchanged.

### H4 — Resource guards for on‑demand scans  ·  effort M · risk med  ·  ✅ DONE
- **Delivered:** an `asyncio.Semaphore(MAX_CONCURRENT_GENERATIONS)` in
  [s3/router.py](../s3/router.py) bounds simultaneous SQL+Parquet builds;
  `QUERY_MAX_ROWS` caps rows per split; `parquet_generations_total` metric added.

### H5 — Retry / resilience policy  ·  effort S–M · risk low  ·  ✅ DONE
- **Delivered:** [db/executor.py](../db/executor.py) retries with linear backoff
  (`DB_MAX_RETRIES` / `DB_RETRY_BACKOFF`) and raises `SourceUnavailable` on
  exhaustion; [s3/router.py](../s3/router.py) maps it to `503 ServiceUnavailable`
  with `Retry-After`.

### H6 — Schema‑drift detection  ·  effort M · risk low  ·  ✅ DONE
- **Delivered:** [db/executor.py](../db/executor.py) `validate_source_schema()`
  introspects the source table at startup and fails fast if any declared
  `TABLE_SCHEMA` column is missing (extra columns are logged, not fatal). Gated
  by `VALIDATE_SOURCE_SCHEMA` (default on).

### H7 — Config & secrets hygiene  ·  effort S · risk low  ·  ✅ DONE
- **Goal:** all secrets from env/secret store; validate required config at startup.
- **Delivered:** [config.py](../config.py) `validate_config()` (fails fast with an
  aggregated message) called from the [main.py](../main.py) lifespan; `redact_db_url()`
  masks credentials in the startup log. Required env documented in the README.
- **Acceptance:** ✅ missing/invalid config fails fast; DB URL passwords never logged.
  Covered by `tests/test_hardening.py`.

### H8 — S3 error‑response fidelity  ·  effort S · risk low  ·  ✅ DONE
- **Delivered:** [s3/xml_responses.py](../s3/xml_responses.py) error bodies now carry
  a unique `RequestId` + `HostId`; [s3/router.py](../s3/router.py) returns
  `416 Range Not Satisfiable` (with `Content-Range: bytes */<size>`) for ranges
  that start beyond the object, matching AWS. Suffix/footer ranges unaffected.

### H9 — Test coverage & CI  ·  effort M · risk low  ·  ✅ DONE
- **Goal:** lock in the hard‑won correctness so it can't regress.
- **Delivered:** `tests/test_hardening.py` covers suffix/range reads,
  deterministic snapshot stability, required v2 metadata fields, and config
  validation/redaction; `tests/test_metrics_health.py` covers H1/H2.
  `.github/workflows/ci.yml` runs a `pytest` matrix (3.11/3.12) plus a
  `fabric-compat` job that boots the proxy and runs `validate_pyiceberg.py`.
- **Acceptance:** ✅ 37 tests green locally; CI runs on push/PR. `pyiceberg`
  reference validation guards Fabric compatibility.

---

## 4. Future‑enhancement workstream (§10)

### F1 — Multi‑table support  ·  effort L · risk med  ·  (§10.1)  ·  ✅ DONE
- **Goal:** serve N tables under `warehouse/db/<table>` from one proxy.
- **Delivered:** [config.py](../config.py) now exposes a `TableDef` dataclass and a
  `TABLES` registry (default = the single demo `sales` table, preserving
  behavior). [iceberg/state_store.py](../iceberg/state_store.py) holds a
  `dict[str, SnapshotState]` registry with `build_table_snapshot` /
  `build_all_snapshots` / `get_all_snapshots`; `SnapshotState` and
  `SplitDescriptor` carry a back‑reference to their `TableDef`.
  [iceberg/metadata.py](../iceberg/metadata.py) and
  [parquet/generator.py](../parquet/generator.py) read `snap.table.schema` instead of
  the global; [s3/router.py](../s3/router.py) aggregates objects across all snapshots
  and resolves the owning snapshot per key; [main.py](../main.py) builds and
  materializes every table (and validates each source schema). `build_snapshot()`
  is kept as a backward‑compatible single‑table wrapper.
- **Acceptance:** ✅ `tests/test_multitable.py` registers `sales` + `products`
  and asserts both tables' metadata, schema isolation, and Parquet rows are served
  independently. All 66 tests green.

### F2 — Snapshot history & time‑travel  ·  effort L · risk med  ·  (§10.2)  ·  ✅ DONE
- **Goal:** multiple snapshots with populated `snapshot-log` / `metadata-log`;
  serve `vN.metadata.json` history; `version-hint.text` > 1.
- **Delivered:** [iceberg/state_store.py](../iceberg/state_store.py) keeps a per-table
  history (`_history`) plus `advance_table_snapshot()`; each version gets its own
  snapshot id / sequence / watermark / versioned manifests / `vN.metadata.json`
  while sharing the deterministic data files. [iceberg/metadata.py](../iceberg/metadata.py)
  renders each version **point-in-time** (snapshots up to that version, a
  `snapshot-log`, and a `metadata-log` of earlier metadata files);
  [s3/router.py](../s3/router.py) serves all historical metadata/manifests and
  reports `version-hint.text` = the current version; `/_admin/refresh` advances
  the version when history is on. Gated by `ICEBERG_SNAPSHOT_HISTORY` (default off).
- **Acceptance:** ✅ `tests/test_phase5.py::test_snapshot_history_and_time_travel`;
  verified live with **pyiceberg**: reading `v1.metadata.json` returns the
  point-in-time view (1 snapshot) and `v3.metadata.json` the current view (3
  snapshots), both scanning all 50000 rows. Data is shared across versions (POC
  limitation — rows don't change between snapshots).

### F3 — Manifest statistics & predicate pushdown  ·  effort L · risk med  ·  (§10.3)  ·  ✅ DONE
- **Goal:** populate column stats (sizes, value/null counts, lower/upper bounds)
  to enable split pruning.
- **Delivered:** [iceberg/stats.py](../iceberg/stats.py) computes per-column stats
  from each generated Parquet file's row-group metadata and encodes bounds with
  Iceberg single-value binary serialization; [iceberg/manifest.py](../iceberg/manifest.py)
  emits the six stat maps as Iceberg array-of-key/value maps **with a `field-id`
  on every key and value** (the missing field-ids were exactly what strict
  readers rejected before). Gated by `ICEBERG_MANIFEST_STATS` (default off).
- **Acceptance:** ✅ `tests/test_phase5.py` round-trips the maps via fastavro and
  decodes bounds; verified live that **pyiceberg** still reads all 50000 rows
  with stats enabled. (Server-side pushdown is N/A — S3 GETs carry no predicate;
  the value is giving the reader stats to prune with.)

### F4 — Async pre‑generation of hot Parquet  ·  effort M · risk low  ·  (§10.4)  ·  ✅ DONE
- **Goal:** pre‑warm popular splits to cut first‑read latency.
- **Delivered:** startup materialization in [main.py](../main.py) now runs
  concurrently (bounded by `MAX_CONCURRENT_GENERATIONS` via an asyncio
  semaphore + `asyncio.gather`), gated by `CONCURRENT_STARTUP_MATERIALIZATION`
  (default on). Note: splits are still materialized eagerly (not lazily) because
  the manifest must declare accurate file sizes before it is served, so there is
  no cold data-file read to pre‑warm; this item instead speeds the warm-up itself.
- **Acceptance:** ✅ verified live — server boots and serves all splits; combined
  with F5, warm restarts skip regeneration entirely.

### F5 — Materialized object‑store cache  ·  effort M–L · risk low  ·  (§10.5)  ·  ✅ DONE
- **Goal:** persist generated Parquet (disk) to survive restarts.
- **Delivered:** [cache/lru_cache.py](../cache/lru_cache.py) write-through to a disk
  directory keyed by a hash of the (deterministic) object key, with read-through
  + `warm_parquet()` used by the [main.py](../main.py) lifespan to skip SQL +
  Parquet regeneration on a warm restart. Gated by `PARQUET_DISK_CACHE`
  (default off); atomic writes via a temp file + `os.replace`.
- **Acceptance:** ✅ `tests/test_phase5.py` write → clear memory → read-from-disk
  round-trip; disabled flag writes nothing.

### F6 — Pluggable SQL dialect adapters  ·  effort M · risk med  ·  (§10.6)  ·  ✅ DONE
- **Goal:** first‑class SQLite / PostgreSQL / SQL Server support (`aioodbc` is
  already a dependency).
- **Delivered:** [planner/dialects.py](../planner/dialects.py) provides `Dialect`
  adapters (SQLite, PostgreSQL, SQL Server) that encapsulate identifier quoting
  (double‑quote vs. brackets), the integer CAST type (`INTEGER` vs. `BIGINT`), and
  the row‑limit clause (`LIMIT` suffix vs. T‑SQL `TOP` prefix). `get_dialect()`
  selects from the `config.DB_URL` scheme; [planner/split_planner.py](../planner/split_planner.py)
  is now fully dialect‑driven and uses the split's own `TableDef`.
- **Acceptance:** ✅ `tests/test_dialects.py` asserts the emitted SQL per dialect
  (TOP vs LIMIT, BIGINT vs INTEGER, bracket vs double‑quote, dotted‑name quoting).
  The SQL Server path is exercised end‑to‑end by the existing `mssql+aioodbc` config option.

---

## 5. Prioritization

| Item | Value | Effort | Risk | Suggested order |
|---|---|---|---|---|
| H2 Health/readiness | High | S | Low | 1 |
| H1 Metrics | High | M | Low | 2 |
| H9 Tests + CI | High | M | Low | 3 |
| H7 Config/secrets | Med | S | Low | 4 |
| H8 S3 error fidelity | Med | S | Low | 5 |
| H4 Resource guards | High | M | Med | 6 |
| H5 Retry/resilience | Med | S–M | Low | 7 |
| H6 Schema drift | Med | M | Low | 8 |
| H3 SigV4 (flagged) | Med | L | Med | 9 |
| F6 SQL dialects | High | M | Med | 10 |
| F1 Multi‑table | High | L | Med | 11 |
| F3 Manifest stats/pushdown | Med | L | Med | 12 |
| F4 Async pre‑gen | Med | M | Low | 13 |
| F5 Materialized cache | Med | M–L | Low | 14 |
| F2 Time‑travel | Low | L | Med | 15 |

---

## 6. Phased milestones

**Phase 1 — Operability (demo‑ready, fast wins):** H2, H1, H9, H7.  ·  ✅ DONE
Outcome: the proxy reports health and metrics and is guarded by CI.

**Phase 2 — Robustness:** H8, H4, H5, H6.  ·  ✅ DONE
Outcome: predictable behavior under large scans, transient failures, and schema drift.

**Phase 3 — Security:** H3 (behind `REQUIRE_SIGV4`, validated against Fabric).  ·  ✅ DONE
Outcome: optional real credential enforcement for shared deployments.

**Phase 4 — "Real" capability:** F6 (dialects), then F1 (multi‑table).  ·  ✅ DONE
Outcome: point at a production SQL Server and expose multiple tables.

**Phase 5 — Advanced Iceberg + performance:** F3 (stats/pushdown), F4 (pre‑gen),
F5 (materialized cache), F2 (time‑travel).  ·  ✅ DONE
Outcome: pruning‑aware, low‑latency, restart‑durable, history‑capable.

---

## 7. Cross‑cutting acceptance gate

Every item, before it's considered done:
1. `pytest tests/ -v` green.
2. `validate_pyiceberg.py` still reads all rows.
3. A fresh Fabric shortcut converts (`_delta_log/latest_conversion_log.txt` →
   *Succeeded*) and a `SELECT * … LIMIT 10` returns rows.
4. No new noise in the default log output.
