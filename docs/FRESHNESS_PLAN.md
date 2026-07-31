# Plan: Data Freshness & Chunk Identity

How to make source-data changes visible in Microsoft Fabric through the proxy,
and why the current fixed modulo chunking can never signal a change, **without
modifying the source databases and uniformly across every SQL variant**.

Status: **planning only.** Companion to [PLANNING.md](PLANNING.md).

---

## 1. Problem

1. **Stale reads.** A row is updated in the source SQL. The same Fabric query
   returns the *old* value, because Fabric caches the shortcut data and never
   re-reads.
2. **Fixed chunk identity.** Chunks are `key % N` and their file paths are
   derived from a fixed seed, so a data change alters a chunk's *contents* but
   not its *path*, there is no metadata change to signal freshness.

Both stem from one design property: **snapshot ids and every object key are
deterministic and content-independent.**

---

## 2. Root cause (grounded in code)

In [iceberg/state_store.py](../iceberg/state_store.py) `build_table_snapshot`:

```python
seed = f"{bucket}/{table_path}"          # no data component
snap_id = int(sha256(seed)[:15], 16)     # same forever
object_key = f"{table_path}/data/split-{i}-{snap_id}.parquet"   # same forever
```

Consequences when a source row changes:
- `metadata.json` keeps the **same `current-snapshot-id`**;
- `version-hint.text` stays **`1`**;
- the manifest points at the **same `split-{i}-{snap_id}.parquet`** paths;
- Parquet bytes are **cached** (eager materialization at startup + memory/disk).

To Fabric, the Iceberg table version is unchanged → no re-read.

`advance_table_snapshot` (F2 time-travel) already mints a new snapshot-id /
version / manifest keys, **but shares the data files** (`new.splits =
cur.splits`), so it still doesn't change data-file paths, which is what the
cache layers key on.

---

## 3. Fabric-side caching (what we can and can't control)

Three independent layers cache, none directly pokeable:

| Layer | Keyed on | Invalidated by |
|---|---|---|
| OneLake shortcut cache | file path + last-modified/retention | new path / expiry |
| Iceberg→Delta (XTable) conversion cache | Iceberg snapshot / metadata version | new `current-snapshot-id` + `version-hint` |
| SQL analytics endpoint metadata sync | Delta table version | table-version change (with lag) |

The only signal that reaches **all three** is a **new Iceberg snapshot whose
data files have new paths**. Fabric-side lag (shortcut cache + endpoint sync)
means freshness is *bounded*, never instant.

---

## 4. Hard constraint & the key insight

> **Constraint (new):** we may **not** change the source servers, no Change
> Tracking, CDC, triggers, or added `rowversion`/`ModifiedDate`, and the
> solution must be **uniform across every SQL variant**. This removes every
> *source-provided* change signal as a baseline. The only thing guaranteed on
> every server is **the data we can already read**.

The deterministic ids exist to fix async XTable 404s (Fabric caches paths across
**restarts**). Freshness needs ids to change across **data changes**. Under the
constraint, the only content-independent-yet-data-derived token available is a
**fingerprint of the content itself**. So make identity **content-addressed**:

- Each chunk's file name embeds a **hash of its bytes**:
  `split-{i}-{sha(content)}.parquet`.
- The snapshot id is a **root hash over the chunk hashes**.
- **Same content → same ids** across restarts (**no 404s**);
  **changed content → new ids** → new snapshot + new data-file paths
  (**Fabric re-reads**). No source cooperation required.

---

## 5. Detecting change without touching the source

**No free lunch.** With zero source cooperation, catching an in-place UPDATE
*requires reading the affected data* (or a fingerprint of it). There is no
standard, uniform, cheap SQL signal that reports "a row changed", those all come
from source-side features we're not allowed to enable.

So the uniform detector is **materialize-and-hash**: on a timer, re-run the same
SELECTs we already use, hash the produced bytes **in the proxy** (portable, done
in Python, not the DB), and compare to the last published hash. It catches
inserts, deletes **and** updates, on any engine, read-only.

| Strategy (`REFRESH_STRATEGY`) | Catches update? | Uniform? | Cost per poll |
|---|---|---|---|
| **content_hash** (default) | ✅ | ✅ | one read of the data (per chunk) |
| **ttl** (blind) | n/a, refreshes regardless | ✅ | re-materialize each tick even when idle |
| **manual** | only on `POST /_admin/refresh` | ✅ | none until called |
| **rowcount** (accelerator) | insert/delete only | ✅ | cheap `COUNT(*)`, a fast "definitely changed", **not** a safe skip for updates |
| **dialect_probe** (optional) | ✅ | ❌ per-dialect / perms | cheap, **safe skip** when no DML, else falls back to content_hash |

The optional `dialect_probe` reads existing **system views**: no source change, but isn't universally available/permitted, so it's an *accelerator*, never the
baseline:
- **SQL Server:** `sys.dm_db_index_usage_stats.last_user_update` (needs `VIEW SERVER STATE`).
- **PostgreSQL:** `pg_stat_user_tables` (`n_tup_ins + n_tup_upd + n_tup_del`).

When the probe says "no DML since last check" we skip the full read; otherwise we
fall back to content_hash. This keeps the guarantee uniform while cutting cost
where the catalog happens to be readable.

**Default = the `auto` cascade:** try `dialect_probe` first; if it's unavailable
or errors, prefer a **manual** refresh; only fall back to an automatic full
content read as a **last resort** (opt-in via `REFRESH_ALLOW_FULL_PULL`). Every
step is wrapped so a probe failure can never crash the server.

---

## 6. Chunking: content-addressed identity

The chunk **is** its content: `split-{i}-{sha(bytes)[:12]}.parquet`.

| | Effect |
|---|---|
| Unchanged chunk | same hash → same path → Fabric cache still valid → **not re-read** |
| Changed chunk | new hash → new path → new manifest entry → Fabric **re-reads only it** |
| Snapshot id | root hash over the ordered chunk hashes → changes **iff** some chunk changed |

Modulo vs. range partitioning both work with content-addressing:
- **Modulo `key % N`** (today): keep it, minimal change. An update rewrites one
  chunk's bytes → only that chunk's hash/path changes → Fabric re-reads one chunk.
- **Range/bucket by key** (enhancement): contiguous ranges give better update
  locality and unlock F3 **min/max pruning** (Fabric skips chunks whose bounds
  exclude the predicate). Recommended once content-addressing lands.

So modulo was never the problem, **fixed, content-independent identity** was.
Content-addressing fixes exactly that, uniformly, with no source signal.

---

## 7. Design

### 7.1 Content-addressed snapshots
- **Hash the logical row content, not the parquet bytes.** Parquet writers embed
  nondeterministic metadata / row-group stats, so hashing bytes would change the
  hash every run → endless churn + broken restart-stability. Each chunk already
  runs `… ORDER BY <key>`, so its rows are deterministic; hash a canonical
  serialization: `chunk_hash = sha256(b"\x1e".join(canonical(row)))[:12]`.
- `object_key = f"{table_path}/data/split-{i}-{chunk_hash}.parquet"`.
- `snapshot_id = int(sha256("|".join(f"{i}:{h}" for i,h in enumerate(chunk_hashes)))[:15], 16)`
, stable per content, new when any chunk changes. `version-hint` /
  `v{N}.metadata.json` advance each time the snapshot id changes.
- Restart-stable: same content ⇒ same hashes ⇒ same ids ⇒ no 404s.

### 7.2 Background poller (the `auto` cascade)
- Lifespan task every `REFRESH_POLL_SECONDS` (default **600 s / 10 min**, matching
  Fabric's sync cadence), per table:
  1. **`dialect_probe`**: token unchanged → **skip** (cheapest path).
  2. Token changed → `materialize_table` + `publish`.
  3. Probe unavailable / errored (`None`) → **do not** auto full-pull; log
     `probe_unavailable` and wait for a **manual** refresh, *unless*
     `REFRESH_ALLOW_FULL_PULL=1` (last resort) → then materialize + publish.
- **Crash-proof:** every step is wrapped in try/except; a failing poll logs and
  reschedules, it can never crash startup or the server.
- Gate with `AUTO_REFRESH` (default off ⇒ today's behavior preserved).

### 7.3 Cache invalidation
- New chunk paths are cold; superseded parquet is evicted from
  [cache/lru_cache.py](../cache/lru_cache.py) (memory) and its F5 disk file deleted.

### 7.4 Config (env / config.json)
| Key | Default | Meaning |
|---|---|---|
| `AUTO_REFRESH` | `0` | Enable the freshness poller |
| `REFRESH_POLL_SECONDS` | `600` | Poll interval (10 min, matches Fabric sync cadence) |
| `REFRESH_STRATEGY` | `auto` | `auto` (probe→manual→[full]) \| `dialect_probe` \| `content_hash` \| `ttl` \| `manual` |
| `REFRESH_ALLOW_FULL_PULL` | `0` | Allow the last-resort full content read when the probe is unavailable |
| `REFRESH_TTL_SECONDS` | `1200` | Window for the `ttl` strategy |

### 7.5 Range chunking + pruning (enhancement)
- Planner emits `WHERE <key> >= :lo AND <key> < :hi` per chunk (bounds from
  `MIN/MAX(<key>)`), and the manifest carries each chunk's min/max (F3) so Fabric
  prunes. Combines with content-addressing for cheap, targeted refresh.

---

## 8. Cost reality (be honest)

Uniform **+** no source changes ⇒ the proxy must **read the source to detect
updates**. That read cost scales with `table_size × poll_frequency` and is a hard
floor, there is no way to detect an in-place update cheaply without a source
signal we're forbidden to add.

Content-addressing minimizes **Fabric-side** churn (only changed chunks re-read;
snapshot only advances on real change) but not the **proxy-side** read cost.
Mitigations, in order:
- Raise `REFRESH_POLL_SECONDS` (staleness ↔ load trade-off).
- Use the optional `dialect_probe` where the catalog is readable (safe skip).
- Per-chunk hashing so only changed chunks re-publish (cuts Fabric cost); reads
  are still per-poll unless a probe skips them.
- For very large, hot tables use `manual` refresh (webhook after a known batch).

**Latency to freshness** = `poll interval` + Fabric-side lag (shortcut cache +
SQL-endpoint sync), never instant. Other properties preserved: no-404 (ids are
stable per content), and F2 time-travel (each distinct content is a retained
snapshot version).

---

## 9. Milestones

| # | Scope | Outcome | Status |
|---|---|---|---|
| **P1** | Content-addressed chunk paths + root-hash snapshot id + cache eviction | A content change yields a new snapshot Fabric re-reads; unchanged ⇒ identical ids (no churn / no 404s) | ✅ done |
| **P2** | Background poller + `content_hash` / `ttl` / `manual` + config + tests | Hands-off freshness, uniform on every dialect | ✅ done |
| **P3** | Optional `dialect_probe` fast-skip (MSSQL / PG) + tests | Lower read cost where the catalog is readable | ✅ done (SQLite/PG/MSSQL probe shipped with P2 `auto`) |
| **P4** | Range/bucket chunking + F3 min/max pruning | Better update locality + Fabric-side pruning for large tables | deferred |

**Effort:** P1–P2 ≈ M, P3 ≈ S, P4 ≈ L. **Risk:** med, touches snapshot identity;
validate each step with `validate_pyiceberg.py` **and** a real Fabric round-trip
(update a row → confirm the new value appears within the poll interval).

---

## 10. Decisions (resolved)

1. **Poll interval** default `REFRESH_POLL_SECONDS = 600` (10 min), matching
   Fabric's metadata-sync cadence.
2. **Detection is a cascade:** always try `dialect_probe` first; if it fails,
   prefer a **manual** override; an automatic **full content pull is the last
   resort** (opt-in via `REFRESH_ALLOW_FULL_PULL`, default off).
3. **Assume the system views are readable, but validate + catch:** every probe is
   wrapped so a missing view / permission error degrades gracefully (fall through
   the cascade) and **never crashes** the poller or startup.
4. **Assume the Fabric shortcut cache cannot be disabled:** freshness latency
   therefore includes an unavoidable Fabric-side lag; we do not rely on that lever.

---

## 11. Implementation plan

> **Status: P1–P3 shipped** (`iceberg/freshness.py`, `AUTO_REFRESH` gate). The
> module content-addresses every chunk, publishes a new version only on real
> change (dedupe verified live: repeated refresh with no data change returns
> `changed: []` and holds the version), runs a crash-proof background poller,
> and exposes a manual `POST /_admin/refresh` override. 10 dedicated tests in
> `tests/test_freshness.py`; full suite 107 passing. P4 remains deferred.

Everything is **gated by `AUTO_REFRESH` (default off)**: with it off the proxy
behaves exactly as today (deterministic ids; 97 tests unchanged; known-good
Fabric path untouched). Content-addressing + polling activate only when freshness
is enabled.

### P1: Content-addressed materialize + publish
- New `iceberg/freshness.py`:
  - `async def materialize_table(table, bucket, prefix) -> SnapshotState`: run
    every split query, build parquet, compute the **content-deterministic**
    `chunk_hash` (§7.1), set `object_key = split-{i}-{chunk_hash}.parquet`, cache
    the bytes, fill record_count/size/stats, and derive `snapshot_id`,
    manifest/uuid/`v{version}` keys.
  - `async def publish(table_name, new_snap) -> bool`: if `new_snap.snapshot_id`
    == current → no-op (`False`); else register as a new version (version++,
    history + metadata-log), **evict superseded chunk caches** (mem + F5 disk),
    drop the table's metadata cache. Returns `True` on change.
- `iceberg/state_store.py`: add `register_snapshot(snap)` to install a fully-built
  snapshot; keep `build_snapshot`/`build_all_snapshots` for the freshness-off path.
- `main.py` lifespan: when `AUTO_REFRESH`, materialize via `materialize_table` at
  startup instead of `build_all_snapshots` + `_materialize`.
- Tests: identical data → identical `snapshot_id` twice (restart-stable); one
  changed row → new `snapshot_id` + exactly one changed chunk path; `publish`
  dedupes when unchanged.

### P2: Poller (cascade) + config + manual refresh
- `iceberg/freshness.py`: `start_poller(app)` schedules an asyncio task running
  `poll_once` per table on the cascade in §7.2, **crash-proof** (try/except per
  step; log + reschedule).
- `POST /_admin/refresh` → force `materialize_table` + `publish` (manual override,
  bypasses the probe).
- `config.py`: `AUTO_REFRESH`, `REFRESH_POLL_SECONDS=600`, `REFRESH_STRATEGY=auto`,
  `REFRESH_ALLOW_FULL_PULL=0`, `REFRESH_TTL_SECONDS=1200` (all via the JSON layer).
- Tests: publishes on change, skips on unchanged, survives a probe exception (no
  crash), manual refresh forces a rebuild.

### P3: Dialect probes (best-effort, always validated)
- `async def probe_change_token(table) -> str | None`, dialect-selected:
  - **SQL Server:** `MAX(last_user_update)` from `sys.dm_db_index_usage_stats`
    for the object (needs `VIEW SERVER STATE`). Caveat: the DMV is empty until
    first access and resets on SQL restart → ambiguous reads return `None`.
  - **PostgreSQL:** `n_tup_ins + n_tup_upd + n_tup_del` from `pg_stat_user_tables`.
  - **SQLite (tests):** `PRAGMA data_version`.
  - Any error / unsupported dialect → `None` (cascade falls through).
- Store the token per table; compare across polls. Always try/except.
- Tests: sqlite `data_version` flips after a write; a raised probe → `None`
  (no crash, cascade falls through).

### P4: Range chunking + F3 pruning (deferred enhancement)
- Range predicates + per-chunk min/max in the manifest for Fabric-side pruning
  and better update locality. Independent of P1–P3.

### Cross-cutting validation
- After P1 and P2: `pytest`, `validate_pyiceberg.py`, and a **real Fabric
  round-trip**: update a row, confirm the new value appears within
  `REFRESH_POLL_SECONDS` + the (unavoidable) Fabric-side lag.

**Effort:** P1–P2 ≈ M, P3 ≈ S, P4 ≈ L. **Risk:** med, P1 reorders snapshot-id
derivation to *after* materialization, so validate restart-stability (same data →
same ids) carefully before enabling in Fabric.
