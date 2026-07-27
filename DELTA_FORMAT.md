# Delta Lake output mode (`TABLE_FORMAT=delta`)

Design notes for the **native Delta** emitter — an alternative to the default
Iceberg output that lets Microsoft Fabric read the virtual tables **without the
Iceberg → Delta conversion layer**.

Grounded in [delta/log.py](delta/log.py), [s3/router.py](s3/router.py),
[main.py](main.py), and [config.py](config.py).

---

## 1. Why

Fabric's S3 shortcut treats **Iceberg** as a foreign format: it runs a
metadata-virtualization pass that rewrites Iceberg metadata into a Delta log on
Fabric's side. That pass:

- adds **5 s – 2 min** of latency before a table is queryable,
- refreshes **at most ~once per 2 minutes**, and can show an inconsistent view
  mid-conversion,
- is where most of the format-level bugs we hit lived (field-ids, decimal type
  form, `last-sequence-number`, size-drift `BlobNotFound`, `READ_EXCEPTION`).

**Delta**, by contrast, is a *native* shortcut format — Fabric reads the
`_delta_log/` directly, no conversion. Serving Delta ourselves removes the whole
conversion surface (and its lag) while **reusing every other part of the proxy**:
the same content-addressed Parquet splits, split pinning, and auto-refresh.

The default stays `iceberg` (the validated known-good path). Delta is opt-in via
`TABLE_FORMAT=delta`.

---

## 2. What gets served

For a table exposed at `db/<server>/<database>/<schema>/<object>/` the object map becomes:

```
db/<server>/<database>/<schema>/<object>/_delta_log/00000000000000000000.json   ← commit 0
db/<server>/<database>/<schema>/<object>/_delta_log/00000000000000000001.json   ← commit 1 (after a refresh)
db/<server>/<database>/<schema>/<object>/data/split-0-<hash>.parquet            ← current data files
db/<server>/<database>/<schema>/<object>/data/split-1-<hash>.parquet
…
```

No `metadata.json`, no `*.avro` manifests, no `version-hint.text`. The Parquet
files are byte-for-byte the same ones the Iceberg path serves (same generator,
same content-addressed names, same pinning).

Point the Fabric shortcut at `db/<server>/<database>/<schema>/<object>` — Fabric
auto-detects `_delta_log/` and reads it as a Delta table.

---

## 3. Commit model

Each **published snapshot version** (the proxy's unit of freshness) maps to one
Delta commit. Delta commit files are JSON-lines (one action per line):

**commit 0** — `00000000000000000000.json`

```json
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"…","name":"<table>","format":{"provider":"parquet","options":{}},"schemaString":"…","partitionColumns":[],"configuration":{},"createdTime":<ms>}}
{"add":{"path":"data/split-0-<hash>.parquet","partitionValues":{},"size":<bytes>,"modificationTime":<ms>,"dataChange":true,"stats":"{\"numRecords\": <n>}"}}
… one add per split …
```

**commit N** (N ≥ 1) — after a refresh publishes a new version

```json
{"add":{"path":"data/split-0-<newhash>.parquet", …}}      ← only the CHANGED splits
{"remove":{"path":"data/split-0-<oldhash>.parquet","deletionTimestamp":<ms>,"dataChange":true,"extendedFileMetadata":true,"partitionValues":{},"size":<bytes>}}
```

Commit N is a **diff** (see §4.1): only splits whose content changed are `add`ed,
and only the files they replaced are `remove`d. Because splits are
content-addressed, unchanged splits keep the same path and carry forward with no
action — a full add-all/remove-all replace would net an unchanged file out of the
table for a replaying reader.

Key invariants:

- **Paths are relative** to the table root (`data/split-…parquet`), because the
  shortcut's sub-path *is* the Delta table root.
- Delta commit files are contiguous from `00000000000000000000.json` (an empty
  no-op diff is skipped, so a Delta commit number need not equal the internal
  snapshot version).
- `stats` carries `numRecords` only (enough for Delta; no min/max needed).
- `protocol` is reader 1 / writer 2 — plain add/remove, no deletion vectors or
  column mapping.

---

## 4. How it's derived (decoupled from freshness)

The emitter does **not** touch the freshness/publish path. Instead
[delta/log.py](delta/log.py) reads the existing snapshot **history** from
`iceberg.state_store` and memoizes commits:

- `_commits: dict[table -> list[str]]` — append-only commit texts, one per
  content-changing version (indexed 0..N; a no-op version is skipped).
- `_prev_files` / `_committed_version` — per-table bookkeeping to build the `add`/
  `remove` diff and to skip already-committed versions.

`sync_all()` walks `get_snapshot_history(table)` (oldest → newest) and appends a
commit for every version not yet committed. It is called:

1. **once at startup** ([main.py](main.py) lifespan, after materialization) so
   commit 0 is captured before any history pruning could drop version 1, and
2. **lazily on every object listing** (`delta_log_objects()`), so commits created
   by the background refresh poller show up without any poller changes.

Because `_commits` is append-only in-process, the log stays contiguous from
commit 0 even after `state_store` prunes old snapshot **data** beyond
`SNAPSHOT_HISTORY_LIMIT`. A Delta reader only physically fetches the
**net-current** files (which are pinned), so the removed older files never need
to exist on disk.

### 4.1 Commit N is a diff, not a replace

Because splits are **content-addressed**, an unchanged split keeps the *same*
path across versions. Commit N therefore emits a minimal diff:

- `add` only files that are in the new version but **not** in the previous one;
- `remove` only files that were in the previous version but are **gone**;
- unchanged splits carry forward with **no action**.

Emitting `add` **and** `remove` for the same path in one commit would net that
file out of the table for a replaying reader (silent data loss), so a full
add-all/remove-all replace is never used. An empty diff (identical file set) is
skipped entirely.

### 4.2 Prior-version files stay servable

When a refresh publishes a new version, Fabric's SQL endpoint may still reference
the **previous** version's files until it re-syncs the `_delta_log`. Those files
remain pinned and retained within `SNAPSHOT_HISTORY_LIMIT`, and
`state_store.get_split_by_key` resolves data files across the retained **history**
(not just the current version), so a stale request gets the bytes instead of a
`404` ("underlying location does not exist"). `delta_log_objects()` likewise
advertises the data files of every retained version.

---

## 5. Type mapping (Iceberg → Delta)

`delta/log.py::_delta_type` (schema is emitted as a Spark `StructType` JSON string):

| Iceberg | Delta |
|---|---|
| `boolean` | `boolean` |
| `int` | `integer` |
| `long` | `long` |
| `float` / `double` | `float` / `double` |
| `date` | `date` |
| `string`, `uuid`, `time` | `string` |
| `binary`, `fixed(n)` | `binary` |
| `decimal(P,S)` | `decimal(P,S)` |
| `timestamp` (no zone) | `timestamp_ntz` |
| `timestamptz` (UTC) | `timestamp` |

---

## 6. Request routing

[s3/router.py](s3/router.py) branches on `config.TABLE_FORMAT`:

- **ListObjectsV2 / HEAD** — `_snapshot_objects()` returns
  `delta.log.delta_log_objects()` in delta mode (the `_delta_log/*.json` commits
  + current data files) instead of the Iceberg metadata/manifest/version-hint
  objects.
- **GET** — a `…/_delta_log/*.json` key is served from `delta.log.get_commit_bytes`
  (range-aware, `application/json`); unknown log files such as `_last_checkpoint`
  return `404` (expected — Delta readers probe for it). The Iceberg metadata
  branch is gated off in delta mode. **Data-file GETs are unchanged** — the same
  `get_split_by_key` → pinned/generated Parquet path serves both formats.

---

## 7. What is *unchanged*

- Parquet generation, content-addressed split names, `PIN_MATERIALIZED_SPLITS`.
- Auto-refresh (`AUTO_REFRESH`) and the freshness poller — no edits; delta commits
  are derived from the snapshots it already publishes.
- The config-builder UI, `/readyz`, tracing, and the monitor dashboard.

---

## 8. Tests

[tests/test_delta.py](tests/test_delta.py) covers the type mapping + schema
string (unit) and the router in delta mode (integration): listing serves
`_delta_log/…json` + `.parquet` and **not** Iceberg artifacts; commit 0 parses to
`protocol` + `metaData` (valid struct schema) + one `add` per split; Parquet data
is served and readable; unknown log files 404. A refresh regression test asserts
commit N is a minimal diff (only the changed split is added/removed, unchanged
splits carry forward) and that the prior version's files stay resolvable. The
Iceberg default path is untouched, so the existing suite stays green.
