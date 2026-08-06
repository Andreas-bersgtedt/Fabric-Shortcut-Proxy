# Chapter 2: Core concepts

This chapter defines the model the rest of the manual uses. Read it once; the terms
here (split, snapshot, canonical path, freshness strategy, dialect) recur in every
later chapter.

## 2.1 The S3 front door

Every request enters through an S3-compatible API that implements `GET`, `HEAD`, and
`ListObjectsV2`, with HTTP range reads. Requests are authenticated with AWS SigV4.
The bucket in the request path decides the serving mode:

- A bucket with no mount is a **warehouse bucket**: it resolves to Iceberg or Delta
  table objects backed by SQL pushdown.
- A bucket with a mount is a **mounted bucket**: it streams object bytes from a
  storage backend.

The default warehouse bucket name is `fabric-iceberg-poc` and is configurable.

## 2.2 Warehouse mode: tables from SQL

In warehouse mode the proxy turns a relational table or view into table objects. You
do not write a column schema by hand. You point the proxy at a source table name and a
key column, and the Iceberg or Delta schema is reflected from the source at startup.

The read path is:

1. Fabric lists the table objects and reads the metadata (`metadata.json` or
   `_delta_log`).
2. Fabric requests a data file (a split).
3. The proxy maps that virtual file path to a parameterized SQL query, runs it, and
   returns the rows as Parquet.

Generated Parquet is cached in memory and, optionally, on disk, so repeat reads of the
same split do not re-query the source.

## 2.3 Splits: how a table is divided

A table is served as several virtual Parquet files called **splits**. Splits let Fabric
read a table in parallel and let the proxy bound the rows returned per query.

The planner assigns rows to splits according to the table's **split strategy**
(`split_strategy`: `range` for integer keys, `date` for temporal keys, `auto` to pick
either, or `modulo`), in order of preference:

- **Range split (preferred).** Contiguous key ranges are read straight off the key
  index. The predicate is `WHERE <key> >= :key_lo AND <key> < :key_hi ORDER BY <key>`.
- **Date split.** The same contiguous-range approach over a temporal key.
- **Modulo split (fallback).** Rows are sharded by `(<key> % num_splits) = split_index`.
  This is the deterministic fallback when range bounds are unavailable.
- **Row-number split.** For non-integer keys, rows are sharded by
  `(ROW_NUMBER() OVER (ORDER BY <key>) - 1) % num_splits`.

Split count is dynamic by default. The planner targets roughly `split_target_rows`
(100,000) rows per split and clamps the count between `split_count_min` and
`split_count_max`. You can also pin a fixed `num_splits` per table.

By default range/date splits cut the key axis into equal widths (`split_balance=span`).
For a skewed key, `split_balance=count` cuts at row quantiles instead, so each split holds
roughly equal rows — derived from the source's statistics histogram on SQL Server and
PostgreSQL (a zero-scan metadata read) or `NTILE` elsewhere.

Each split returns at most `QUERY_MAX_ROWS` rows (default 500,000). A table must satisfy
`total_rows ≤ num_splits × QUERY_MAX_ROWS`; if it does not, raise the split count or the
row cap. A per-table `split_target_rows` above the cap raises it for that table.

Strategy, balance, target rows, and split count are all overridable per table.

### The key column

The key column is the one thing you must choose per table.

- If you set a key column, it is the split key and its presence enables automatic
  schema reflection.
- If you do not, the proxy uses the table's primary key, auto-detected.
- Integer keys are preferred because they enable range planning. Non-integer keys fall
  back to row-number sharding.
- Views usually have no primary key. Always set a key column for a view.

## 2.4 Snapshots and versions

A published table is a **snapshot**: a fixed set of split data files plus the metadata
that points at them. When the proxy re-reads the source and the content changes, it
publishes a new snapshot with a new version. Data files from a snapshot are pinned so
they remain byte-identical while that snapshot is current, which prevents size-drift
read failures.

With Iceberg snapshot history enabled, prior snapshots are retained for time travel and
`/_admin/refresh` advances the version. Delta expresses the same lifecycle as commits in
`_delta_log`; each commit is a diff, and prior-version files stay servable. See
[DELTA_FORMAT.md](../DELTA_FORMAT.md).

## 2.5 Canonical object paths

Table objects are served under a canonical path derived from the source identity:

```
db/<server>/<database>/<schema>/<object>/...
```

Canonical paths are namespaced by server and database, so tables from different sources
never collide even when they share a name. This is the default. Legacy path aliases exist
but are disabled by default.

A Fabric shortcut points at the metadata entry point under this path:

- Iceberg: `db/<server>/<database>/<schema>/<object>/metadata/v1.metadata.json`
- Delta: `db/<server>/<database>/<schema>/<object>`

Fabric discovers the remaining objects from that entry point.

## 2.6 Freshness and auto-refresh

Auto-refresh keeps a published snapshot current by re-reading the source on a timer and
publishing a new snapshot only when the content actually changes. It is off by default.

The refresh strategy decides how change is detected:

- **`auto`**: use a cheap dialect-specific change probe when available, and fall back to
  a full content hash when `refresh_allow_full_pull` is on.
- **`content_hash`**: hash the materialized content and publish only when the hash
  changes.
- **`ttl`**: refresh on a fixed time interval.

`refresh_poll_seconds` sets the polling interval. `refresh_allow_full_pull=true` keeps
tables fresh (and silences the change-probe-unavailable warning) when the cheap probe is
not available for a source.

Random tokenization (chapter 7) is incompatible with content-based refresh, because a
fresh random token on every read would always look like changed content. The proxy fails
closed on that combination at startup.

## 2.7 Dialects

The proxy selects a SQL dialect from the connection string scheme and generates
correct SQL per engine. Supported dialects:

| Dialect | Scheme prefix | Identifier quoting | Row limit |
|---|---|---|---|
| SQLite | `sqlite+aiosqlite` | `"id"` | `LIMIT` |
| PostgreSQL | `postgresql+asyncpg` | `"id"` | `LIMIT` |
| SQL Server | `mssql+aioodbc` | `[id]` | `TOP` |
| Oracle | `oracle+oracledb` | `"id"` | `FETCH FIRST` |
| Databricks | `databricks` | `` `id` `` | `LIMIT` |

Each dialect knows its identifier quoting, integer cast type, row-limit syntax, and the
native functions used for tokenization. Capability differences (async driver, primary-key
reflection, fast row estimate, tokenization support) are recorded in a per-dialect
capability matrix that gates behavior at startup and in the config builder. Chapter 9 has
the full matrix.

## 2.8 Warehouse vs mount, side by side

| Aspect | Warehouse bucket | Mounted bucket |
|---|---|---|
| Backing | Relational source (SQL pushdown) | File share / S3 / Azure |
| Output | Iceberg or Delta + Parquet | Object bytes, unchanged |
| Freshness | Snapshots + auto-refresh | Live from the backend |
| Schema | Reflected from the source | None (opaque bytes) |
| Auth | SigV4 (optional unless enforced) | SigV4 forced by default |

## 2.9 Next

Continue to [Chapter 3: Architecture](03-architecture.md) to see how these concepts map
to processes and modules, or skip to [Chapter 4: Installation](04-installation.md) to
stand up a deployment.
