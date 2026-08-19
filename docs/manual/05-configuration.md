# Chapter 5: Configuration

This chapter is the working guide to pointing the proxy at a source and registering the
tables it serves. It covers the settings model, the config files, the table registry, the
config builder, and multi-connection sources. The complete PostgreSQL and SQL Server
reference with reflection and type-mapping detail is in
[CONFIGURATION.md](../CONFIGURATION.md); the settings registry in `config.py` is the source
of truth for every key.

## 5.1 The settings model

Settings resolve with this precedence, highest first:

1. **Environment variable** (for example `DB_URL`, `PORT`, `KEY_COLUMN`).
2. **External JSON file** (`config.system.json`, `config.connection.json`, and the table
   registry; legacy single `config.json` is still honored).
3. **Built-in default** from `config.py`.

An environment variable always overrides the same key in a JSON file, which overrides the
default. This lets you keep structure in files and inject secrets from the environment.

Settings are grouped into these categories (the order used by the config builder and the
settings catalog): Connection, S3 endpoint, Server, Splits & query, Caching, Robustness,
Admin & observability, Iceberg (advanced), Data freshness, Cluster (scale), Other. Chapter
9 lists the notable keys per group.

Some settings can change in the running process without a restart (splits, caching, some
observability). Structural settings (connection string, port, bucket, HA, control plane)
always require a restart. Chapter 9 marks which are live.

## 5.2 The config files

Configuration is split by concern so secrets and structure stay separated. Each file has a
committed `*.example.json` template; the real files are gitignored.

| File | Holds | Template |
|---|---|---|
| `config.system.json` | S3 endpoint, server, ports, feature flags, cluster settings | `config.system.example.json` |
| `config.connection.json` | Connection string and query/robustness settings | `config.connection.example.json` |
| `config.tables.json` | The table registry | `config.tables.example.json` |
| `config.mounts.json` | Storage-proxy mount table (references credential ids, not secrets) | `config.mounts.example.json` |

A legacy single `config.json` (template `config.example.json`) still works and is picked up
automatically, or point `CONFIG_FILE` at a specific path. Copy a template, edit it, and
place it next to `main.py`:

```powershell
Copy-Item config.connection.example.json config.connection.json
Copy-Item config.tables.example.json config.tables.json
```

Never commit a file that contains a connection string or key. `config.connection.json`,
`config.system.json`, `config.tables.json`, and `config.mounts.json` are all gitignored.

## 5.3 Connecting to a source

The connection string selects the SQL dialect from its scheme prefix. You do not choose a
dialect separately.

```
postgresql+asyncpg://user:pass@host:5432/dbname
mssql+aioodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
oracle+oracledb://user:pass@host:1521/service
databricks://token:<pat>@<host>?http_path=/sql/1.0/warehouses/<id>
```

Set it as an environment variable or in `config.connection.json`:

```powershell
$env:DB_URL = "mssql+aioodbc://appreader:secret@sql-host:1433/salesdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
```

Install the matching driver first (chapter 4, §4.4). SQL Server also needs the OS ODBC
Driver 18.

## 5.4 The key column

The key column is the one thing you choose per table.

- Set it, and it becomes the split key; its presence enables automatic schema reflection.
- Leave it unset, and the proxy uses the table's auto-detected primary key.
- Integer keys enable range planning (index-pruned reads). Non-integer keys fall back to
  row-number sharding.
- Views usually have no primary key. Always set a key column for a view.

You do not write a column schema by hand. The proxy reflects the source columns at startup
and maps them to Iceberg types. The full type-mapping table is in
[CONFIGURATION.md §3.3](../CONFIGURATION.md).

## 5.5 A single table

A single table needs only environment variables:

```powershell
$env:DB_URL = "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb"
$env:DB_SOURCE_TABLE = "public.orders"
$env:KEY_COLUMN = "order_id"
$env:NUM_SPLITS = "8"
python main.py
```

## 5.6 The table registry (multiple tables)

Multiple tables are cleanest in `config.tables.json`, with no Python editing. Each entry
points at a source table and a key column; the schema is reflected.

```jsonc
{
  "tables": [
    { "name": "orders",    "source_table": "public.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "public.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

Per-table fields you will use most:

| Field | Meaning |
|---|---|
| `name` | The object name in the served path |
| `source_table` | Schema-qualified source table or view |
| `key_column` | Split key (required for views) |
| `num_splits` | Pin a fixed split count for this table (otherwise dynamic) |
| `split_strategy` | `modulo`, `range`, `date`, or `auto` for this table (else the global default) |
| `split_target_rows` | Target rows per split for this table; a value above the row cap also raises the cap for this table |
| `split_balance` | `span` (equal key/time width) or `count` (equal rows per split) for this table |
| `split_sample_rows` | Cap rows fed into `count` quantile planning for this table (0 = full scan) |
| `schema` | Optional explicit column list; omit a column to remove it from output |
| `connection_id` | Bind this table to a named connection (see §5.8) |

Table names must be unique, and every `connection_id` must reference a defined connection;
the proxy validates both at startup.

## 5.7 The config builder

If you would rather not write JSON, enable the browser config builder. It reflects tables,
auto-detects key columns, edits per-column policies, and downloads or applies the config.

```powershell
$env:ENABLE_CONFIG_BUILDER = "1"
.\Manager.ps1 -SkipInstall
# open http://localhost:9200/_config/   (Manager control plane)
# standalone python main.py: http://localhost:9000/_config/
```

It is off by default and accepts database credentials, so run it on a private surface only.
It also hosts the per-column tokenization policy editor (chapter 7). The builder preserves
existing explicit schemas and transforms across reloads and apply operations.

## 5.8 Multi-connection sources

A single deployment can serve tables from more than one source. Define named connections
and bind each table to one with `connection_id`. The default connection is `default` (the
top-level `DB_URL`). Each connection can use a different dialect, so one proxy can front a
SQL Server and a PostgreSQL source at once. The config builder manages connections in the
UI; the encrypted credential store (chapter 7) holds each connection's secret and hydrates
`DB_URL_<ID>` at startup.

## 5.9 Output format

`TABLE_FORMAT` selects the published format from the same query path:

- `iceberg` — `metadata.json`, Avro manifests, `version-hint.text`.
- `delta` — native `_delta_log` that Fabric reads directly with no conversion.

Delta is often preferred for Fabric. See [DELTA_FORMAT.md](../DELTA_FORMAT.md) for the
commit model and type mapping.

## 5.10 Materialization mode

`MATERIALIZE_MODE` chooses when the proxy builds split Parquet (restart-required):

- `eager` (default) — build all splits at startup; lowest read latency; best for
  live/mutating sources.
- `lazy` — build a table's splits on first read, then pin them; unread tables cost nothing at
  startup. Works cluster-wide (a multi-agent fleet needs a shared artifact store) and for the
  C++ serving agent (Manager-mediated).
- `virtual` — build on first read to learn sizes, then keep zero bytes at rest and regenerate
  each split deterministically on demand; for immutable / snapshot-isolated sources only, with
  a determinism self-check that fails closed on drift.

`lazy` and `virtual` are incompatible with auto-refresh. Full behavior and sizing guidance are
in [Chapter 8, §8.8](08-operations.md).

## 5.11 Open Mirroring targets

Open Mirror publishing is configured separately from the shortcut table registry. Copy
[config.open_mirror.example.json](../../config.open_mirror.example.json) to
`config.open_mirror.json`, then set the source connection, Fabric mirrored database identifiers,
landing-zone root, and tables to publish.

Each table uses one of these strategies:

- `watermark_column` present: source-incremental upserts ordered by the watermark and key columns.
  This does not detect deletes or rows whose watermark is not greater than the committed cursor.
- `watermark_column` omitted: a full-source snapshot scan with local row-hash diffing. This detects
  inserts, updates, and deletes, but reads the source table on every cycle.
- `mode: "initial"`: an explicit full load. Invocation mode takes precedence over table mode,
  which takes precedence over `OPEN_MIRROR_MODE`.

The Manager stores cursor and recovery state outside the landing zone. Set
`OPEN_MIRROR_STATE_DIR` explicitly for a service installation, or create the Linux service
directory before startup. Missing state permits the first load; corrupt or unreadable state stops
the table until an operator uses the explicit reset endpoint.

For OneLake targets, the Manager checks mirroring before source extraction. With
`self_healing: true` (the default), it uses the existing proxy Entra identity to start mirroring
when Fabric reports `Initialized`, `Paused`, or `Stopped`, then waits for `Running`. This starts
the mirrored database operation only; it does not start Fabric capacity.

The Manager Monitor includes a Data File Manager for processed landing-zone files. Set
`cleanup_retention_days` on a target to choose how long files remain in Fabric's
`_FilesReadyToDelete` folder. A table-level value overrides the target value. The default is
seven days. Inspect is always a dry run. Delete eligible files requires explicit confirmation and
removes only a complete `_FilesReadyToDelete` folder whose files have all passed the retention
period. Active table files and the current sequence file are not selected.

## 5.12 Validation

The proxy validates configuration at startup and fails closed with a clear, redacted
message on problems: unknown dialects, missing token keys, transformed split keys, malformed
policy schemas, duplicate table names, undefined connections, and incompatible
refresh/tokenization combinations. Fix the reported problem and restart. A masked (`***`)
connection string is never stored, hydrated, or treated as a credential.

## 5.13 Next

Continue to [Chapter 6: Connecting Microsoft Fabric](06-connectivity.md) to wire a shortcut
to the tables you just registered.
