# Configuration Manual: PostgreSQL & SQL Server

Working, end-to-end configuration for pointing the Fabric Shortcut Proxy at a real
**PostgreSQL** or **Microsoft SQL Server** source, single-table and multi-table.

You do **not** hand-write a column schema. Point the proxy at a **table/view
name** and a **key column**; the Iceberg schema is **reflected from the source
database automatically** at startup.

Grounded in [config.py](../config.py), [db/executor.py](../db/executor.py) (reflection),
and [planner/dialects.py](../planner/dialects.py).

---

## 1. How configuration works

Settings resolve with this precedence (highest wins):

1. **Environment variable**
2. **External JSON file** (`config.json`, or `$CONFIG_FILE`)
3. Built-in default

| Surface | What it controls | How to set it |
|---|---|---|
| **Environment variables** | Connection string, source table, **key column**, bucket, port, splits, flags | `DB_URL`, `DB_SOURCE_TABLE`, `KEY_COLUMN`, … |
| **`config.json`** | Everything above **plus the full `tables` registry**: no Python | Copy [config.example.json](../config.example.json) → `config.json` |
| **`config.py` → `TABLES`** | The table registry (alternative to `config.json`) | Edit [config.py](../config.py) |

At startup the proxy:
1. Reads `DB_URL` and auto-selects the SQL **dialect** from its scheme.
2. For every table, **reflects the source columns** and maps them to Iceberg
   types (or uses your explicit schema, if you provided one).
3. Resolves the **split key column** (explicit `KEY_COLUMN`, else the primary key).
4. Materializes split Parquet files and serves them under canonical paths
  `db/<server>/<database>/<schema>/<object>/…`.

> A **single table** needs only environment variables. **Multiple tables** are
> cleanest via `config.json` (no Python editing at all).

### 1.1 Using `config.json` (recommended for multi-table)

Copy the template and edit it, everything, including multiple tables, is
JSON-driven and schema-free:

```jsonc
// config.json  (place next to main.py, or point CONFIG_FILE at it)
{
  "db_url": "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb",
  "num_splits": 8,
  "require_sigv4": false,
  "tables": [
    { "name": "orders",    "source_table": "public.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "public.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

```powershell
.\Manager.ps1 -SkipInstall        # config.json is picked up automatically (Windows)
# Linux/macOS:
bash ./Manager.sh --skip-install
# or point at a specific file:
$env:CONFIG_FILE = "C:\deploy\prod.config.json"; .\Manager.ps1 -SkipInstall
# Linux/macOS:
CONFIG_FILE=/deploy/prod.config.json bash ./Manager.sh --skip-install
```

`config.json` is gitignored (it holds your connection string). Environment
variables still override individual keys, e.g. `$env:DB_URL=…` wins over the
file's `db_url`. Full key list is in §8; per-table fields in §5.2.

### 1.2 Or build it in the browser (`ENABLE_CONFIG_BUILDER`)

Don't want to write JSON at all? Enable the **config builder** and generate the
file from a UI:

```powershell
$env:ENABLE_CONFIG_BUILDER = "1"
.\Manager.ps1 -SkipInstall
# open http://localhost:9200/_config/   # Manager control-plane surface
```

If you run standalone `python main.py` (no Manager), use `http://localhost:9000/_config/`.

Enter host / user / password → pick tables (key column auto-detected, overridable)
→ **Download config.json**. It's **off by default** and accepts DB credentials, so
run it locally only. The Manager bootstrap installs all supported Python database drivers.

---

## 2. Prerequisites (drivers)

| Source | Driver | Install | Bundled? |
|---|---|---|---|
| SQLite (demo) | `aiosqlite` | included | ✅ |
| **SQL Server** | `aioodbc` **+** OS *ODBC Driver 18 for SQL Server* | Python driver included; install the ODBC driver from Microsoft | ⚠️ OS driver required |
| **PostgreSQL** | `asyncpg` | Manager bootstrap or `pip install -e '.[postgres]'` | ✅ Manager |
| **Oracle** | `oracledb` | Manager bootstrap or `pip install -e '.[oracle]'` | ✅ Manager |
| **Amazon Redshift** (preview) | `sqlalchemy-redshift` + `redshift-connector` | Manager bootstrap or `pip install -e '.[redshift]'` | ✅ Manager |
| **Teradata** (preview) | `teradatasqlalchemy` | Manager bootstrap or `pip install -e '.[teradata]'` | ✅ Manager |
| **Apache Impala** (preview) | `impyla` | Manager bootstrap or `pip install -e '.[impala]'` | ✅ Manager |

```powershell
# Manual install outside the Manager bootstrap: all supported Python DB drivers
.\.venv\Scripts\python.exe -m pip install -e ".[drivers]"

# SQL Server also needs the OS ODBC driver:
#   https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server
```

---

## 3. Core concepts

### 3.1 Connection string (`DB_URL`)

The scheme prefix auto-selects the dialect:

```
postgresql+asyncpg://user:pass@host:5432/dbname
mssql+aioodbc://user:pass@host:1433/dbname?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
redshift+redshift_connector://user:pass@host:5439/dbname
teradatasql://user:pass@host/?database=dbname&dbs_port=1025
impala://host:21050/dbname
```

### 3.2 The key column (`KEY_COLUMN`): the only thing you must choose

By default, split count is dynamic (`split_target_rows=100000`) and planning uses
contiguous key ranges for index-pruned reads when possible. The legacy modulo
predicate remains as deterministic fallback. Range/date splits are sized by equal
key width by default (`split_balance=span`) or by equal rows (`split_balance=count`);
see §3.5.

Range form (preferred):

```sql
WHERE <key> >= :key_lo AND <key> < :key_hi
ORDER BY <key>
```

Modulo fallback form:

```sql
WHERE (CAST(<key> AS BIGINT) % :num_splits) = :split_index
ORDER BY <key>
```

- If `KEY_COLUMN` is set, that column is the split key **and** its presence turns
  on automatic schema reflection.
- If it is not set, the proxy uses the table's **primary key** (auto-detected).
- Integer keys are preferred for range planning. For non-integer keys, the planner
  falls back to deterministic row-number sharding.

> **Views** usually have no primary key, always pass `KEY_COLUMN` for a view.

### 3.3 Automatic type mapping

Reflected source types are mapped to Iceberg types automatically:

| Source type | Iceberg |
|---|---|
| `bigint` | `long` |
| `integer`, `smallint`, `int` | `int` |
| `double precision` / `float` | `double` |
| `real` | `double` |
| `numeric(P,S)` / `decimal(P,S)` | `decimal(P,S)` |
| `varchar`, `text`, `nvarchar`, `char` | `string` |
| `boolean` / `bit` | `boolean` |
| `date` | `date` |
| `timestamp` / `datetime2` | `timestamp` |
| `timestamptz` / `datetimeoffset` | `timestamptz` |
| `bytea` / `varbinary` | `binary` |
| `uuid` / `uniqueidentifier` | `uuid` |
| anything else | `string` (safe fallback) |

Column **order** and **names** come straight from the source; `field_id`s are
assigned `1..N` in column order.

### 3.4 Schema-qualified source tables

`DB_SOURCE_TABLE` / `source_table` may be schema-qualified. Reflection and the
generated SQL both handle it:

| Value | PostgreSQL SQL | SQL Server SQL |
|---|---|---|
| `sales` | `"sales"` | `[sales]` |
| `public.sales` | `"public"."sales"` | — |
| `dbo.sales` | — | `[dbo].[sales]` |

### 3.5 Row budget

Each split returns up to `QUERY_MAX_ROWS` (default 500,000) rows. Ensure
`total_rows ≤ num_splits × QUERY_MAX_ROWS`; otherwise raise `NUM_SPLITS` (or the
table's `num_splits`) or `QUERY_MAX_ROWS`.

Per-table `split_target_rows` overrides the global default for a single table —
useful when a narrow, high-volume table should pack far more rows per split than
the 100,000 default:

```json
{ "name": "clickstream", "source_table": "dbo.clickstream",
  "key_column": "event_id", "split_target_rows": 1000000 }
```

A per-table `split_target_rows` also raises that table's per-split row cap: the
effective cap is `max(query_max_rows, split_target_rows)`, so a larger target is
never truncated by the smaller default. Omit the field to inherit the global
`split_target_rows`.

Per-table `split_strategy` overrides the global strategy for a single table, so a
small dimension and a large fact table can plan differently — `modulo` (full
scan), `range` (integer key ranges), `date` (temporal ranges) or `auto`:

```json
{ "name": "customers", "source_table": "dbo.customers", "key_column": "customer_id",
  "num_splits": 4, "split_strategy": "modulo" },
{ "name": "clickstream", "source_table": "dbo.clickstream", "key_column": "event_id",
  "split_strategy": "range", "split_target_rows": 1000000 }
```

Omit `split_strategy` to inherit the global `split_strategy`. Both fields are also
editable per table in the config-builder **Tables** tab.

Per-table `split_balance` controls how `range`/`date` splits are **sized**:
`span` (default) cuts the key/time axis into equal widths; `count` cuts at row
quantiles (`NTILE`) so each split holds roughly equal rows — which keeps splits
near `split_target_rows` even when the key is skewed (gappy identity columns,
time-clustered facts). `count` costs a one-time ordered scan of the key column at
planning time (cheapest when the key is indexed) and falls back to `span` when
the source can't compute quantiles; it does not change the number of splits or
the serving queries. Omit to inherit the global `split_balance`.

For large tables, `split_sample_rows` (global, or per-table) caps the rows fed
into `count` planning: when the integer key column is larger, a deterministic
stride sample bounds the planning sort/window/tempdb cost. Boundaries become
approximate but stay far more balanced than `span`; the overall max is still read
from the full table so the last range always covers it. `0` (default) scans the
full key column.

On SQL Server and PostgreSQL, `count` planning for an **integer** key first tries
the optimizer's existing statistics histogram (`sys.dm_db_stats_histogram` /
`pg_stats.histogram_bounds`) — a metadata read with **zero data scan** — and only
falls back to `NTILE` (then equal-span) when no stats exist. Set
`split_use_stats_histogram=false` to force `NTILE` if the stats may be stale.

```json
{ "name": "clickstream", "source_table": "dbo.clickstream", "key_column": "event_id",
  "split_strategy": "range", "split_balance": "count", "split_target_rows": 1000000,
  "split_sample_rows": 500000 }
```

---

## 4. PostgreSQL: single table (env only, no Python)

### 4.1 Source

```sql
CREATE TABLE public.orders (
    order_id    bigint PRIMARY KEY,
    customer    varchar(200),
    amount      numeric(12,2),
    created     date,
    is_paid     boolean
);
```

### 4.2 Configure & launch: that's it

```powershell
.\.venv\Scripts\python.exe -m pip install asyncpg   # once

$env:DB_URL          = "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb"
$env:DB_SOURCE_TABLE = "public.orders"   # table or view
$env:KEY_COLUMN      = "order_id"        # integer split key -> enables auto-schema
$env:TABLE_NAME      = "orders"          # Iceberg name (path is canonical by default)
$env:NUM_SPLITS      = "8"
.\Manager.ps1 -SkipInstall
```

The Iceberg schema (`order_id long`, `customer string`, `amount decimal(12,2)`,
`created date`, `is_paid boolean`) is reflected automatically. Generated split
SQL (PostgreSQL dialect):

```sql
SELECT "order_id", "customer", "amount", "created", "is_paid"
FROM "public"."orders"
WHERE "order_id" >= :key_lo AND "order_id" < :key_hi
ORDER BY "order_id"
LIMIT :max_rows
```

### 4.3 Verify

```powershell
curl http://localhost:9000/readyz
curl "http://localhost:9000/fabric-iceberg-poc/db/<server>/<database>/<schema>/orders/metadata/v1.metadata.json"

$env:S3EMU_SERVER = "http://127.0.0.1:9000"
.\.venv\Scripts\python.exe validate_pyiceberg.py   # (edit meta path for non-'sales' tables)
```

### 4.4 Fabric shortcut

| Setting | Value |
|---|---|
| URL | `http://<proxy-host>:9000` |
| Bucket | `fabric-iceberg-poc` |
| Sub path | `db/<server>/<database>/<schema>/orders/metadata/v1.metadata.json` |

---

## 5. PostgreSQL: multi table

Multiple tables is the one case that needs `config.py`, but still **no column
schemas**, just a short list.

### 5.1 Source

```sql
CREATE TABLE public.orders (
    order_id bigint PRIMARY KEY, customer varchar(200), amount numeric(12,2),
    created date, is_paid boolean
);
CREATE TABLE public.customers (
    customer_id bigint PRIMARY KEY, full_name varchar(200), email varchar(200),
    signup_date date, lifetime_value numeric(12,2)
);
```

### 5.2 Register both tables: `config.json` (no Python)

```json
{
  "db_url": "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb",
  "tables": [
    { "name": "orders",    "source_table": "public.orders",    "key_column": "order_id",    "num_splits": 8 },
    { "name": "customers", "source_table": "public.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

Per-table JSON fields:

| Field | Required | Notes |
|---|---|---|
| `source_table` | yes | Table/view; schema-qualified allowed |
| `name` | no | Defaults to the source table's last segment; canonical path is derived from source identity |
| `key_column` | no | Integer split key; defaults to the primary key |
| `num_splits` | no | Defaults to the top-level `num_splits` / `NUM_SPLITS` |
| `schema` | no | Explicit override: `[{ "field_id", "name", "type", "nullable" }]` |
| `connection` | no | Source connection id (see §5.4). Defaults to `default` |

**Or** the equivalent `config.py` (`TABLES` list), one line per table:

```python
# config.py
TABLES: list[TableDef] = [
    TableDef(name="orders",    source_table="public.orders",    key_column="order_id",    num_splits=8),
    TableDef(name="customers", source_table="public.customers", key_column="customer_id", num_splits=4),
]
```

`schema` is omitted → reflected from each source. `key_column` is optional if the
table has a primary key (auto-detected); pass it for views or composite keys.

### 5.4 Multiple sources / dialects (one proxy, many databases)

Tables can be served from **different source databases of different dialects** at
once. Declare each extra source in a `connections` array in
[config.connection.json](../config.connection.example.json), then bind a table to
one with its `connection` field.

```jsonc
// config.connection.json
{
  "connection": {                              // the DEFAULT source (id "default")
    "db_url": "mssql+aioodbc://user:pass@sql-host/erp?driver=ODBC+Driver+18+for+SQL+Server"
  },
  "connections": [                             // additional named sources
    { "id": "warehouse_pg",  "db_url": "postgresql+asyncpg://user:pass@pg-host:5432/salesdb" },
    { "id": "legacy_oracle", "db_url": "oracle+oracledb://user:pass@ora-host:1521/ORCLPDB1" }
  ]
}
```

```jsonc
// config.tables.json
{
  "tables": [
    { "name": "orders",    "source_table": "dbo.orders",       "key_column": "order_id" },                              // default (SQL Server)
    { "name": "shipments", "source_table": "public.shipments", "key_column": "shipment_id", "connection": "warehouse_pg" }, // PostgreSQL
    { "name": "invoices",  "source_table": "FIN.INVOICES",     "key_column": "invoice_id",  "connection": "legacy_oracle" } // Oracle
  ]
}
```

Notes:
- The id `default` is **reserved** and always derived from `db_url` / `DB_URL`;
  `connections[]` entries must use other ids.
- Each connection gets its **own** engine pool, dialect, capability profile, and
  `SOURCE_MAX_CONCURRENCY` backpressure gate, one busy source can't starve
  another.
- Per-connection `query_timeout_seconds`, `query_max_rows`, `db_max_retries`, and
  `db_retry_backoff_seconds` are optional overrides; unset values fall back to the
  global defaults.
- Canonical object paths are namespaced by each source's server/database, so
  tables from different connections never collide.
- Credentials in `connections[]` are credential-gated exactly like the default
  section, prefer environment variables / secret stores for passwords.

### 5.5 Launch & verify

```powershell
$env:DB_URL = "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb"
.\Manager.ps1 -SkipInstall

curl "http://localhost:9000/fabric-iceberg-poc/db/<server>/<database>/<schema>/orders/metadata/v1.metadata.json"
curl "http://localhost:9000/fabric-iceberg-poc/db/<server>/<database>/<schema>/customers/metadata/v1.metadata.json"
curl "http://localhost:9000/fabric-iceberg-poc?list-type=2&prefix=db/&delimiter=/"
```

Create one Fabric shortcut per table (`…/orders/…`, `…/customers/…`).

---

## 6. SQL Server: single table (env only)

### 6.1 Source

```sql
CREATE TABLE dbo.orders (
    order_id  bigint       NOT NULL PRIMARY KEY,
    customer  nvarchar(200),
    amount    decimal(12,2),
    created   date,
    is_paid   bit
);
```

### 6.2 Configure & launch

SQL Server supports three authentication methods. Pick one in the Config Builder's
**Authentication** selector (SQL Server only), or set the `DB_URL` directly:

```powershell
# 1) SQL authentication (Driver 18 encrypts by default -> TrustServerCertificate for dev)
$env:DB_URL          = "mssql+aioodbc://sa:Str0ng!Pass@mssql-host:1433/SalesDb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
$env:DB_SOURCE_TABLE = "dbo.orders"
$env:KEY_COLUMN      = "order_id"
$env:TABLE_NAME      = "orders"
$env:NUM_SPLITS      = "8"
.\Manager.ps1 -SkipInstall

# 2) Windows / integrated authentication (the account running the Manager/agent;
#    on Linux this needs a Kerberos-joined host). No username or password is stored.
# $env:DB_URL = "mssql+aioodbc://@mssql-host/SalesDb?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=yes&TrustServerCertificate=yes"

# 3) Entra ID service principal (SPN). The client id is the UID, the secret is the PWD,
#    over an encrypted channel. Needs ODBC Driver 18 (or 17.4+). Grant the SPN a login
#    + read access on the database.
# $env:DB_URL = "mssql+aioodbc://<client-id>:<client-secret>@mssql-host/SalesDb?driver=ODBC+Driver+18+for+SQL+Server&Authentication=ActiveDirectoryServicePrincipal&Encrypt=yes"

# 4) Reuse the proxy's OWN Entra identity — the one already configured for Key Vault
#    (issue #16) — with no separate credentials. In the Config Builder pick
#    "Entra ID — reuse the proxy identity"; the connector uses the proxy's service
#    principal, managed identity, or default credential (per auth_mode). Grant that
#    identity a SQL login + read access. (Effective DB_URL is the same as option 2/3
#    for the resolved mode, filled from AZURE_CLIENT_ID / AZURE_CLIENT_SECRET.)
```

In the Config Builder, choosing **Windows (Integrated Security)** hides the credential
fields, **Service Principal (Entra ID)** swaps them for a client id + client secret, and
**Entra ID — reuse the proxy identity** uses the proxy's own Entra identity from issue #16
(the Key Vault service principal / managed identity / default credential) with nothing to
enter here. The SPN secret is encrypted in the Manager credential store (and mirrored to Key
Vault when write-back is on — see [SECURITY.md](SECURITY.md)). SQL Login is unchanged.

Generated split SQL (SQL Server dialect, brackets, `BIGINT`, `TOP`):

```sql
SELECT TOP (:max_rows) [order_id], [customer], [amount], [created], [is_paid]
FROM [dbo].[orders]
WHERE (CAST([order_id] AS BIGINT) % :num_splits) = :split_index
ORDER BY [order_id]
```

Verify / Fabric shortcut: identical to §4.3 / §4.4.

---

## 7. SQL Server: multi table

### 7.1 Source

```sql
CREATE TABLE dbo.orders (
    order_id bigint NOT NULL PRIMARY KEY, customer nvarchar(200),
    amount decimal(12,2), created date, is_paid bit
);
CREATE TABLE dbo.inventory (
    sku_id bigint NOT NULL PRIMARY KEY, sku nvarchar(64), warehouse nvarchar(64),
    on_hand int, reorder_at int, updated_at datetime2
);
```

### 7.2 `config.py`

```python
TABLES: list[TableDef] = [
    TableDef(name="orders",    source_table="dbo.orders",    key_column="order_id", num_splits=8),
    TableDef(name="inventory", source_table="dbo.inventory", key_column="sku_id",   num_splits=4),
]
```

Or the equivalent `config.json` `tables` array (see §5.2), the JSON keys are the
same for any dialect.

### 7.3 Launch

```powershell
$env:DB_URL = "mssql+aioodbc://sa:Str0ng!Pass@mssql-host:1433/SalesDb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
.\Manager.ps1 -SkipInstall
```

Create one Fabric shortcut per table.

---

## 8. Environment variable reference (connection & tuning)

| Variable | Default | Notes |
|---|---|---|
| `DB_URL` | `sqlite+aiosqlite:///./poc_source.db` | Scheme selects the dialect |
| `DB_SOURCE_TABLE` | `sales` | Source table/view; schema-qualified allowed |
| `KEY_COLUMN` | *(unset)* | Integer split key; **set it to enable auto-schema** for your table |
| `TABLE_NAME` | `sales` | Iceberg name; canonical path defaults to `db/<server>/<database>/<schema>/<object>` |
| `NUM_SPLITS` | `8` | Split count |
| `TABLE_FORMAT` | `iceberg` | Output format served to Fabric: `iceberg` (Fabric virtualizes to Delta) or `delta` (native `_delta_log`, no conversion, lower lag). See §13 |
| `QUERY_MAX_ROWS` | `500000` | Max rows per split |
| `QUERY_TIMEOUT` | `30` | SQL timeout (seconds) |
| `S3_BUCKET` | `fabric-iceberg-poc` | Bucket Fabric connects to |
| `PORT` | `9000` | HTTP listen port |
| `VALIDATE_SOURCE_SCHEMA` | `1` | Validate declared columns exist (no-op for reflected schemas) |
| `REQUIRE_SIGV4` | `1` | Enforce AWS SigV4 (keys must match `S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY`, or any stored access key) |
| `CORS_ALLOWED_ORIGINS` | *(empty)* | Comma-separated browser origins allowed to call the API |
| `AGENT_HOST_ALLOWLIST` | `127.0.0.1,0.0.0.0,::1,::,localhost` | Hosts or CIDRs accepted in Manager agent registration |
| `ENABLE_STORAGE_PROXY` | `0` | Serve mounted buckets (`config.mounts.json`) as read-only passthrough, see §14 |
| `ENFORCE_MOUNT_AUTH` | `1` | Require SigV4 on mounted buckets even when `REQUIRE_SIGV4=0` |
| `ENABLE_AUDIT_LOG` | `1` | Audit every mounted-object access (identity/bucket/key/bytes) |
| `AUDIT_LOG_FILE` | *(unset)* | Optional append-only audit file |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | *(unset)* | Serve HTTPS at the proxy when both are set |

> The single-table shortcut above is env-only. **Multiple** tables use the
> `tables` array in `config.json` (§1.1) or the `TABLES` list in `config.py`
> (both schema-free). Every environment variable also has a `config.json` key
> (lower-case, without the `S3_`/`DB_` prefix, e.g. `DB_URL`→`db_url`,
> `S3_BUCKET`→`bucket`, `KEY_COLUMN`→`key_column`, `NUM_SPLITS`→`num_splits`).

---

## 9. Advanced: overriding the reflected schema

Reflection covers the common cases. Provide an explicit `schema` on a `TableDef`
only when you need to override a mapped type (e.g. store a text column as a
`date`, as the built-in SQLite demo does):

```python
TABLES = [
    TableDef(
        name="orders",
        source_table="public.orders",
        key_column="order_id",
        schema=[   # explicit override — bypasses reflection for this table
            ColumnDef(field_id=1, name="order_id", iceberg_type="long", nullable=False),
            ColumnDef(field_id=2, name="created",  iceberg_type="date"),
            # …
        ],
    ),
]
```

---

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `RuntimeError: … no integer split key found` | No `KEY_COLUMN` and no integer PK | Set `KEY_COLUMN` to an integer column |
| `RuntimeError: key_column … must be an integer` | Key column is text/uuid/etc. | Choose an integer key |
| `RuntimeError: Could not reflect any columns for …` | Bad name or no permission | Fix `DB_SOURCE_TABLE`; grant `SELECT`/metadata rights |
| Rows appear truncated | `total_rows > num_splits × QUERY_MAX_ROWS` | Raise `NUM_SPLITS` or `QUERY_MAX_ROWS` |
| PostgreSQL: `ModuleNotFoundError: asyncpg` | Driver not installed | `pip install asyncpg` |
| SQL Server: `Can't open lib 'ODBC Driver 18…'` | OS ODBC driver missing | Install *ODBC Driver 18 for SQL Server* |
| SQL Server: SSL/certificate error | Driver 18 encrypts by default | Add `&TrustServerCertificate=yes` (dev) or use a trusted cert |
| A column has the wrong Iceberg type | Reflected type differs from intent | Override with an explicit `schema` (§9) |
| SQL endpoint warns *"Columns of the specified data types are not supported … TIMESTAMP_NTZ"* | Naive `datetime`/`datetime2` mapped to Iceberg `timestamp` (NTZ), which the Fabric SQL endpoint rejects | Fixed by default (`TIMESTAMP_ASSUME_UTC=1` maps naive datetimes to `timestamptz`); set `TIMESTAMP_ASSUME_UTC=0` to opt out |
| SQL endpoint sync fails `404 BlobNotFound` on a table, or Table view spins >5 min / never loads (esp. multi-table) | Fabric's conversion outlives the in-memory Parquet cache (LRU eviction or `PARQUET_CACHE_TTL`), so a split is regenerated on demand with a *different* byte size than the manifest declared → ranged reads miss | Startup now **pins** materialized splits (`PIN_MATERIALIZED_SPLITS=1`, default) so each data file is served byte-identical for the snapshot's life. If you disable pinning, enable `PARQUET_DISK_CACHE=1` and/or raise `PARQUET_CACHE_MAX_BYTES` / `PARQUET_CACHE_TTL`. |
| XTable conversion log: `Error Code: READ_EXCEPTION` (conversion `Failed`) | XTable fetched a Parquet whose bytes didn't match the manifest-declared size (split regenerated after cache eviction/expiry), same size-drift root cause | Keep `PIN_MATERIALIZED_SPLITS=1` (default). Then **delete + recreate** the shortcut (Fabric caches the failed conversion) and re-check `_delta_log/latest_conversion_log.txt`. |

Diagnostics: `GET /readyz`, `GET /_admin/stats`, and `validate_pyiceberg.py`
(reads the actual served bytes).

**Capturing the Fabric conversion timeline** (request tracing, on by default):

| Endpoint | What it shows |
|---|---|
| `GET /_admin/timeline?table=<name>` | Per-table request count, wall-clock span, time spent **in the proxy** vs. **Fabric-side gaps**, per-kind breakdown, slowest requests, biggest gaps, and 4xx/5xx (missing-blob) samples |
| `GET /_admin/trace?table=&kind=&status=&limit=` | Raw request log, newest first; e.g. `?status=404` lists every blob Fabric asked for that we didn't have |
| `POST /_admin/trace/reset` | Clear the buffer right before starting a fresh Fabric run |
| `GET /_admin/objects?table=<name>` | Every served object with **declared** vs **cached** size, non-zero `size_drift_files` / `uncached_data_files` pinpoints the `BlobNotFound` cause |

Toggle with `REQUEST_TRACE=0`; buffer size via `TRACE_BUFFER_SIZE` (default 5000).

**Monitoring dashboard** (`ENABLE_MONITOR=1`, default off): a read-only web UI at
`/_monitor/` (served like `/_config/`) with per-table read/query statistics,
snapshot version, cache/pinned occupancy, and the **query lag** breakdown
(Fabric request → SQL execution → Parquet generation → bytes returned) per data
request. Data comes from `GET /_monitor/api/summary`; `POST /_monitor/api/reset`
clears the buffers before a fresh Fabric run. Run it locally, don't expose publicly.

---

## 11. Optional feature flags (any dialect)

| Flag | Effect |
|---|---|
| `PARQUET_DISK_CACHE=1` | Persist generated Parquet; warm restarts skip regeneration (F5) |
| `ICEBERG_MANIFEST_STATS=1` | Emit column bounds/counts for reader pruning (F3; validate against Fabric first) |
| `ICEBERG_SNAPSHOT_HISTORY=1` | Retain snapshot versions; `POST /_admin/refresh` advances the version (F2) |

See [README.md](../README.md) and [PLANNING.md](PLANNING.md) for the full flag list.

## 12. Data freshness (auto-refresh)

By default the proxy serves a fixed point-in-time snapshot (built once at
startup). Enable **auto-refresh** to have it re-read the source, publish a new
Iceberg snapshot whenever content actually changes, and let Fabric pick up the
new data. Nothing is required on the source SQL server, detection is uniform
across SQLite / PostgreSQL / SQL Server.

| Setting | Default | Effect |
|---|---|---|
| `AUTO_REFRESH=1` | `0` (off) | Turn on content-addressed snapshots + the background poller |
| `REFRESH_POLL_SECONDS` | `600` | Poll interval (matches Fabric's metadata-sync cadence) |
| `REFRESH_STRATEGY` | `auto` | `auto` (dialect probe → skip), `dialect_probe`, `content_hash` (always re-read + hash), `ttl`, `manual` |
| `REFRESH_ALLOW_FULL_PULL` | `0` | In `auto`, allow a full content read when the probe is unavailable |
| `REFRESH_TTL_SECONDS` | `1200` | Re-read window for the `ttl` strategy |

How it works: every chunk file is named by the hash of *its rows* (not the
non-deterministic parquet bytes), so identical data is restart-stable (no churn)
and any change yields a new data-file path + a new `current-snapshot-id`. A new
snapshot version is published **only** when content differs. `POST /_admin/refresh`
forces an immediate materialize + publish (bypassing the probe) and reports which
tables changed. Probes and polls are best-effort and fully wrapped, a failing
probe or query degrades gracefully and never crashes startup or the server.

```powershell
$env:AUTO_REFRESH="1"; $env:REFRESH_POLL_SECONDS="600"; python main.py
# force a refresh now:
Invoke-RestMethod -Method Post http://127.0.0.1:9000/_admin/refresh
```

> Freshness latency includes an unavoidable Fabric-side shortcut-cache lag on
> top of `REFRESH_POLL_SECONDS`. See [FRESHNESS_PLAN.md](FRESHNESS_PLAN.md).

---

## 13. Table format: Iceberg vs Delta (`TABLE_FORMAT`)

The proxy can serve each virtual table in **two on-the-wire formats**. The data
Parquet files are identical; only the *table metadata* differs.

| `TABLE_FORMAT` | What Fabric sees | Trade-off |
|---|---|---|
| `iceberg` *(default)* | Iceberg v2 metadata (`metadata.json` + Avro manifests + `version-hint.text`) | Fabric **virtualizes** Iceberg → Delta on its side. That conversion adds 5 s–2 min of lag, refreshes *at most* once per ~2 min, and is where most conversion-time bugs live. |
| `delta` | A native Delta log (`_delta_log/NNNNNNNNNNNNNNNNNNNN.json`) + the same `data/*.parquet` | Fabric reads the Delta table **directly, no Iceberg→Delta conversion layer**, so lower lag and fewer conversion failure modes. |

Set it once (env or `config.json`):

```powershell
$env:TABLE_FORMAT="delta"; python main.py
```
```jsonc
// config.json
{ "db_url": "…", "table_format": "delta", "tables": [ … ] }
```

Everything else is unchanged: the same content-addressed splits, split **pinning**
(`PIN_MATERIALIZED_SPLITS`), and **auto-refresh** (§12) all work in both modes.
In Delta mode each published snapshot version becomes one Delta commit:

- **commit 0** (`00000000000000000000.json`), `protocol` + `metaData` (the schema)
  + one `add` per split.
- **commit N** (N ≥ 1), a **diff**: `add` only the splits that changed and
  `remove` only the splits they replaced. Because splits are content-addressed,
  unchanged splits keep the same path and carry forward untouched.

Delta commit files are memoized in-process and stay contiguous from commit 0 even
after old snapshot data is pruned. A stale Fabric reader may still ask for a
prior version's data file until it re-syncs the log; those files stay pinned and
served (within `SNAPSHOT_HISTORY_LIMIT`), so you don't get "underlying location
does not exist" errors mid-refresh. Point your Fabric S3 shortcut at
`db/<server>/<database>/<schema>/<object>`, Fabric auto-detects the `_delta_log/`.

> Design notes and the full action layout: [DELTA_FORMAT.md](DELTA_FORMAT.md).

## 14. Storage proxy: mounted buckets (files & object stores)

Independently of the DB→table virtualization, the same S3 endpoint can serve
**existing files** from a storage backend as **read-only byte passthrough**. This
is **additive**: a bucket with a **mount** streams bytes from its backend; every
other bucket (including the DB warehouse) resolves exactly as before. Grounded in
[storage/mounts.py](../storage/mounts.py), [storage/passthrough.py](../storage/passthrough.py),
and [config.mounts.example.json](../config.mounts.example.json).

Turn it on with `ENABLE_STORAGE_PROXY=1` and a `config.mounts.json` (gitignored),
or use the config-builder **Storage** tab.

### 14.1 Mount table (`config.mounts.json`)

```jsonc
{
  "mounts": [
    // NFS/SMB share mounted by the OS — zero extra deps.
    { "bucket": "secure-nfs", "backend": "local", "root": "/mnt/finance", "read_only": true },

    // Native S3 / MinIO / S3-compatible bucket ('root' = upstream bucket).
    { "bucket": "s3vault", "backend": "s3", "root": "reports-bucket", "prefix": "2026/",
      "endpoint": "https://minio.local:9000", "region": "us-east-1",
      "addressing_style": "path", "credential": "s3vault", "read_only": true },

    // Native Azure Blob / ADLS Gen2 container ('root' = container).
    { "bucket": "blobvault", "backend": "azure", "root": "reports",
      "account": "mystorageacct", "credential": "blobvault", "read_only": true },

    // Public bucket needs an explicit credential-less auth mode.
    { "bucket": "adls-open", "backend": "azure", "root": "public",
      "account": "opendatalake", "auth": "default", "read_only": true }
  ]
}
```

- `bucket`: the S3 bucket Fabric/clients use; must differ from `S3_BUCKET` and be
  a valid S3 name.
- `root`: filesystem path (`local`) / upstream bucket (`s3`) / container (`azure`).
- `prefix`: optional; confine serving to a subtree (also `..`-hardened).
- `credential`: id of an encrypted upstream credential in the store (never inline).
- `auth`: for a credential-less mount, an explicit mode (S3: `anonymous`/`instance`;
  Azure: `default`/`managed_identity`/`anonymous`).
- Mounts are **read-only** in v1.

Install the SDK for native backends: `pip install '.[s3proxy]'` (S3),
`pip install '.[azureblob]'` (Azure). `local` needs nothing.

### 14.2 Backends & install extras

| Backend | `root` is | Extra | Auth modes (via `credential` blob or `auth`) |
|---|---|---|---|
| `local` | a filesystem path (NFS/SMB mount) | — | n/a |
| `s3` | the upstream S3 bucket | `.[s3proxy]` | static, session, assume_role, web_identity, profile, sso, instance, process, anonymous |
| `azure` | the container | `.[azureblob]` | connection_string, account_key, sas, aad_client_secret, managed_identity, default, anonymous |

Upstream secrets live encrypted in the credential store and are set in the config
builder (**Storage → mount editor**) or via `/_config/api/{s3,azure}-credentials`.

### 14.3 Access keys & authorization

When a mount exists, issue scoped **access keys** so each client reaches only its
allowed buckets/prefixes:

| Setting | Default | Effect |
|---|---|---|
| `ENABLE_STORAGE_PROXY` | `0` | Serve mounted buckets |
| `ENFORCE_MOUNT_AUTH` | `1` | Require SigV4 on mounts even when `REQUIRE_SIGV4=0` |
| `REQUIRE_SIGV4` | `1` | Enforce SigV4 on **all** buckets |
| `CORS_ALLOWED_ORIGINS` | *(empty)* | Comma-separated browser origins allowed to call the API |
| `ENABLE_AUDIT_LOG` | `1` | Audit every mounted-object access |
| `AUDIT_LOG_FILE` | *(unset)* | Optional append-only audit file |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | *(unset)* | Serve HTTPS at the proxy |

Manage keys in the config-builder **Storage → Access keys** panel (create returns
the secret once; rotate/delete supported). The legacy single key stays a wildcard
until the first access key is created. Full security model: [SECURITY.md](SECURITY.md).
