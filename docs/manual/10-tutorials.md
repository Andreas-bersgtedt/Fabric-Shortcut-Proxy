# Chapter 10: Tutorials

This chapter ties the earlier chapters into task-oriented walkthroughs. Each tutorial is
end to end and copy-paste runnable, and points back to the chapter that explains the model.
Do them in order for a first deployment, or pick the one that matches your task.

- [10.1 The five-minute demo (no source database)](#101-the-five-minute-demo-no-source-database)
- [10.2 A SQL Server table to a Fabric shortcut](#102-a-sql-server-table-to-a-fabric-shortcut)
- [10.3 Serving a file share as passthrough](#103-serving-a-file-share-as-passthrough)
- [10.4 Tokenizing a PII column](#104-tokenizing-a-pii-column)

## 10.1 The five-minute demo (no source database)

Goal: see the S3 path work before wiring a real source. With no `DB_URL`, the proxy seeds
and serves a local SQLite `sales` table (50,000 rows across 8 splits).

```powershell
# from the repository root, in the activated .venv
python main.py
```

In another terminal, confirm the service and list the demo table objects:

```powershell
curl http://localhost:9000/healthz          # {"status":"ok"}
curl http://localhost:9000/readyz           # 200 once the snapshot is built
# list the warehouse bucket
curl "http://localhost:9000/fabric-iceberg-poc?list-type=2&prefix=db/"
```

To validate the served objects with a reference Iceberg reader:

```powershell
python validate_pyiceberg.py
```

This exercises the warehouse read path from chapter 3 without any external dependency. Stop
the proxy with Ctrl+C. The rest of the tutorials point at real sources.

## 10.2 A SQL Server table to a Fabric shortcut

Goal: publish one SQL Server table and read it from a Fabric shortcut through an OPDG. This
combines chapter 4 (install), chapter 5 (configure), and chapter 6 (connect).

### Prerequisites

- The OS ODBC Driver 18 for SQL Server (chapter 4).
- A read-only SQL login scoped to the table you expose.
- An On-Premises Data Gateway that can reach the proxy host (chapter 6).

### Step 1: point the proxy at the table

Set the connection string and the key column, then start the proxy. Use `delta` so Fabric
reads `_delta_log` directly.

```powershell
$env:DB_URL = "mssql+aioodbc://appreader:secret@sql-host:1433/salesdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
$env:DB_SOURCE_TABLE = "dbo.orders"
$env:KEY_COLUMN = "order_id"
$env:TABLE_FORMAT = "delta"
$env:REQUIRE_SIGV4 = "1"
$env:S3_ACCESS_KEY_ID = "AKIA-your-key"
$env:S3_SECRET_ACCESS_KEY = "your-secret"
python main.py
```

The schema is reflected from `dbo.orders` at startup; you do not write a column list. For
several tables, use `config.tables.json` instead (chapter 5, §5.6).

### Step 2: confirm the objects

```powershell
curl http://localhost:9000/readyz
curl "http://localhost:9000/fabric-iceberg-poc?list-type=2&prefix=db/"
```

The Delta entry point is `db/<server>/<database>/<schema>/<object>`; note the exact path for
your server and database (chapter 2, §2.5).

### Step 3: create the Fabric shortcut

In a Fabric Lakehouse: New shortcut → Amazon S3 compatible.

| Field | Value |
|-------|-------|
| URL | `http://<proxy-private-ip>:9000` |
| Access key / Secret | your `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` |
| Data gateway | select your OPDG |
| Path | browse `fabric-iceberg-poc` → the table folder under `db/...` |

Only the queried rows traverse the gateway. If the shortcut resolves and shows rows, the path
works end to end. For 403s, recheck the access key and its ACL (chapter 7); for a stuck
`readyz`, recheck the connection string and driver (chapter 8, §8.10).

## 10.3 Serving a file share as passthrough

Goal: expose an existing folder (a local path or an OS-mounted NFS/SMB share) as a read-only
bucket, with no database involved. This uses the storage proxy from chapters 2 and 6.

### Step 1: enable the storage proxy and add a mount

Create `config.mounts.json` next to `main.py`. The mount bucket must differ from the
warehouse bucket.

```json
{
  "mounts": [
    { "bucket": "secure-nfs", "backend": "local", "root": "/mnt/finance", "read_only": true }
  ]
}
```

```powershell
$env:ENABLE_STORAGE_PROXY = "1"
$env:REQUIRE_SIGV4 = "1"
$env:S3_ACCESS_KEY_ID = "AKIA-your-key"
$env:S3_SECRET_ACCESS_KEY = "your-secret"
python main.py
```

`ENFORCE_MOUNT_AUTH` is on by default, so the mount is never served anonymously even though
it is a file share (chapter 7, §7.3).

### Step 2: confirm the mount lists

```powershell
# the mounted bucket appears in ListBuckets and lists its objects
curl "http://localhost:9000/"                               # includes secure-nfs
curl "http://localhost:9000/secure-nfs?list-type=2"
```

Every key is normalized and confined to the mount subtree; `..` traversal is rejected
(chapter 7, §7.5).

### Step 3: shortcut it

Create the Fabric shortcut against the `secure-nfs` bucket exactly as in tutorial 10.2; only
the bucket name changes. For an S3/MinIO or Azure mount, add the backend fields and a
credential id instead of an inline secret (chapter 6, §6.8).

## 10.4 Tokenizing a PII column

Goal: publish a table where an email column is replaced by a stable, keyed token so plaintext
never leaves the source. This uses the tokenization feature from chapter 7.

### Step 1: set the token key in the environment

The key is referenced by a logical name in configuration and resolved only from the
environment. A key reference of `customer-pii-v1` maps to the variable
`FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1` (uppercased, non-alphanumerics become underscores).

```powershell
$env:FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1 = "replace-with-a-long-random-value"
```

Never place the key in `config.tables.json` or the connection string.

### Step 2: define the column policy

In `config.tables.json`, alias the source `email` column to a token output and remove the raw
`ssn` column by omitting it from the schema:

```json
{
  "tables": [
    {
      "name": "customers_safe",
      "source_table": "dbo.customers",
      "key_column": "customer_id",
      "schema": [
        { "field_id": 1, "name": "customer_id", "type": "long", "nullable": false },
        {
          "field_id": 2,
          "name": "email_token",
          "source": "email",
          "type": "string",
          "transform": {
            "kind": "deterministic_hash",
            "key_ref": "customer-pii-v1",
            "domain": "customer-email",
            "normalization": "trim_lower"
          }
        }
      ]
    }
  ]
}
```

Or set the same policy per column in the config builder (chapter 5, §5.7). Deterministic
tokens keep equality, so `GROUP BY email_token` still groups the same customers, but the
address itself is never published.

### Step 3: start and verify

```powershell
$env:DB_URL = "mssql+aioodbc://appreader:secret@sql-host:1433/salesdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
python main.py
```

Startup must complete without a configuration error. Generated SQL uses the engine's native
hash (`HASHBYTES('SHA2_256', ...)` on SQL Server); the token key appears in logs only as a
redacted `fsp_token_*` parameter (chapter 7, §7.8). Read `customers_safe` through Fabric and
confirm `email_token` is a 64-character hex value, equal rows share a token, and `ssn` is
absent from the schema.

The full acceptance procedures are in [TOKENIZATION_UAT.md](../TOKENIZATION_UAT.md) (SQL
Server) and [TOKENIZATION_MULTI_DIALECT_UAT.md](../TOKENIZATION_MULTI_DIALECT_UAT.md)
(PostgreSQL, Oracle, Databricks).

## 10.5 Where to go next

- Harden the deployment before exposing it: [Chapter 7: Security](07-security.md).
- Scale to more agents and add high availability: [Chapter 8: Operations](08-operations.md).
- Look up any setting, flag, or path: [Chapter 9: Reference](09-reference.md).

Return to the [manual index](README.md) for the full table of contents.
