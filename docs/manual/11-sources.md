# Chapter 11: Sources

A source is a database connection the proxy reads from. Sources are defined in
`config.connection.json`; tables choose one by its connection id. Store the URL
in the encrypted credential store or an environment variable, not in Git.

## 11.1 Default source

The `connection` object defines the default source. Its id is always `default`.

```json
{
  "connection": {
    "db_url": "postgresql+asyncpg://appreader:<password>@pg.example.com:5432/sales",
    "query_timeout_seconds": 30,
    "query_max_rows": 500000,
    "validate_source_schema": true
  }
}
```

Set `DB_URL` to override `db_url` at process start. A secret stored in the
credential store hydrates `DB_URL` before the proxy loads configuration.

## 11.2 Additional sources

Use `connections` for tables from different databases or dialects. Each entry
needs a unique `id`; use that id in a table's `connection` field.

```json
{
  "connections": [
    {
      "id": "warehouse_pg",
      "db_url": "postgresql+asyncpg://appreader:<password>@pg.example.com:5432/sales"
    },
    {
      "id": "finance_sql",
      "db_url": "mssql+aioodbc://appreader:<password>@sql.example.com:1433/finance?driver=ODBC+Driver+18+for+SQL+Server"
    }
  ]
}
```

An environment value named `DB_URL_WAREHOUSE_PG` overrides the stored URL for
`warehouse_pg`. Convert the connection id to uppercase and replace non-alphanumeric
characters with underscores. The default source uses `DB_URL`.

## 11.3 Source requirements

Give every source principal read access only to the tables and views the proxy
will serve. Views require an explicit, stable `key_column`. For large tables,
index the key column and the watermark column used by Open Mirroring.

The connection scheme selects the dialect. Install its driver before starting
(see [Chapter 4](04-installation.md)). Databricks also requires an HTTP path to
a SQL warehouse. Oracle, Databricks, Redshift, Teradata, and Impala use the
synchronous execution path; control source pressure with `SOURCE_MAX_CONCURRENCY`.

## 11.4 Validate a source

Use **Sources** in the Config Builder to test a connection, inspect schemas, and
save an encrypted credential. A successful test proves network access and driver
configuration; it does not publish a table.

At startup, source-schema validation checks every configured table. A failed
source is quarantined when `QUARANTINE_FAILED_TABLES=1`, allowing healthy tables
to remain available. Inspect `/readyz` and the Manager monitor for quarantined
tables, repair the source, then wait for the retry interval or restart.

## 11.5 Next

Continue to [Chapter 12: Table publishing](12-table-publishing.md) to select
source objects, split strategy, output format, and refresh behavior.
