# Chapter 12: Table Publishing

Table publishing exposes a source table or view as Iceberg or Delta objects in
the warehouse bucket. Define the registry in `config.tables.json`; the proxy
validates it and resolves source columns at startup.

## 12.1 Define a table

```json
{
  "tables": [
    {
      "name": "orders",
      "source_table": "public.orders",
      "connection": "warehouse_pg",
      "key_column": "order_id",
      "split_strategy": "range",
      "split_target_rows": 100000
    }
  ]
}
```

`name` appears in the Fabric shortcut path. `source_table` is the schema-qualified
source object. Omit `connection` to use `default`. Normal startup requires an
integer (`int` or `long`) `key_column`; specify it for views, Databricks, and
Impala, which cannot rely on primary-key reflection.

## 12.2 Choose split behavior

`modulo` assigns rows by key remainder. `range` reads contiguous integer-key
slices. `auto` selects from available strategies. The lower-level `date`
strategy accepts temporal keys, but normal table startup currently rejects them.
Use `split_balance=count` for skewed keys; SQL Server and PostgreSQL can use
statistics histograms, while other dialects use a bounded sample or `NTILE` query.

`split_target_rows` and an enabled byte target control dynamic split count. To use
`num_splits` as a fixed count in the native table registry, set both `split_target_rows` and
`split_target_bytes` to `0` for that table. The effective row cap is the maximum
of the connection's `query_max_rows`, the row target, and the byte target divided
by the estimated compressed bytes per row (rounded up). Size the cap for the
largest split, including skew, to avoid truncating rows.

## 12.3 Select an output format

Set the global `TABLE_FORMAT` environment variable or `table_format` in
`config.performance.json`. The table registry does not support a per-table format.

- `delta` publishes a native `_delta_log` and is usually the direct Fabric path.
- `iceberg` publishes Iceberg metadata, manifests, and Parquet splits.

Both formats use the same source projection, split plan, cache, and refresh model.
See [Chapter 2](02-concepts.md) and [DELTA_FORMAT.md](../DELTA_FORMAT.md) for
format-specific object paths and metadata behavior.

## 12.4 Publish and verify

With `MATERIALIZE_MODE=eager`, startup resolves every enabled table and builds
its current snapshot. Check `/readyz` before creating a shortcut. With `lazy`,
the first metadata request creates the table snapshot. `virtual` regenerates
splits on demand and is for immutable or snapshot-isolated sources only.

Use the Config Builder **Tables** area to add, inspect, and apply table entries.
After a schema, split, connection, or output-format change, restart the Manager
and confirm the table has a new healthy snapshot. Use `/_admin/objects?table=`
to compare declared and cached sizes when Fabric reports a missing or size-mismatched
object.

## 12.5 Refresh and removal

`AUTO_REFRESH=1` checks tables on the configured cadence. `auto` uses a cheap
source probe when available; set `REFRESH_ALLOW_FULL_PULL=1` only when a full
content check is acceptable. `POST /_admin/refresh` performs an operator-requested
refresh.

Disable a table with `"enabled": false` before removing it from the registry
when you need to preserve configuration for investigation. Removing an entry
stops new publication; clean unreferenced artifacts with retention GC after the
retained snapshot window has passed.

## 12.6 Next

Continue to [Chapter 13: Open Mirroring](13-open-mirroring.md) to publish selected
source changes into a Fabric mirrored database landing zone.
