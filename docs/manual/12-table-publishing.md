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
      "split_target_rows": 100000,
      "table_format": "delta"
    }
  ]
}
```

`name` appears in the Fabric shortcut path. `source_table` is the schema-qualified
source object. Omit `connection` to use `default`. `key_column` is required for
views and recommended for every table because it anchors split planning.

## 12.2 Choose split behavior

`modulo` assigns rows by key remainder. `range` reads contiguous key slices.
`date` partitions temporal keys. `auto` selects the strongest available option.
Use `split_balance=count` for skewed keys; SQL Server and PostgreSQL can use
statistics histograms, while other dialects use a bounded sample or `NTILE` query.

`split_target_rows` controls dynamic split count. `num_splits` fixes a count for
small or predictable tables. Each split is bounded by `query_max_rows`; increase
the cap when a configured split may contain more rows than the global limit.

## 12.3 Select an output format

Set `table_format` per table, or use the global `TABLE_FORMAT` setting.

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
