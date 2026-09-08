# Chapter 13: Open Mirroring

Open Mirroring writes selected source tables into a Fabric mirrored database
landing zone. It is separate from shortcut table publishing: shortcut tables
serve S3 reads, while mirror targets create numbered landing-zone batches.

## 13.1 Configure a target

Copy `config.open_mirror.example.json` to `config.open_mirror.json`. A target
references a configured source connection and the Fabric workspace, mirrored
database, and landing-zone identifiers.

```json
{
  "open_mirror": {
    "open_mirror_targets": [
      {
        "id": "fabric-sales",
        "connection": "default",
        "landing_zone_root": "https://onelake.dfs.fabric.microsoft.com/<workspace-id>/<mirrored-database-id>/Files/LandingZone",
        "workspace_id": "<workspace-id>",
        "mirrored_database_id": "<mirrored-database-id>",
        "enabled": true,
        "tables": [
          {
            "name": "sales",
            "source_table": "dbo.sales",
            "target_table": "sales",
            "key_column": "id",
            "watermark_column": "ModifiedDate"
          }
        ]
      }
    ]
  }
}
```

The proxy needs an Entra identity with access to the landing zone and mirrored
database. Configure it as described in [Chapter 7](07-security.md). Do not place
client secrets in this file.

## 13.2 Choose change tracking

A table with `watermark_column` uses source-incremental upserts ordered by the
watermark and key columns. The watermark must be non-null and advance whenever
a row changes. An index on `(watermark_column, key_column)` prevents large scans.

Without a watermark, the proxy performs a full source scan and compares row hashes.
That detects inserts, updates, and deletes, but it reads the table on every cycle.
Use `mode: "initial"` for an explicit full load. Invocation mode overrides table
mode, which overrides the global `OPEN_MIRROR_MODE`.

## 13.3 Run safely

Set `OPEN_MIRROR_PUBLISH=1` for scheduled publishing, or use **Publish now** in
the Config Builder. The scheduled interval is `OPEN_MIRROR_INTERVAL_SECONDS`.
Use **Dry run** and **Check health** before the first publish and after changing
a connection, source schema, or Fabric target.

Check health is read-only. It checks source access, control columns, Fabric
mirroring status, landing-zone access, and Manager credentials. It does not read
source rows, write files, start Fabric capacity, or advance mirror state.

`OPEN_MIRROR_MAX_ROWS`, `OPEN_MIRROR_MAX_PAGES_PER_CYCLE`, and
`OPEN_MIRROR_MAX_ROWS_PER_CYCLE` bound source load. Begin large initial loads
with a finite page size and cycle limit.

## 13.4 State and recovery

`OPEN_MIRROR_STATE_DIR` stores cursors and pending-batch recovery data outside
the landing zone. Set it to durable storage for a service deployment. Set
`OPEN_MIRROR_ENCRYPT_STATE=1` when cursors or snapshot keys are sensitive.

Corrupt or unreadable state stops the affected table. Review the state, then use
the explicit reset action to authorize a fresh initial load. Do not delete state
files to force a reload; that bypasses the recovery guard.

## 13.5 Retention and tokenization

`fabric_retention_days` configures Fabric mirrored-database retention from 1 to
30 days. `cleanup_retention_days` controls processed files in
`_FilesReadyToDelete`; a table value overrides the target value. Inspect cleanup
as a dry run before deleting eligible files.

Mirror tables use the same central tokenization selections as shortcut tables.
Key and watermark columns must remain available to the publisher even when they
are omitted from output. Run [TOKENIZATION_OPEN_MIRROR_UAT.md](../TOKENIZATION_OPEN_MIRROR_UAT.md)
before enabling tokenized production mirrors.

## 13.6 Next

Continue to [Chapter 14: Tokenization policies](14-tokenization-policies.md) to
create approved policies and assign them to tables, mounts, and mirror targets.
