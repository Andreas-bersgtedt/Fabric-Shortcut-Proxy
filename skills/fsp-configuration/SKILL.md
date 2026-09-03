---
name: fsp-configuration
description: "Configure Fabric Shortcut Proxy connections, tables, output formats, split config files, mounts, credentials, access keys, security, freshness, and the Config Builder UI. Use when adding a source, changing DB_URL, configuring S3/Azure mounts, or diagnosing configuration precedence."
argument-hint: "Describe the source, tables or mounts, security requirements, and deployment platform."
---

# Fabric Shortcut Proxy Configuration

## Use When

- Adding or changing a PostgreSQL, SQL Server, SQLite, Oracle, Redshift, Teradata, or Impala source.
- Registering tables, key columns, schemas, split counts, or output format.
- Configuring local, S3, MinIO, Azure Blob, or ADLS mounts.
- Managing encrypted credentials, access keys, ACLs, or Key Vault.
- Using the Config Builder.

## Precedence and File Layout

Effective values are selected in this order:

1. Environment variable.
2. Split JSON file such as `config.connection.json`, `config.tables.json`, `config.performance.json`, `config.system.json`, `config.freshness.json`, `config.mounts.json`, or `config.open_mirror.json`.
3. Built-in default.

Use the matching `config.*.example.json` as the starting point. Real config files are intended to be local and gitignored. With a separate config directory:

```powershell
$env:FSP_CONFIG_DIR = "C:\deploy\fsp-config"
.\Manager.ps1 -SkipInstall
```

```bash
FSP_CONFIG_DIR=/etc/fabric-shortcut-proxy bash ./Manager.sh --skip-install
```

When a value appears in both an environment file and JSON, change the higher-precedence environment value or remove it. Do not debug the losing file.

## Configure a Database Table

1. Set `DB_URL` or `connection.db_url` using the correct SQLAlchemy scheme.
2. Set `DB_SOURCE_TABLE`/`source_table` and `KEY_COLUMN`/`key_column`; views usually require an explicit key column.
3. Add each table to `config.tables.json` for multi-table deployments.
4. Choose `TABLE_FORMAT=iceberg` or `TABLE_FORMAT=delta`.
5. Install the database driver extra. SQL Server additionally requires the OS ODBC Driver 18.
6. Start or restart the Manager and verify reflection, snapshot creation, `/readyz`, and a representative S3 object read.

Examples:

```text
postgresql+asyncpg://user:password@host:5432/database
mssql+aioodbc://user:password@host:1433/database?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

Prefer integer split keys. Non-integer keys use deterministic row-number sharding. `MATERIALIZE_MODE=virtual` cannot be combined with `AUTO_REFRESH=1`; choose eager materialization for refresh or disable refresh for virtual reads.

## Config Builder and Security

Enable the builder only on a trusted administrative network:

```powershell
$env:ENABLE_CONFIG_BUILDER = "1"
$env:MANAGER_AUTH_ENABLED = "1"
.\Manager.ps1 -SkipInstall
```

Use `/_config/` for sources, reflected tables, mounts, open mirroring, credentials, access keys, ACLs, and encrypted backups. Keep `FSP_CRED_KEY` stable across restarts and hosts that share the credential store. Never place live passwords, SAS tokens, or connection strings in committed examples.

## Mounts and Authentication

- `local` requires an OS-mounted NFS/SMB path.
- `s3` requires the optional S3 proxy extra and upstream credential configuration.
- `azure` requires the optional Azure Blob extra and an appropriate credential mode.
- `ENFORCE_MOUNT_AUTH` is on by default; mounted buckets require SigV4 even if general SigV4 enforcement is off.
- Scope proxy access keys to buckets and prefixes. Treat the legacy single key as a wildcard until scoped keys are configured.

## References

- [Configuration manual](../../docs/CONFIGURATION.md)
- [Manual configuration chapter](../../docs/manual/05-configuration.md)
- [Security](../../docs/SECURITY.md)
- [Storage virtualization](../../docs/s3virtulization.md)
- [Backup and restore](../../docs/BACKUP_RESTORE.md)