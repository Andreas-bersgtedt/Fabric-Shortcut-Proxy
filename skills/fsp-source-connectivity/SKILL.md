---
name: fsp-source-connectivity
description: "Set up and troubleshoot Fabric Shortcut Proxy source database connectivity and driver prerequisites. Use for DB_URL, PostgreSQL, SQL Server, Oracle, Redshift, Teradata, Impala, ODBC, Python extras, TLS, firewall rules, schema reflection, and source smoke tests."
argument-hint: "Provide the database engine, host/network path, authentication mode, and redacted DB_URL scheme."
---

# Fabric Shortcut Proxy Source Connectivity

## Use When

- Adding a source database or changing `DB_URL`.
- Installing Python database extras or OS-level ODBC drivers.
- Testing DNS, TCP, TLS, authentication, reflection, or source permissions.
- Diagnosing `/readyz` failures caused by the source database.

## Driver Matrix

| Source | URL/driver family | Prerequisite |
| --- | --- | --- |
| SQLite | `sqlite+aiosqlite` | Included with the base setup |
| PostgreSQL | `postgresql+asyncpg` | `[postgres]` or `[drivers]` extra |
| SQL Server | `mssql+aioodbc` | `[drivers]` plus OS ODBC Driver 18 for SQL Server |
| Oracle | `oracle+oracledb` | `[oracle]` or `[drivers]` extra |
| Amazon Redshift | `redshift+redshift_connector` | `[redshift]` or `[drivers]` extra |
| Teradata | `teradatasql` | `[teradata]` or `[drivers]` extra |
| Apache Impala | `impala` | `[impala]` or `[drivers]` extra |

The Manager launchers install the supported driver set through the project extras. When installing manually, use the same virtual environment that runs Manager:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[drivers]"
```

```bash
.venv/bin/python -m pip install -e '.[drivers]'
```

For encrypted credentials on Linux, install `.[credentials]`. For storage-proxy upstreams, install `.[s3proxy]` or `.[azureblob]` separately.

## Connection Setup

1. Confirm the source hostname resolves from the proxy host or AKS pod.
2. Confirm the source port is reachable through VNet routing, firewall, NSG, VPN, or ExpressRoute.
3. Install the Python extra and any OS driver before testing reflection.
4. Set `DB_URL` through a protected environment source or `config.connection.json`; never commit live credentials.
5. Set `DB_SOURCE_TABLE`/`source_table` and `KEY_COLUMN`/`key_column`. Views generally need an explicit key column because they often have no primary key.
6. Start the proxy and inspect logs for dialect selection, connection, reflection, and snapshot errors.
7. Verify `/healthz`, then `/readyz`, then one authenticated S3 object read.

Use URL schemes that match the installed driver:

```text
postgresql+asyncpg://user:password@host:5432/database
mssql+aioodbc://user:password@host:1433/database?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes
```

URL-encode reserved characters in usernames and passwords. Prefer passwordless identity or the encrypted credential store where supported. Redact the password when sharing a URL or log line.

## SQL Server and ODBC

On Windows, install Microsoft ODBC Driver 18 for SQL Server and ensure the driver name in the URL matches the installed name. On Linux, install the Microsoft driver and unixODBC, then verify:

```bash
odbcinst -q -d
ldconfig -p | grep libodbc
```

The expected driver is `[ODBC Driver 18 for SQL Server]`. `aioodbc`/`pyodbc` alone is not enough. If only Driver 17 exists, use the matching driver name intentionally and document the exception.

## Layered Smoke Tests

Run each test from the proxy runtime location:

```bash
getent hosts <source-host>
nc -vz <source-host> <source-port>
.venv/bin/python -c "import sqlalchemy; print(sqlalchemy.__version__)"
.venv/bin/python -c "import asyncpg; print('asyncpg OK')"
```

Use the engine-specific driver import only when applicable. Do not put secrets in shell history; prefer a protected environment file or the project credential store. For SQL Server, verify ODBC separately before trying the application URL.

## Readiness and Reflection Diagnosis

- `/healthz` failing indicates a process, bind, or port problem, not necessarily a database problem.
- `/healthz` succeeding while `/readyz` returns `503` usually means source reachability, credentials, schema reflection, snapshot materialization, or missing Agent readiness.
- Connection succeeds but reflection fails: check database/schema/table permissions, quoted identifiers, `source_table`, and `KEY_COLUMN`.
- Reflection succeeds but reads fail: check key-column type, dialect-specific SQL, source-side row permissions, and split planning.
- A successful TCP test does not prove TLS negotiation or database authentication; perform the application-level test next.

## Network and TLS Checklist

Confirm the source database allows the proxy's actual egress IP or subnet. For AKS, test from an Agent pod because node, subnet, NAT, and private DNS behavior may differ from the jump box. Verify private DNS zones, NSG rules, database firewall rules, certificates, and server-name requirements. Avoid `TrustServerCertificate=yes` in production unless the certificate trust decision is explicit.

## References

- [Configuration manual](../../docs/CONFIGURATION.md)
- [Linux deployment: source drivers](../../docs/installation/Linux_Deployment.md)
- [Windows deployment: source drivers](../../docs/installation/Windows_Deployment.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Linux troubleshooting guide](../../docs/LINUX_MANAGER_TROUBLESHOOTING.md)