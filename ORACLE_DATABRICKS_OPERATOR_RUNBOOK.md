# Oracle + Databricks Operator Runbook

Date: 2026-07-26
Audience: Platform operators and on-call engineers
Scope: Real-environment validation and operations for Oracle and Databricks SQL Warehouse sources.

## 1. Purpose
This runbook covers how to:
- configure and validate Oracle + Databricks source connectivity,
- execute real-environment integration smoke tests,
- diagnose the most common connectivity/reflection/query failures,
- roll back quickly to known-good settings.

## 2. Capability Summary
Current behavior is driven by the capability matrix in db/capabilities.py.

- Oracle:
  - Execution mode: sync-threadpool fallback
  - Reflection: tables/views + PK reflection supported
  - Split planning: modulo and range eligible (subject to key bounds)
- Databricks SQL Warehouse:
  - Execution mode: sync-threadpool fallback
  - Reflection: table/view listing supported
  - PK reflection: may be unavailable depending on connector/catalog metadata
  - Required connection field: http_path

## 3. Prerequisites

### 3.1 Local proxy environment
1. Python 3.11+
2. Project virtual environment initialized:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
```

### 3.2 Oracle prerequisites
1. Reachable Oracle host + service/database
2. Credentials with read permissions on target schemas
3. Python driver support via SQLAlchemy URL oracle+oracledb

Recommended environment variables:

```powershell
$env:ORACLE_HOST = "oracle-host.company.net"
$env:ORACLE_PORT = "1521"
$env:ORACLE_DATABASE = "ORCLPDB1"
$env:ORACLE_USERNAME = "proxy_reader"
$env:ORACLE_PASSWORD = "<secret>"
```

### 3.3 Databricks prerequisites
1. Reachable Databricks SQL endpoint
2. PAT token with query/read privileges
3. Valid SQL Warehouse HTTP path

Recommended environment variables:

```powershell
$env:DATABRICKS_HOST = "dbc-<workspace>.cloud.databricks.com"
$env:DATABRICKS_PORT = "443"
$env:DATABRICKS_TOKEN = "<dapi-token>"
$env:DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/<warehouse-id>"
# Optional:
$env:DATABRICKS_CATALOG = "main"
$env:DATABRICKS_SCHEMA = "default"
```

## 4. Runtime Configuration

### 4.1 Oracle source DB_URL

```powershell
$env:DB_URL = "oracle+oracledb://proxy_reader:<password>@oracle-host.company.net:1521/ORCLPDB1"
```

### 4.2 Databricks source DB_URL

```powershell
$env:DB_URL = "databricks://token:<token>@dbc-<workspace>.cloud.databricks.com:443?http_path=%2Fsql%2F1.0%2Fwarehouses%2F<warehouse-id>"
```

If `http_path` is missing, config-builder connection validation is expected to fail.

## 5. Preflight Validation Checklist
1. Start proxy/manager locally.
2. Confirm liveness/readiness:

```powershell
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/readyz
```

3. Use config builder at http://127.0.0.1:9000/_config/ and confirm:
- Connect succeeds.
- Capability payload is returned.
- Warnings are shown for sync-threadpool fallback.
- Table/view listing works.

4. Validate a minimal scalar query by exercising object reads or integration tests.

## 6. Real-Environment Integration Smoke Tests

Test file: tests/test_integration_oracle_databricks.py

Behavior:
- Tests are skipped unless required env vars are set.
- Oracle test validates reflection + SELECT 1 FROM dual query path.
- Databricks test validates reflection + SELECT 1 query path.

Run:

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_integration_oracle_databricks.py -q
```

Optional smoke table inspection:

```powershell
$env:INTEGRATION_SMOKE_TABLE = "schema.table_name"
```

## 7. Operations Guidance

### 7.1 Concurrency tuning
Oracle/Databricks currently run in sync-threadpool fallback mode.
Tune cautiously:
- SOURCE_MAX_CONCURRENCY starts low and increase gradually.
- Monitor SQL latency and timeouts under load.

### 7.2 Split key policy
For environments where PK reflection is weak/inconsistent (especially Databricks catalogs), set `key_column` explicitly in table definitions.

### 7.3 Safe fallback expectations
- Range planning falls back to modulo if key-bound capabilities are unavailable.
- Reflection failures should degrade gracefully to diagnostics rather than crashing startup where possible.

## 8. Troubleshooting Matrix

### 8.1 Databricks connect fails with required field error
Symptom: connect endpoint returns error mentioning `http_path`.
Action: provide DATABRICKS_HTTP_PATH or add `http_path` query parameter in DB_URL.

### 8.2 Query path times out under load
Symptom: query_timeout / SourceUnavailable errors.
Action:
1. Reduce SOURCE_MAX_CONCURRENCY.
2. Increase QUERY_TIMEOUT conservatively.
3. Confirm source-side warehouse/session capacity.

### 8.3 Reflection returns empty table list
Symptom: connect succeeds but list is empty.
Action:
1. Verify catalog/schema permissions.
2. Provide explicit catalog/schema for Databricks.
3. Test direct query with execute_scalar path.

### 8.4 Split planning falls back unexpectedly
Symptom: logs show range_planning_fallback_modulo.
Action:
1. Confirm integer key column exists and is configured.
2. Validate source min/max query permissions.
3. Keep modulo fallback if source flavor metadata limits apply.

## 9. Rollback Plan

### 9.1 Immediate rollback (source flavor)
1. Revert DB_URL to previously validated PostgreSQL/SQL Server setting.
2. Restart manager/agent.
3. Verify healthz/readyz and object reads.

### 9.2 Code rollback
1. Checkout prior release/commit tag.
2. Re-deploy binaries/container.
3. Run quick smoke checks:

```powershell
curl http://127.0.0.1:9000/healthz
curl http://127.0.0.1:9000/readyz
```

## 10. Change Management
Before enabling Oracle/Databricks in production:
1. Run integration smoke tests against real target.
2. Capture baseline latency and error rates.
3. Document final DB_URL pattern and secrets handling.
4. Schedule staged rollout with rollback owner and window.

## 11. Canonical Path Rollout (Immediate Alias Disable)

### 11.1 Default behavior (current)
Current defaults are:
1. OBJECT_PATH_LAYOUT=canonical
2. ENABLE_LEGACY_PATH_ALIASES=0

That means objects are published only under:
`db/<server>/<database>/<schema>/<object>/...`

### 11.2 Recommended startup command

```powershell
.\Manager.ps1 -NoPull -TableFormat delta -Gateway -AdminUi -ObjectPathLayout canonical -DisableLegacyAliases
```

### 11.3 One-time cleanup for deterministic tree in Fabric
If prior runs used legacy paths, clear artifacts once before restart:

```powershell
Remove-Item -Recurse -Force .\.artifacts
```

Then restart Manager with the command above.

### 11.4 Expected Fabric browser shape
Under bucket root, table objects should appear as:
1. `db/<server>/<database>/<schema>/<object>/_delta_log/...`
2. `db/<server>/<database>/<schema>/<object>/data/split-*.parquet`

Legacy `db/<table>/...` folders should not be visible when aliases are disabled.
