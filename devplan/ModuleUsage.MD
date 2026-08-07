# Module and Dependency Usage Map

This document maps the Python distributions declared in the root
`pyproject.toml` and `enterprise/pyproject.toml` to their runtime use. It also
records direct imports supplied transitively and packaging issues that remain.

Reviewed: **2026-08-07**.

## Package boundary

The repository produces two Python distributions:

| Distribution | Install command | Contents |
| --- | --- | --- |
| `fabric-shortcut-proxy` | `pip install -e .` | Lite Agent, S3 data plane, table generation, storage proxy, configuration, monitoring, and shared runtime modules |
| `fabric-shortcut-proxy-enterprise` | `pip install -e . -e ./enterprise` | Manager, Agent control link, registry, supervisor, gateway, HA, retention, and admin control plane |

The Enterprise distribution pins `fabric-shortcut-proxy==2.1.1`. It imports
the shared Agent and runtime modules from that core distribution rather than
packaging a second copy.

## Lite core runtime dependencies

These packages are in root `[project].dependencies`, so a Lite installation
always installs them.

| Package | Purpose | Primary use |
| --- | --- | --- |
| `fastapi` | Agent API, S3 routes, operations endpoints, config builder, and monitor | `main.py`, `s3/router.py`, `storage/passthrough.py`, `configbuilder/router.py`, `monitor/router.py`, `observability/endpoints.py` |
| `uvicorn[standard]` | ASGI server for the Agent; also used by the Enterprise Manager entrypoint | `main.py`, `enterprise/manager.py` |
| `sqlalchemy[asyncio]` | Async and synchronous source engines, reflection, SQL execution, and URL parsing | `db/executor.py`, `db/reflect.py`, `demo/seed_db.py`, `iceberg/state_store.py` |
| `pyarrow` | Parquet encoding, metadata inspection, Iceberg schema conversion, and split statistics | `parquet/generator.py`, `runtime/materializer.py`, `iceberg/schema.py`, `iceberg/stats.py` |
| `fastavro` | Iceberg manifest and manifest-list Avro encoding | `iceberg/manifest.py` |
| `structlog` | Structured application logging behind `get_logger` | `observability/logging.py` |
| `tzdata` | IANA timezone database fallback, especially on Windows | Indirectly consumed by Python `zoneinfo` while handling timezone-aware values in `iceberg/stats.py` |
| `aiosqlite` | SQLite async SQLAlchemy driver for the default demo database and tests | Loaded through `sqlite+aiosqlite://` URLs |
| `aioodbc` | SQL Server async SQLAlchemy driver | Loaded through `mssql+aioodbc://` URLs |
| `databricks-sqlalchemy` | Databricks SQLAlchemy dialect and connector integration | Loaded through `databricks://` URLs |
| `pytest` | Test runner | Declared as a hard dependency, but used only by `tests/`; see current findings |
| `pytest-asyncio` | Async pytest integration | Declared as a hard dependency, but used only by `tests/`; see current findings |

## Enterprise runtime dependencies

These packages are declared by `enterprise/pyproject.toml`. Installing Lite
alone does not install them.

| Package | Purpose | Primary use |
| --- | --- | --- |
| `fabric-shortcut-proxy==2.1.1` | Exact-version core required by the Enterprise control plane | Imports `config`, `runtime`, `iceberg`, `observability`, and the Agent entrypoint from the Lite distribution |
| `httpx` | Gateway reverse proxy, Agent-to-Manager REST client, and fleet monitor scraping | `enterprise/control/gateway.py`, `enterprise/control/transport.py`, `enterprise/control/monitor_proxy.py` |
| `psutil` | Supervisor RSS sampling, thresholds, and memory-triggered restarts | `enterprise/control/supervisor.py` |

`psutil` is a hard Enterprise package requirement but a defensive optional
import in code. If the import fails, supervision still works while memory
sampling and memory-triggered restart remain disabled.

The Agent heartbeat implementation is `enterprise/agent_link.py`. It uses the
REST client in `enterprise/control/transport.py`, which is where `httpx` is
imported.

## Source database drivers

SQLAlchemy loads these packages from the configured URL. Application modules do
not import the drivers directly.

| Package | Dialect URL | Declaration | Install behavior |
| --- | --- | --- | --- |
| `aiosqlite` | `sqlite+aiosqlite://` | Core dependency | Installed with Lite |
| `aioodbc` | `mssql+aioodbc://` | Core dependency | Installed with Lite |
| `databricks-sqlalchemy` | `databricks://` | Core dependency | Installed with Lite |
| `asyncpg` | `postgresql+asyncpg://` | Root `postgres` extra | Install with `.[postgres]` |
| `oracledb` | `oracle+oracledb://` | Root `oracle` extra | Install with `.[oracle]` |

The shorthand-to-driver map is `_DIALECT_TO_DRIVER` in `db/reflect.py`.
Async-versus-sync engine selection is implemented in `db/executor.py`.

## Optional runtime extras

These integrations are loaded only when their feature is configured.

| Extra | Packages | Purpose and use |
| --- | --- | --- |
| `credentials` | `cryptography` | Fernet credential encryption on non-Windows hosts and when explicitly selected. Imported lazily by `security/credential_store.py`; Windows can use DPAPI through `ctypes`. |
| `s3proxy` | `boto3` | S3, MinIO, and S3-compatible mounted storage. `storage/s3_auth.py` lazily imports `boto3` and `botocore`; `storage/s3_store.py` serves the resulting client. |
| `azureblob` | `azure-storage-blob`, `azure-identity` | Azure Blob and ADLS Gen2 mounts. `storage/azure_auth.py` builds Blob clients and AAD credentials; `storage/azure_store.py` serves objects. |
| `postgres` | `asyncpg` | PostgreSQL source driver. |
| `oracle` | `oracledb` | Oracle source driver. |

## Development and validation dependencies

Root `[project.optional-dependencies].dev` contains:

| Package | Purpose | Primary use |
| --- | --- | --- |
| `pyiceberg[pyarrow]` | Validate generated metadata and data with an independent Iceberg reader | `validate_pyiceberg.py`, CI `fabric-compat` job |
| `botocore` | Produce real SigV4-signed requests in authentication tests | `tests/test_auth.py`, `tests/test_access_control.py` |
| `httpx` | ASGI test client for Lite API tests | Tests that call FastAPI applications in-process |

`pytest` and `pytest-asyncio` are not in the `dev` extra at present. They are
listed in core `[project].dependencies`, even though their imports are confined
to tests.

## Direct imports supplied transitively

| Package | Why it is available | Direct use |
| --- | --- | --- |
| `starlette` | Required by `fastapi` | `enterprise/control/auth.py` imports middleware, request, and response classes; `enterprise/control/gateway.py` imports `BackgroundTask` |
| `botocore` | Required by the optional `boto3` package | `storage/s3_auth.py` uses credential providers and request configuration; `storage/s3_store.py` recognizes `ClientError` without importing it at module load |

Directly importing a transitive dependency couples the code to another
package's dependency contract. `starlette` is tightly coupled to FastAPI and is
currently version-selected by FastAPI. `botocore` is part of the `boto3`
installation used by the `s3proxy` extra.

## Current findings

### 1. Test tools are installed in production Lite environments

**Status: open.** `pytest` and `pytest-asyncio` are root hard dependencies, but
the application code does not import them. Move both to the `dev` extra to keep
the Lite runtime installation smaller:

```toml
[project.optional-dependencies]
dev = [
   "pytest>=8.0",
   "pytest-asyncio>=0.23",
   "pyiceberg[pyarrow]>=0.6",
   "botocore>=1.34",
   "httpx>=0.27",
]
```

### 2. Enterprise dependency ownership is now separated correctly

**Status: fixed in the manifests; corrected in this map.** `httpx` and `psutil`
belong to `fabric-shortcut-proxy-enterprise`. Lite needs `httpx` only for tests,
where it is supplied by the root `dev` extra.

### 3. PostgreSQL and Oracle drivers are discoverable extras

**Status: fixed.** The root manifest declares `postgres` and `oracle` extras,
matching `postgresql+asyncpg` and `oracle+oracledb` in `db/reflect.py`.

### 4. Credential and cloud storage dependencies are feature-scoped

**Status: fixed.** `cryptography`, `boto3`, `azure-storage-blob`, and
`azure-identity` are exposed through named extras. Local/NFS/SMB mounts do not
install either cloud SDK.

### 5. Enterprise paths in the previous map were stale

**Status: fixed in this map.** Control-plane modules live under
`enterprise/control/`, and the heartbeat link is `enterprise/agent_link.py`, not
`control/` or `runtime/agent_link.py`.
