# Chapter 4: Installation

This chapter takes a bare host to a running proxy. It covers prerequisites, getting the
code, the virtual environment, source drivers, and the two editions. Host-specific,
copy-paste baselines live in [installation/Windows_Deployment.md](../installation/Windows_Deployment.md)
and [installation/Linux_Deployment.md](../installation/Linux_Deployment.md); this chapter
is the edition-agnostic path and the map to those guides.

## 4.1 Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.11 or newer | `python --version` / `python3 --version` |
| Git | any recent | to clone the repository |
| Source driver | per source | see §4.4; SQLite is bundled for the demo |
| ODBC Driver 18 for SQL Server | current | OS-level, only for SQL Server sources |

Sizing baselines (from the deployment guides): a small deployment is 2 vCPU / 4 GB for
one agent and small-to-medium tables; a moderate deployment is 4 vCPU / 8–16 GB for two or
more agents or larger materializations. Each agent idles near 300 MB, alerts at 800 MB, and
auto-restarts at 1200 MB by default. Budget disk for the OS plus roughly the size of your
largest materialized table set if you enable the Parquet disk cache or the artifact store.

Run the service under a dedicated low-privilege account, and give the proxy a **read-only**
database login scoped to the tables you expose.

## 4.2 Get the code

```powershell
# Windows
$Root = 'C:\FabricShortcutProxy'
git clone https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git $Root
Set-Location $Root
```

```bash
# Linux/macOS
git clone https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git fabric-shortcut-proxy
cd fabric-shortcut-proxy
```

## 4.3 Choose an edition

The repository builds two distributions. Install the one that matches your deployment.

| Edition | Package | Entry point | Install |
|---|---|---|---|
| **Lite** | `fabric-shortcut-proxy` | `python main.py` | `pip install -e .` |
| **Enterprise (cluster)** | `fabric-shortcut-proxy-enterprise` | `python -m enterprise.manager` | `pip install -e . -e ./enterprise` |

The enterprise wheel is pinned to the exact core version it was built against
(`fabric-shortcut-proxy==2.3.0`). A Lite-only install runs the standalone proxy unchanged;
the cluster hooks in `main.py` import the enterprise package lazily and print a clear hint
if it is not installed.

### The launchers

`Manager.ps1` (Windows) and `Manager.sh` (Linux/macOS) bootstrap the `.venv`, install
dependencies, and start the cluster edition. They are the recommended way to run the
enterprise edition; §4.6 lists the flags.

## 4.4 Install a source driver

SQLite is bundled for the demo. Install the driver for your real source:

| Source | Driver | Install |
|---|---|---|
| SQLite (demo) | `aiosqlite` | bundled |
| SQL Server | `aioodbc` + OS ODBC Driver 18 | driver bundled; install the ODBC driver from Microsoft |
| PostgreSQL | `asyncpg` | `pip install -e ".[postgres]"` |
| Oracle | `oracledb` | `pip install -e ".[oracle]"` |
| Databricks SQL | `databricks-sqlalchemy` | bundled; requires an HTTP path to a SQL warehouse |

Optional dependency extras (declared in `pyproject.toml`): `postgres`, `oracle`, `s3proxy`
(native S3/MinIO mounts), `azureblob` (Azure Blob/ADLS mounts), `credentials` (encrypted
store), and `dev` (test dependencies). Combine them, for example
`pip install -e ".[postgres,s3proxy]"`.

```powershell
# SQL Server also needs the OS ODBC driver:
winget install --id Microsoft.msodbcsql.18 -e --source winget
```

## 4.5 Install and run: Lite

```powershell
# Windows, from the repository root
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e .
# point at a source and run the single-node proxy
$env:DB_URL = "postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb"
$env:KEY_COLUMN = "order_id"
$env:DB_SOURCE_TABLE = "public.orders"
python main.py
```

```bash
# Linux/macOS
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
export DB_URL="postgresql+asyncpg://appuser:secret@pg-host:5432/salesdb"
export KEY_COLUMN="order_id"
export DB_SOURCE_TABLE="public.orders"
python main.py
```

The Lite proxy listens on port 9000 by default. With no `DB_URL`, it seeds and serves a
local SQLite demo so you can validate the S3 path before wiring a real source.

## 4.6 Install and run: enterprise cluster

The launchers create the virtual environment, install both packages, and start the Manager
plus one or more agents.

```powershell
# Windows
.\Manager.ps1
# open the control plane at http://localhost:9200/_manager (admin only)
```

```bash
# Linux/macOS
bash ./Manager.sh
```

Common launcher flags (Windows names shown; the Bash launcher accepts the equivalent
`--kebab-case`):

| Flag | Purpose |
|---|---|
| `-SkipInstall` | Skip dependency install; start faster on an already-provisioned host |
| `-Recreate` | Recreate the `.venv` from scratch |
| `-NoPull` | Do not `git pull` before starting |
| `-AgentCount <n>` | Number of supervised agent processes |
| `-Gateway` | Enable the built-in round-robin S3 gateway in front of the agents |
| `-AdminUi` | Serve the admin console at `/_manager` |
| `-ConfigUi` | Serve the config builder at `/_config` |
| `-Ha` | Enable Manager leader-lease high availability |
| `-RetentionGc` | Enable the retention garbage collector |
| `-DbUrl <url>` | Set the source connection string for this run |
| `-TableFormat <iceberg\|delta>` | Choose the output format |
| `-ControlPort <n>` / `-AgentPort <n>` | Override control and data ports |

The launcher also has flags for building and running the optional C++ serving agent
(`-BuildCppAgent`, `-RunCppAgent`, and related `-Cpp*` options). Those are advanced; the
Python agent is the default path.

## 4.7 Verify the install

```powershell
# Liveness and readiness (readiness also checks the source DB is reachable)
curl http://localhost:9000/healthz
curl http://localhost:9000/readyz
```

A `200` from `/readyz` means the snapshot is built and the source is reachable. If it
returns `503`, the source connection or configuration is not ready; chapter 8 covers
troubleshooting. To validate the served table objects with a reference Iceberg reader, run
`python validate_pyiceberg.py`.

## 4.8 Host-specific baselines

For OS-level details (service accounts, firewalls, OPDG placement, systemd units, TLS), use
the deployment guides:

- [installation/Windows_Deployment.md](../installation/Windows_Deployment.md) — Windows
  Server / 10 / 11, OPDG on the same host or LAN.
- [installation/Linux_Deployment.md](../installation/Linux_Deployment.md) — Ubuntu / Debian
  / RHEL, private and public patterns.
- [SSL_Deployment.md](../../SSL_Deployment.md) — public-internet TLS with Linux + nginx.
- [LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) — launcher fixes.

## 4.9 Next

Continue to [Chapter 5: Configuration](05-configuration.md) to point the proxy at your
source and register tables.
