# Windows Deployment Guide — Fabric Shortcut Proxy

A complete, copy‑paste installation baseline for running the Fabric Shortcut Proxy on
**Windows Server / Windows 10‑11**, from a bare host to a working Microsoft Fabric
shortcut. Written for **low‑to‑moderate IT skills**: every step has PowerShell you can
paste from an elevated (Administrator) prompt.

> **Golden rule (private baseline):** in the recommended private pattern, Microsoft Fabric
> never talks to the proxy directly — an **On‑Premises Data Gateway (OPDG)** sits in front of
> the Fabric Shortcut Proxy. On Windows the OPDG frequently runs on the **same host** as the
> proxy (or an adjacent Windows box on the same LAN), which is why Windows is a natural
> fit next to an on‑prem SQL Server. (A public‑internet endpoint needs no OPDG — Fabric
> connects directly to the TLS FQDN; see section 12.)

**Companion docs:** [../CONFIGURATION.md](../CONFIGURATION.md) (all settings) ·
[../SECURITY.md](../SECURITY.md) (auth/TLS/audit) ·
[../UsecasesAndScenarios.md](../UsecasesAndScenarios.md) (connectivity patterns) ·
[Linux_Deployment.md](Linux_Deployment.md) · [../../SSL_Deployment.md](../../SSL_Deployment.md)
(public‑internet TLS, Linux/nginx).

---

## 1. Architecture choices (OPDG is mandatory)

The proxy exposes an **S3‑compatible endpoint with AWS SigV4 auth**. Fabric reaches it
through an **On‑Premises Data Gateway** in every supported topology:

```mermaid
flowchart LR
  subgraph Fab[Microsoft Fabric / OneLake]
    SC[S3-compatible shortcut]
  end
  subgraph Win[Windows host / LAN]
    OPDG[On-Premises Data Gateway]
    PX[Fabric Shortcut Proxy<br/>S3 + SigV4 · agents :9000+]
    MG[Manager control plane :9200]
  end
  SRC[(Source: SQL Server / PostgreSQL / Oracle)]

  SC -->|S3 + SigV4| OPDG -->|http to :9000| PX -->|SQL pushdown| SRC
  MG -. supervises .- PX
```

| Pattern | When | Fabric → proxy path | Public exposure |
|---|---|---|---|
| **A. Private (recommended)** | OPDG + proxy on the same host or LAN | OPDG dials `http://127.0.0.1:9000` or a private IP | **None** |
| **B. Public internet** | No private path OPDG↔proxy | Terminate TLS in front; Fabric connects **directly** to `https://<fqdn>` — **no OPDG** | 443 only |

On Windows, **Pattern A** is the norm and this guide focuses on it. The public‑internet
variant is documented for **Linux + nginx** in [SSL_Deployment.md](../../SSL_Deployment.md);
if you must expose a Windows host publicly, see section 12.

> OPDG connectivity background: <https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>

---

## 2. Baseline recommendations

| Aspect | Recommendation |
|---|---|
| **OS** | Windows Server 2019/2022 (recommended) or Windows 10/11. |
| **Python** | 3.11 or newer, added to PATH (`python --version`). |
| **vCPU / RAM (small)** | 2 vCPU / 4 GB — 1 agent, small/medium tables. |
| **vCPU / RAM (moderate)** | 4 vCPU / 8–16 GB — 2+ agents / larger materializations. Each agent idles ~300 MB, alerts at 800 MB, auto‑restarts at 1200 MB by default. |
| **Disk** | OS + 10 GB, plus headroom for the optional Parquet disk cache / artifact store. |
| **Ports** | `9000`(+`9001`…) agent S3 data plane (reachable by the OPDG); `9200` control plane (admin only, keep local). |
| **Identity** | Run the service under a dedicated low‑privilege account (a gMSA or a local service account). |
| **Source DB** | A **read‑only** SQL login scoped to the exposed tables. |

---

## 3. Install prerequisites

From an **elevated PowerShell** (Run as Administrator). `winget` is the quickest path:

```powershell
winget install --id Git.Git -e --source winget
winget install --id Python.Python.3.12 -e --source winget
# Microsoft ODBC Driver 18 for SQL Server (needed for SQL Server sources):
winget install --id Microsoft.msodbcsql.18 -e --source winget
```

No `winget`? Install manually:
- Python 3.11+ — <https://www.python.org/downloads/windows/> (check *Add python.exe to PATH*).
- Git — <https://git-scm.com/download/win>.
- ODBC Driver 18 for SQL Server — <https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server>.

Verify, then allow local scripts for your session:

```powershell
python --version        # 3.11+
git --version
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

---

## 4. Get the code

```powershell
$Root = 'C:\FabricShortcutProxy'
git clone https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git $Root
Set-Location $Root
```

---

## 5. Bootstrap the virtual environment

`Manager.ps1` creates the `.venv`, installs dependencies, and (when you launch it) starts
the Manager + agent. Build it first without launching by using `-SkipInstall:$false` on a
throwaway run, or just let the first real launch do it. To pre‑build cleanly:

```powershell
# Creates .venv and installs core dependencies (no launch yet is not a Manager.ps1 mode;
# this launches — press Ctrl+C once you see "Started server process" to stop).
.\Manager.ps1 -Recreate -NoPull
# Ctrl+C to stop after it reports the agent/control URLs.
```

> `-Recreate` builds a clean venv; `-NoPull` skips git sync for this first offline build.

---

## 6. Source database drivers

The Manager bootstrap installs all supported Python database drivers plus the object-store
tokenizer readers (delta-rs + pyiceberg) through the `[drivers,objectstore]` extras. No
additional pip install is needed when using `Manager.ps1`.

```powershell
# Storage-proxy backends (only if serving files instead of a DB):
#   .\.venv\Scripts\python.exe -m pip install -e '.[s3proxy]'      # S3 / MinIO
#   .\.venv\Scripts\python.exe -m pip install -e '.[azureblob]'    # Azure Blob / ADLS
```

> On Windows the encrypted credential store uses **DPAPI** and needs **no extra package** —
> `[credentials]` (cryptography) is only for non‑Windows hosts.

---

## 7. Configuration files

Copy the templates (all **gitignored** — secrets never get committed) and edit them in
Notepad/VS Code.

```powershell
Copy-Item config.connection.example.json config.connection.json
Copy-Item config.tables.example.json      config.tables.json
Copy-Item config.system.example.json      config.system.json
Copy-Item config.freshness.example.json   config.freshness.json
```

### 7.1 Source connection — `config.connection.json`

SQL Server with a **read‑only** login (Windows co‑located example):

```json
{
  "connection": {
    "db_url": "mssql+aioodbc://fabric_ro:CHANGE_ME@localhost:1433/Salesdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
    "source_table": "dbo.orders",
    "key_column": "order_id",
    "table_name": "orders",
    "query_timeout_seconds": 300,
    "query_max_rows": 500000,
    "validate_source_schema": true
  }
}
```

For Windows Integrated auth instead of a SQL login, use
`mssql+aioodbc://@localhost/Salesdb?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes&TrustServerCertificate=yes`
and run the service under an account with DB read access.
PostgreSQL: `postgresql+asyncpg://fabric_ro:CHANGE_ME@pg-host:5432/salesdb` ·
Oracle: `oracle+oracledb://fabric_ro:CHANGE_ME@oracle-host:1521/ORCLPDB1`.

> **Production:** move the password out of this file into the DPAPI credential store
> (section 8) or a machine environment variable.

### 7.2 Tables — `config.tables.json`

```json
{
  "tables": [
    { "name": "orders",    "source_table": "dbo.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "dbo.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

### 7.3 System / server — `config.system.json`

For Pattern A with the OPDG on the same host, bind the agent to loopback and keep the
control plane local. Prefer **Delta** output for Fabric:

```json
{
  "system": {
    "bucket": "fabric-iceberg-poc",
    "host": "127.0.0.1",
    "port": 9000,
    "control_host": "127.0.0.1",
    "control_port": 9200,
    "require_sigv4": true,
    "table_format": "delta",
    "enable_config_builder": true,
    "enable_monitor": true,
    "agent_count": 1
  }
}
```

> If the OPDG is on a **different** LAN host, set `"host": "0.0.0.0"` and open `tcp/9000`
> to the OPDG's IP only (section 11).

### 7.4 Freshness — `config.freshness.json` (optional)

```json
{
  "freshness": {
    "auto_refresh": true,
    "refresh_poll_seconds": 600,
    "refresh_strategy": "auto",
    "refresh_allow_full_pull": true
  }
}
```

`refresh_allow_full_pull: true` keeps tables fresh (and silences the
`refresh_probe_unavailable` warning) when the cheap change probe is unavailable.

---

## 8. Secrets and authentication

Keep secrets out of the JSON files.

**DB password → DPAPI credential store (recommended on Windows).** After the service is
running (section 9), open the config UI at `http://127.0.0.1:9200/_config`, go to the
**Connection** tab, enter the real password, **Test connection**, then **Save credentials
to Manager**. It is encrypted with DPAPI and survives restarts — no plaintext in config.

**Other secrets → machine environment variables** (readable by the service account). Set
them from an elevated prompt (`/M` = machine scope):

```powershell
# S3 SigV4 keys Fabric will present (choose your own values):
setx /M S3_ACCESS_KEY_ID     "fabric-key-id"
setx /M S3_SECRET_ACCESS_KEY "REPLACE_WITH_A_STRONG_SECRET"

# Admin token for mutating /_manager actions:
$tok = -join ((48..57)+(97..102) | Get-Random -Count 48 | ForEach-Object {[char]$_})
setx /M ADMIN_TOKEN $tok

# Password-protect the operator console (Basic auth over the local/trusted network):
setx /M MANAGER_AUTH_ENABLED 1
setx /M MANAGER_AUTH_USERNAME operator
setx /M MANAGER_AUTH_PASSWORD "REPLACE_WITH_A_STRONG_PASSWORD"
```

> `setx /M` writes to the machine registry; restrict the host to trusted admins. Restart
> the service (or the shell) so new machine variables are picked up.

---

## 9. Run as a Windows service

The repo launches via `Manager.ps1`; wrap it in a service so it starts at boot and restarts
on crash. Two options — **NSSM** (recommended, gives auto‑restart) or the built‑in **Task
Scheduler**.

First create a small launch wrapper so the service always starts cleanly:

```powershell
@'
Set-Location C:\FabricShortcutProxy
# Deps are already installed; skip reinstall for a fast, reliable service start.
.\Manager.ps1 -SkipInstall -AdminUi -ConfigUi -AutoStash
'@ | Set-Content -Encoding UTF8 C:\FabricShortcutProxy\service-launch.ps1
```

### Option A — NSSM (recommended)

```powershell
winget install --id NSSM.NSSM -e --source winget   # or download from https://nssm.cc/download

$ps = (Get-Command powershell).Source
nssm install FabricShortcutProxy $ps "-NoProfile -ExecutionPolicy Bypass -File C:\FabricShortcutProxy\service-launch.ps1"
nssm set FabricShortcutProxy AppDirectory C:\FabricShortcutProxy
nssm set FabricShortcutProxy Start SERVICE_AUTO_START
nssm set FabricShortcutProxy AppStdout C:\FabricShortcutProxy\logs\service.out.log
nssm set FabricShortcutProxy AppStderr C:\FabricShortcutProxy\logs\service.err.log
New-Item -ItemType Directory -Force C:\FabricShortcutProxy\logs | Out-Null
nssm start FabricShortcutProxy
Get-Service FabricShortcutProxy
```

Stop/restart later with `nssm restart FabricShortcutProxy` or `Restart-Service
FabricShortcutProxy`.

### Option B — Task Scheduler (no extra download)

```powershell
$action  = New-ScheduledTaskAction -Execute (Get-Command powershell).Source `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File C:\FabricShortcutProxy\service-launch.ps1"
$trigger = New-ScheduledTaskTrigger -AtStartup
$set     = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit 0
Register-ScheduledTask -TaskName FabricShortcutProxy -Action $action -Trigger $trigger `
  -Settings $set -RunLevel Highest -User "NT AUTHORITY\SYSTEM"
Start-ScheduledTask -TaskName FabricShortcutProxy
```

---

## 10. Verify locally

```powershell
Invoke-RestMethod http://127.0.0.1:9000/healthz          # status = ok
Invoke-RestMethod http://127.0.0.1:9000/readyz           # ready when snapshots built + DB reachable
# Fleet (add -Headers @{Authorization=...} or -Credential if MANAGER_AUTH is on):
Invoke-RestMethod http://127.0.0.1:9200/_manager/api/fleet | ConvertTo-Json -Depth 4
# Service logs (NSSM):
Get-Content C:\FabricShortcutProxy\logs\service.out.log -Tail 40 -Wait
```

Healthy startup shows `snapshots_ready` (or `refresh_published`) then `agent_registered_ok`.

---

## 11. Open the port to the OPDG (only if the OPDG is on another host)

If the OPDG runs on the **same** Windows host, keep `host: 127.0.0.1` and skip this. If the
OPDG is on a different LAN host, bind the agent to `0.0.0.0` (section 7.3) and open the port
to that host only:

```powershell
New-NetFirewallRule -DisplayName "FSP agent (OPDG only)" -Direction Inbound -Protocol TCP `
  -LocalPort 9000 -RemoteAddress <opdg-host-ip> -Action Allow
```

Keep `9200` (control plane) closed to the network — administer it locally or over RDP/SSH.

---

## 12. Create the OPDG and the Fabric shortcut

1. **Install the gateway** on this Windows host (or an adjacent one): download
   *On‑premises data gateway* (standard mode), install, sign in with your Fabric account,
   and register it. Confirm **online** under Fabric → *Connections and gateways*.
2. **Create the shortcut** in a Fabric Lakehouse:
   - *New shortcut → Amazon S3 compatible*.
   - **URL**: `http://127.0.0.1:9000` (OPDG on this host) or `http://<proxy-ip>:9000`.
   - **Access key / Secret**: the `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` from section 8.
   - **Data gateway**: select your OPDG.
   - Browse to the `fabric-iceberg-poc` bucket and select the table folder(s).

Reference: <https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>.

**Public internet on Windows:** the recommended public path is **Linux + nginx**
([SSL_Deployment.md](../../SSL_Deployment.md)). If you must expose a Windows host, either
run a single standalone process with native TLS (`TLS_CERT_FILE` + `TLS_KEY_FILE` in
`config.system.json`, a CA‑trusted cert Fabric will accept) or front it with IIS + ARR as a
reverse proxy that passes the `Host` and `Authorization` headers unchanged (SigV4 depends on
them). In this public path there is **no OPDG** — Fabric connects directly to the HTTPS
endpoint (set *Data gateway* to *None* in the shortcut).

---

## 13. Day‑2 operations

**Deploy a code update** (the launcher pulls `origin/main` on start unless `-NoPull`):

```powershell
Restart-Service FabricShortcutProxy    # or: nssm restart FabricShortcutProxy
git -C C:\FabricShortcutProxy log --oneline -1
```

**Change settings live** — open `http://127.0.0.1:9200/_config`, edit the Advanced tab,
Save; most settings apply on the next restart.

**Scale agents** — set `agent_count` in `config.system.json` and restart, or use the
`/_manager` console scale action (needs the admin token).

---

## 14. Troubleshooting quick reference

| Symptom | Fix |
|---|---|
| `Manager.ps1 cannot be loaded ... execution policy` | `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned` (or the service uses `-ExecutionPolicy Bypass`). |
| `Data source name not found` / ODBC error | Install **ODBC Driver 18** (section 3); confirm the `driver=` in the URL matches. |
| Service starts then exits | Check `logs\service.err.log`. A permanent source‑DB error exits with code 78 — fix the connection/credentials in `/_config`. |
| Agents `registered:false` right after start | Normal cold‑start: agents materialize every table before registering. |
| Recurring `refresh_probe_unavailable` | Set `refresh_allow_full_pull: true` (section 7.4) and restart. |
| Timestamp/timezone read errors | `tzdata` ships as a dependency; ensure the venv install completed (re‑run `Manager.ps1 -Reinstall`). |

---

## 15. Baseline checklist

- [ ] Windows Server 2019/2022 (or Win10/11), Python 3.11+ on PATH, Git installed.
- [ ] ODBC Driver 18 installed (SQL Server sources).
- [ ] Source driver extra installed (`[postgres]`/`[oracle]`) as needed.
- [ ] Read‑only DB login; password in the **DPAPI credential store** or a machine env var, **not** in JSON.
- [ ] `require_sigv4: true`; SigV4 keys set; `ADMIN_TOKEN` set; `MANAGER_AUTH_*` on.
- [ ] Agent bound to `127.0.0.1` (same‑host OPDG) or firewalled to the OPDG IP; `9200` kept local.
- [ ] `auto_refresh` on with `refresh_allow_full_pull: true` for live data.
- [ ] Service registered (NSSM or Task Scheduler), auto‑start; `healthz`/`readyz` green; OPDG online; shortcut resolves.
