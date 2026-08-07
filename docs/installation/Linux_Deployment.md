# Linux Deployment Guide — Fabric Shortcut Proxy

A complete, copy‑paste installation baseline for running the Fabric Shortcut Proxy on
**Linux**, from a bare VM to a working Microsoft Fabric shortcut. Written for **low‑to‑
moderate IT skills**: every step has commands you can paste.

> **Golden rule:** for the **private** pattern (the recommended baseline), Microsoft
> Fabric never talks to the proxy directly — an **On‑Premises Data Gateway (OPDG)** sits
> in front as the controlled bridge from OneLake to your proxy. For the **public‑internet**
> variant there is **no OPDG**: Fabric connects **directly** to the proxy's TLS‑protected
> public FQDN (HTTPS + SigV4 + auth).

**Companion docs:** [../CONFIGURATION.md](../CONFIGURATION.md) (all settings) ·
[../SECURITY.md](../SECURITY.md) (auth/TLS/audit) ·
[../LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) (fixes) ·
[../UsecasesAndScenarios.md](../UsecasesAndScenarios.md) (connectivity patterns) ·
[../../SSL_Deployment.md](../../SSL_Deployment.md) (public‑internet TLS) ·
[Windows_Deployment.md](Windows_Deployment.md).

---

## 1. Architecture choices

The proxy exposes an **S3‑compatible endpoint with AWS SigV4 auth**. How Fabric reaches it
depends on the pattern: through an **On‑Premises Data Gateway (OPDG)** for the private
baseline, or **directly** over HTTPS for the public‑internet variant.

```mermaid
flowchart LR
  subgraph Fab[Microsoft Fabric / OneLake]
    SC[S3-compatible shortcut]
  end
  OPDG[On-Premises Data Gateway<br/>Windows host · private pattern only]
  subgraph Host[Linux host / VNet]
    PX[Fabric Shortcut Proxy<br/>S3 + SigV4 · agents :9000+]
    MG[Manager control plane :9200<br/>console + config API]
  end
  SRC[(Source: SQL Server / PostgreSQL /<br/>Oracle / NAS / object store)]

  SC -->|A private: S3 + SigV4| OPDG -->|http to :9000| PX
  SC -->|B public: HTTPS + SigV4 direct to FQDN| PX
  PX -->|SQL pushdown / byte passthrough| SRC
  MG -. supervises .- PX
```

Pick the connectivity pattern that matches your network:

| Pattern | When | Fabric → proxy path | Public exposure |
|---|---|---|---|
| **A. Private (recommended)** | Fabric must reach a proxy that has no public exposure | **OPDG** dials the proxy's **private IP** `http://10.x.x.x:9000` | **None** — proxy has no public listener |
| **B. Public internet (Linux)** | You want Fabric to reach the proxy directly, no gateway | Fabric connects **directly** to a **public `https://` FQDN** terminated by nginx — **no OPDG** | 443 only, TLS + auth (see [SSL_Deployment.md](../../SSL_Deployment.md)) |

Both keep the source database credentials **inside the proxy** (credential mediation):
Fabric only ever sees SigV4 keys, never your DB password. Pattern A is the default
recommendation and uses the OPDG; Pattern B (section 11) exposes a public TLS endpoint that
Fabric connects to directly.

> OPDG background (Pattern A):
> <https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>

---

## 2. Baseline recommendations

| Aspect | Recommendation |
|---|---|
| **OS** | Ubuntu 22.04 LTS / 24.04 LTS (or Debian 12). RHEL/Alma work with `dnf` equivalents. |
| **Python** | 3.11 or newer (`python3 --version`). |
| **vCPU / RAM (small)** | 2 vCPU / 4 GB — 1 agent, a few small/medium tables. |
| **vCPU / RAM (moderate)** | 4 vCPU / 8–16 GB — 2+ agents, larger materializations, disk cache. Each agent process idles ~300 MB and alerts at 800 MB / auto‑restarts at 1200 MB by default. |
| **Disk** | OS + 10 GB, plus headroom for the optional Parquet disk cache / artifact store if enabled (size ≈ your largest materialized table set). |
| **Ports (Pattern A)** | `9000`(+`9001`…) agent S3 data plane (reachable by the OPDG host only); `9200` control plane (admin only, keep private). |
| **Ports (Pattern B)** | `443` (Fabric via nginx), `80` (redirect), `9443` (admin console) — app ports bound to loopback. |
| **Identity** | Run under a dedicated non‑login service user (`fsp`). |
| **Source DB** | A **read‑only** login scoped to the tables you expose. |

---

## 3. Install prerequisites

```bash
sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip
```

### SQL Server sources only — Microsoft ODBC Driver 18

The proxy talks to SQL Server over ODBC (`aioodbc`/`pyodbc`). Install the driver + unixODBC:

```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
curl -sSL https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev
odbcinst -q -d      # should list: [ODBC Driver 18 for SQL Server]
```

> PostgreSQL and Oracle need no OS packages — their drivers are Python extras (section 6).

---

## 4. Get the code

```bash
sudo mkdir -p /opt/fabric-shortcut-proxy
sudo chown "$USER" /opt/fabric-shortcut-proxy
git clone https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git /opt/fabric-shortcut-proxy
cd /opt/fabric-shortcut-proxy
```

---

## 5. Create the service user and virtual environment

`Manager.sh` bootstraps the `.venv` and installs dependencies. Build it **once** as the
service user so the venv's absolute paths belong to `fsp`.

```bash
# dedicated, non-login service account
sudo useradd --system --create-home --shell /usr/sbin/nologin fsp
sudo chown -R fsp:fsp /opt/fabric-shortcut-proxy
sudo -u fsp git config --global --add safe.directory /opt/fabric-shortcut-proxy
```

The `.venv` and dependencies are built **automatically on the first `systemctl start`**
(section 9) — the unit allows up to 10 minutes for that first install. To pre‑build now and
watch it, run the launcher interactively and stop it with `Ctrl+C` once it prints the agent
/ control URLs:

```bash
sudo -u fsp bash /opt/fabric-shortcut-proxy/Manager.sh --recreate --no-pull
# ... wait for "Started server process", then press Ctrl+C.
```

`--recreate` builds a clean venv; `--no-pull` skips git sync for this offline build. If your
host has no `python3-venv` ensurepip, install it and retry:
`sudo apt-get install -y python3-venv python3-pip` (see
[../LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) §2).

---

## 6. Install the source database driver

Core install covers SQLite (demo) and SQL Server (ODBC). Add the extra for your source:

```bash
cd /opt/fabric-shortcut-proxy
# PostgreSQL source:
sudo -u fsp ./.venv/bin/pip install -e '.[postgres]'
# Oracle source:
sudo -u fsp ./.venv/bin/pip install -e '.[oracle]'
# Amazon Redshift source (preview):
sudo -u fsp ./.venv/bin/pip install -e '.[redshift]'
# Teradata source (preview):
sudo -u fsp ./.venv/bin/pip install -e '.[teradata]'
# Apache Impala source (preview):
sudo -u fsp ./.venv/bin/pip install -e '.[impala]'
# Encrypted credential store on Linux (recommended — see section 8):
sudo -u fsp ./.venv/bin/pip install -e '.[credentials]'
# Storage-proxy backends (only if you serve NAS/S3/Azure files instead of a DB):
#   sudo -u fsp ./.venv/bin/pip install -e '.[s3proxy]'     # S3 / MinIO
#   sudo -u fsp ./.venv/bin/pip install -e '.[azureblob]'   # Azure Blob / ADLS
```

> On Linux the credential store needs `cryptography` (the `[credentials]` extra). Windows
> uses DPAPI and needs no extra.

---

## 7. Configuration files

Configuration is split into small JSON files (all **gitignored** — your secrets never get
committed). Copy the templates and edit.

```bash
cd /opt/fabric-shortcut-proxy
sudo -u fsp cp config.connection.example.json config.connection.json
sudo -u fsp cp config.tables.example.json      config.tables.json
sudo -u fsp cp config.system.example.json      config.system.json
sudo -u fsp cp config.freshness.example.json   config.freshness.json
```

### 7.1 Source connection — `config.connection.json`

Point `db_url` at your source with a **read‑only** login. SQL Server example:

```json
{
  "connection": {
    "db_url": "mssql+aioodbc://fabric_ro:CHANGE_ME@sqlhost:1433/Salesdb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
    "source_table": "dbo.orders",
    "key_column": "order_id",
    "table_name": "orders",
    "query_timeout_seconds": 300,
    "query_max_rows": 500000,
    "validate_source_schema": true
  }
}
```

PostgreSQL: `postgresql+asyncpg://fabric_ro:CHANGE_ME@pg-host:5432/salesdb` ·
Oracle: `oracle+oracledb://fabric_ro:CHANGE_ME@oracle-host:1521/ORCLPDB1`.
You can add more sources under a `connections` array (see the example file's comments).

> **Do not leave the password in this file for production.** Section 8 moves it into the
> encrypted credential store or an environment variable.

### 7.2 Tables — `config.tables.json`

List the tables/views to expose. `key_column` must be an integer key used to split reads.

```json
{
  "tables": [
    { "name": "orders",    "source_table": "dbo.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "dbo.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

### 7.3 System / server — `config.system.json`

For **Pattern A** (private, OPDG on the LAN), bind the agent to the private interface (or
`0.0.0.0` behind a host firewall), keep the control plane private, and prefer **Delta**
output (Fabric reads `_delta_log` directly):

```json
{
  "system": {
    "bucket": "fabric-iceberg-poc",
    "host": "0.0.0.0",
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

### 7.4 Freshness — `config.freshness.json` (optional but recommended)

Re‑read the source on a timer and publish a new snapshot only when content changes:

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

`refresh_allow_full_pull: true` lets `auto` fall back to a full re‑read when the cheap
per‑dialect change probe is unavailable (e.g. SQL Server's DMV is empty after a restart),
so tables stay fresh without the recurring `refresh_probe_unavailable` warning.

---

## 8. Secrets and authentication

Keep secrets out of the JSON files. Use an **environment file** owned by `fsp`, `0600`.

```bash
sudo install -o fsp -g fsp -m 600 /dev/null /etc/fabric-shortcut-proxy.env

# (a) DB password via env var instead of config.connection.json (DB_URL overrides the file):
read -rs DBURL; printf 'DB_URL=%s\n' "$DBURL" | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null; unset DBURL

# (b) OR use the encrypted credential store key (Linux Fernet backend). Generate once:
python3 - <<'PY' | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
import base64, os; print("FSP_CRED_KEY=" + base64.urlsafe_b64encode(os.urandom(32)).decode())
PY

# S3 SigV4 keys Fabric will present (choose your own values):
printf 'S3_ACCESS_KEY_ID=%s\n'     'fabric-key-id'          | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
read -rs SK; printf 'S3_SECRET_ACCESS_KEY=%s\n' "$SK"       | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null; unset SK

# Admin token for mutating /_manager actions (start/stop/restart/drain):
printf 'ADMIN_TOKEN=%s\n' "$(openssl rand -hex 24)" | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
```

### Password‑protect the operator console (recommended)

The Manager console/config API (`9200`) supports a built‑in HTTP Basic gate. Turn it on so
the admin surface needs a password:

```bash
sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null <<'EOF'
MANAGER_AUTH_ENABLED=1
MANAGER_AUTH_USERNAME=operator
EOF
read -rs PW; printf 'MANAGER_AUTH_PASSWORD=%s\n' "$PW" | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null; unset PW
```

> Basic auth sends credentials on every request — only expose `9200` on a trusted network
> or behind TLS. In Pattern B it runs behind nginx TLS (section 11).

---

## 9. Run as a systemd service

Create `/etc/systemd/system/fabric-shortcut-proxy.service`:

```bash
sudo tee /etc/systemd/system/fabric-shortcut-proxy.service >/dev/null <<'EOF'
[Unit]
Description=Fabric Shortcut Proxy Manager
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=fsp
Group=fsp
WorkingDirectory=/opt/fabric-shortcut-proxy
EnvironmentFile=/etc/fabric-shortcut-proxy.env
ExecStart=/bin/bash /opt/fabric-shortcut-proxy/Manager.sh --admin-ui --config-ui --auto-stash
Restart=on-failure
RestartSec=5
TimeoutStartSec=600
KillMode=control-group

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now fabric-shortcut-proxy.service
systemctl status fabric-shortcut-proxy.service
```

`Manager.sh` on start: fast‑forwards the repo from `origin/main`, installs deps if changed,
then launches the Manager (control plane on `9200`) which supervises the agent(s) on `9000`
(+`9001`…). It restarts a crashed agent automatically.

> The unit reads `ADMIN_TOKEN`, `MANAGER_AUTH_*`, `DB_URL`, etc. from the env file, keeping
> secrets out of `systemctl cat`. To bind the console to the private NIC for remote admin,
> add `--control-host 0.0.0.0` **and** keep `MANAGER_AUTH_*` on plus a firewall.

---

## 10. Verify locally

```bash
# Health (open endpoints)
curl -s http://127.0.0.1:9000/healthz        # {"status":"ok"}
curl -s http://127.0.0.1:9000/readyz         # ready once snapshots built + DB reachable

# Fleet + effective config (console; add -u if MANAGER_AUTH is on)
curl -s http://127.0.0.1:9200/_manager/api/fleet | python3 -m json.tool | grep -E 'ready|serving_tables'

# Follow logs
sudo journalctl -u fabric-shortcut-proxy.service -f
```

Healthy startup logs show `snapshots_ready` (or `refresh_published`) then
`agent_registered_ok`. On a SQL Server source, `source_unavailable:0` in the monitor
summary means the DB is reachable.

---

## 11. Public‑internet variant (Pattern B) — TLS via nginx

Use this when you want Fabric to reach the proxy **directly over the internet** rather than
through a gateway. There is **no OPDG** in this pattern. The full, tested procedure is in
**[../../SSL_Deployment.md](../../SSL_Deployment.md)**. In short:

1. Bind the app to loopback (`HOST=127.0.0.1`, `CONTROL_HOST=127.0.0.1`) so `9000/9200`
   are never public.
2. Install nginx and a **CA‑trusted** certificate (Let's Encrypt via the VM's DNS label,
   or an enterprise/public CA) — Fabric rejects self‑signed on the data plane.
3. Front the S3 data plane on `443` (SigV4 preserved by passing `Host` + `Authorization`
   unchanged) and the console on `9443`, and turn on `MANAGER_AUTH_*`.
4. Create the Fabric shortcut pointing **directly** at `https://<your-fqdn>` with the
   **Data gateway** left as *None* (section 12).

Follow [SSL_Deployment.md](../../SSL_Deployment.md) end‑to‑end for the cert commands,
nginx vhosts, firewall rules, and zero‑downtime renewal.

---

## 12. Create the Fabric shortcut (with the OPDG for Pattern A)

**Pattern A** uses an OPDG on a **Windows** host that can reach the proxy on the same
LAN/VNet. **Pattern B** uses **no gateway** — skip steps 1–2 and set *Data gateway* to *None*.

1. **(Pattern A) Install the gateway** on a Windows host and sign in with your Fabric/Power
   BI account: download "On‑premises data gateway" (standard mode), install, and register it
   to your tenant. Confirm it shows **online** in Fabric admin → *Connections and gateways*.
2. **(Pattern A) Open the proxy port to the gateway host**: allow the OPDG host's IP to
   reach `tcp/9000` on the Linux host; keep `9200` closed to everyone but admins.
3. **Create the shortcut** in a Fabric Lakehouse:
   - *New shortcut → Amazon S3 compatible*.
   - **URL**: `http://<proxy-private-ip>:9000` (Pattern A) or `https://<your-fqdn>` (Pattern B).
   - **Access key / Secret**: the `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` from section 8.
   - **Data gateway**: select your **OPDG** (Pattern A) or leave **None** (Pattern B).
   - Browse to the bucket (`fabric-iceberg-poc`) and select the table folder(s).

Only the queried rows are read. Reference:
<https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>.

---

## 13. Day‑2 operations

**Deploy a code update** (the service pulls `origin/main` on restart):

```bash
sudo systemctl restart fabric-shortcut-proxy.service
git -C /opt/fabric-shortcut-proxy log --oneline -1   # confirm the commit advanced
```

**Change settings live** — open `http://127.0.0.1:9200/_config` (SSH‑tunnel if remote),
edit in the Advanced tab, Save; most settings apply on the next restart.

**Scale agents** — set `agent_count` (config.system.json) and restart, or use the console's
scale action.

**Rolling restart / drain** — from the `/_manager` console (needs the admin token).

---

## 14. Troubleshooting quick reference

| Symptom | Fix |
|---|---|
| `libodbc.so.2: cannot open shared object file` | Install ODBC driver (section 3). |
| `No module named pip` during bootstrap | `sudo apt-get install -y python3-venv python3-pip`, then `Manager.sh --recreate`. |
| Agents `registered:false` right after start | Normal cold‑start: agents materialize every table before registering. Watch for `agent_registered_ok`. |
| Recurring `refresh_probe_unavailable table=…` | Set `refresh_allow_full_pull: true` (section 7.4) and restart. |
| SQL Server crash‑loop `Incorrect syntax near 'LIMIT'` | Set `SPLIT_STRATEGY=modulo` in the env file and restart (stopgap). |
| Deployed commit didn't advance | Unit uses `--no-pull`, or local edits block fast‑forward — see [../LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md) §10. |

Full runbook: [../LINUX_MANAGER_TROUBLESHOOTING.md](../LINUX_MANAGER_TROUBLESHOOTING.md).

---

## 15. Baseline checklist

- [ ] Ubuntu 22.04+/Debian 12, Python 3.11+, dedicated `fsp` user.
- [ ] ODBC Driver 18 installed (SQL Server sources).
- [ ] Source driver extra installed (`[postgres]`/`[oracle]`) + `[credentials]` on Linux.
- [ ] Read‑only DB login; password in the env file or credential store, **not** in JSON.
- [ ] `require_sigv4: true`; SigV4 keys set; `ADMIN_TOKEN` set; `MANAGER_AUTH_*` on.
- [ ] Control plane `9200` private (loopback or firewalled + auth).
- [ ] Pattern A: `9000` reachable **only** by the OPDG host. Pattern B: app on loopback, nginx TLS ([SSL_Deployment.md](../../SSL_Deployment.md)).
- [ ] `auto_refresh` on with `refresh_allow_full_pull: true` if you want live data.
- [ ] systemd unit enabled; `healthz`/`readyz` green; OPDG online; shortcut resolves.
