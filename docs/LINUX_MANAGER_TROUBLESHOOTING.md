# Linux Manager Troubleshooting Guide

Operational fixes for running the Fabric Shortcut Proxy **Manager** on Linux
(`Manager.sh` → `enterprise.manager`), covering bootstrap, credentials, the SQL
Server ODBC path, systemd, and deploying code fixes through the auto-update flow.

Examples below assume a systemd deployment with:

- Repo at `/opt/fabric-shortcut-proxy`, owned by service user `fsp`
- Env file `/etc/fabric-shortcut-proxy.env`
- Unit `fabric-shortcut-proxy.service`

Adjust paths/user to your install.

---

## Fast diagnostics

```bash
# Follow logs live
sudo journalctl -u fabric-shortcut-proxy.service -f

# Full, untruncated lines (journal's pager truncates with '>')
sudo journalctl -u fabric-shortcut-proxy.service --no-pager -o cat | tail -n 100

# Which Python/venv the Manager (and agents) actually run from
pgrep -af 'enterprise.manager|main.py'

# What commit the deployed repo is on
git -C /opt/fabric-shortcut-proxy log --oneline -1

# Service state
systemctl status fabric-shortcut-proxy.service
```

Two recurring root causes tie most of this together:
1. **Wrong venv** — you installed a package into a dev venv, but the service runs
   from a different venv/user. Always confirm with `pgrep -af enterprise.manager`.
2. **Stale code** — the box only runs what's on `origin/main`; a local commit
   that isn't pushed (and a service that isn't restarted) changes nothing.

---

## 1. `./Manager.sh: Permission denied`

**Cause:** the script isn't marked executable (common after copy/clone).

```bash
chmod +x Manager.sh
./Manager.sh --auto-stash
# or run it without the exec bit:
bash Manager.sh --auto-stash
```

If you see `bad interpreter: /bin/bash^M`, the file has Windows CRLF endings:

```bash
sed -i 's/\r$//' Manager.sh
```

---

## 2. `No module named pip` (venv has no pip)

**Cause:** on Debian/Ubuntu, `python3 -m venv` produces a pip-less venv when
`python3-venv`'s ensurepip bootstrap is missing. `Manager.sh` then fails at
`-m pip install --upgrade pip`.

```bash
sudo apt update
sudo apt install -y python3-venv python3-pip
rm -rf .venv
bash Manager.sh --recreate --auto-stash
```

No sudo/apt available — bootstrap pip into the existing venv:

```bash
.venv/bin/python -m ensurepip --upgrade
.venv/bin/python -m pip install --upgrade pip
```

The apt route is more reliable; minimal images strip `ensurepip` too.

---

## 3. `libodbc.so.2: cannot open shared object file` (SQL Server source)

**Cause:** SQL Server connections use ODBC (`aioodbc`/`pyodbc`), which needs the
unixODBC driver manager plus Microsoft's ODBC driver. The proxy targets
**ODBC Driver 18 for SQL Server** (falls back to 17).

```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
  | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
curl -sSL https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list >/dev/null

sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

odbcinst -q -d      # should list: [ODBC Driver 18 for SQL Server]
```

`msodbcsql18` pulls in `unixodbc`, which provides `libodbc.so.2`. Restart the
Manager and retry. If only driver 17 registers, set the connection's Driver
field (or `?driver=ODBC+Driver+17+for+SQL+Server`).

---

## 4. `no encryption backend available` (credential store)

Banner: *"The Manager can't encrypt credentials on this host."*

**Cause:** `cryptography` is an **optional** extra (`credentials` in
`pyproject.toml`), not a core dependency. `Manager.sh` runs `pip install -e .`,
so it is never installed automatically, and a `--recreate`d or relocated venv
won't have it. `FSP_CRED_KEY` does **not** bypass this — that path also imports
`Fernet` from `cryptography`.

Install it into the venv the service actually uses:

```bash
pgrep -af enterprise.manager     # confirm the venv path first
sudo -u fsp /opt/fabric-shortcut-proxy/.venv/bin/python -m pip install cryptography
sudo -u fsp /opt/fabric-shortcut-proxy/.venv/bin/python -c "import cryptography; print('OK', cryptography.__version__)"
sudo systemctl restart fabric-shortcut-proxy.service
```

Make it survive a future `--recreate` by installing the extra explicitly:

```bash
sudo -u fsp /opt/fabric-shortcut-proxy/.venv/bin/python -m pip install -e '/opt/fabric-shortcut-proxy[credentials]'
```

**Alternative (no store):** skip encryption entirely and put the password in the
`DB_URL` (or `DB_URL_<ID>`) env var in `/etc/fabric-shortcut-proxy.env`.

---

## 5. Setting `FSP_CRED_KEY`

`FSP_CRED_KEY` is a urlsafe-base64 **Fernet** key. It lets you pin a fixed
encryption key (e.g. shared across hosts/containers) instead of the auto-created
per-host `.credkey` file. `cryptography` must still be installed (see §4).

```bash
# generate
.venv/bin/python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Store it in the env file, readable only by the service user, then restart:

```bash
printf 'FSP_CRED_KEY=%s\n' 'pQK4Zt...c3g=' | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
sudo chown fsp:fsp /etc/fabric-shortcut-proxy.env
sudo chmod 600 /etc/fabric-shortcut-proxy.env
sudo systemctl restart fabric-shortcut-proxy.service
```

Keep the key stable. If it changes, existing entries in
`secrets/credentials.json` can't be decrypted and must be re-entered.

---

## 6. `POST /_config/api/credentials` → 400 (Save credentials)

The access log only shows the status; the reason is in the JSON response body
(config-builder red banner, or DevTools → Network → Response). Common causes:

- **No encryption backend** — see §4; restart the Manager after installing.
- **Masked URL** — the password field shows the stored `***`. Enter the real
  password, click **Test connection & list tables**, then Save. Saving is only
  valid on a freshly tested, unmasked URL.
- Empty form (`provide db_url or connection fields`) or
  `ENABLE_CREDENTIAL_STORE=0`.

---

## 7. Bind the Manager to all interfaces

The control plane binds `CONTROL_HOST` (default `127.0.0.1`).

```bash
# flag
bash Manager.sh --control-host 0.0.0.0
# env
export CONTROL_HOST=0.0.0.0
# persisted: config.system.json -> "control_host": "0.0.0.0"
```

`CONTROL_HOST` only affects the Manager control plane; agents use `HOST`/`PORT`
(default `0.0.0.0:9000`).

**Security:** `0.0.0.0` exposes the control plane (spawns/stops agents, mediates
credentials). Restrict port 9200 to trusted source IPs with a firewall/NSG,
enable TLS, and set an `ADMIN_TOKEN` (see §8).

---

## 8. Register as a systemd service

Dedicated user, env file for secrets, launch via `Manager.sh` (auto-update),
control plane on all interfaces, admin + config UIs guarded by a token.

```bash
# service user (no login)
sudo useradd --system --create-home --shell /usr/sbin/nologin fsp

# own the repo; rebuild the venv in place (venvs hold absolute paths)
sudo chown -R fsp:fsp /opt/fabric-shortcut-proxy
sudo -u fsp git config --global --add safe.directory /opt/fabric-shortcut-proxy
sudo -u fsp bash /opt/fabric-shortcut-proxy/Manager.sh --recreate --no-pull --control-host 0.0.0.0

# secrets (0600, owned by fsp)
sudo install -o fsp -g fsp -m 600 /dev/null /etc/fabric-shortcut-proxy.env
printf 'FSP_CRED_KEY=%s\n'  'pQK4Zt...c3g='          | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
printf 'ADMIN_TOKEN=%s\n'   "$(openssl rand -hex 24)" | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
```

Unit file `/etc/systemd/system/fabric-shortcut-proxy.service`:

```ini
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
ExecStart=/bin/bash /opt/fabric-shortcut-proxy/Manager.sh --control-host 0.0.0.0 --admin-ui --config-ui --auto-stash
Restart=on-failure
RestartSec=5
TimeoutStartSec=600
KillMode=control-group

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fabric-shortcut-proxy.service
systemctl status fabric-shortcut-proxy.service
```

UIs (port 9200): operator console at `/_manager` (fleet + start/stop/restart/
drain), config builder at `/_config/`. Mutating `/_manager` actions need the
token via `X-Admin-Token` header or `?token=`.

Notes:
- `Manager.sh` `exec`s the Manager in the foreground → `Type=simple`.
- `ADMIN_TOKEN` is read straight from the env (no CLI flag), keeping it out of
  `systemctl cat`.
- `--admin-ui` on `0.0.0.0` without a token = unauthenticated fleet control. Set
  the token and firewall the port.

---

## 9. SQL Server agents crash-loop with `42000 ... near 'LIMIT'`

**Symptom:** agents restart repeatedly; logs show pyodbc `42000`,
`Incorrect syntax near 'LIMIT'`, on the startup materialization query when
`SPLIT_STRATEGY=range`.

**Cause:** T-SQL has no `LIMIT`. The range strategy must emit `TOP`. (Fixed in
`planner/dialects.py`; if your deployment predates the fix, use the stopgap.)

**Immediate stopgap — no deploy:** switch to the modulo strategy, which already
emits the correct `TOP` form:

```bash
echo 'SPLIT_STRATEGY=modulo' | sudo tee -a /etc/fabric-shortcut-proxy.env >/dev/null
sudo systemctl restart fabric-shortcut-proxy.service
```

Range is more efficient (index-pruned); revert the override once the code fix is
deployed (§10).

**Confirm the strategy in use** from the log line: `strategy=range` vs
`strategy=modulo`. A healthy fixed range query reads
`SELECT TOP (:max_rows) ... ORDER BY [col]` with no `LIMIT`.

---

## 10. Deploying a code fix (auto-update flow)

The service launches `Manager.sh`, which `git pull`s `origin/main` on start. A
fix only reaches the box when it's **pushed to `origin/main`** and the service is
**restarted**.

```bash
# on your workstation
git push origin main

# on the server
sudo systemctl restart fabric-shortcut-proxy.service

# verify the pull actually landed
git -C /opt/fabric-shortcut-proxy log --oneline -1     # should match the pushed commit
```

If the deployed commit doesn't advance:
- the unit uses `--no-pull`, or
- local changes block the fast-forward → `cd /opt/fabric-shortcut-proxy &&
  sudo -u fsp git pull origin main` (add `--auto-stash` behavior as needed),
  then restart.

---

## Appendix: relevant config keys

| Setting | Env var | File | Notes |
| --- | --- | --- | --- |
| Control-plane host | `CONTROL_HOST` | `config.system.json` | default `127.0.0.1` |
| Control-plane port | `CONTROL_PORT` | `config.system.json` | default `9200` |
| Agent data plane | `HOST` / `PORT` | `config.system.json` | default `0.0.0.0` / `9000` |
| Split strategy | `SPLIT_STRATEGY` | `config.performance.json` | `modulo` \| `range` \| `date` \| `auto` |
| Credential store | `ENABLE_CREDENTIAL_STORE` | `config.system.json` | encryption backend required |
| Credential key | `FSP_CRED_KEY` | env only | urlsafe-base64 Fernet key |
| Admin console | `ENABLE_ADMIN_UI` | `config.system.json` | `/_manager` |
| Config builder | `ENABLE_CONFIG_BUILDER` | `config.system.json` | `/_config/` |
| Admin token | `ADMIN_TOKEN` | env / `config.system.json` | guards mutating `/_manager` |
| Source DB URL | `DB_URL` / `DB_URL_<ID>` | `config.connection.json` | keep secrets in env, not files |
