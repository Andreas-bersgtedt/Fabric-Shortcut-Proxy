# Security Policy

## Credential Management

**CRITICAL**: All credentials (passwords, API keys, tokens) MUST be loaded from environment variables, never hardcoded in configuration files or code.

### Files with Sensitive Information

The following files may contain credentials and are **never** committed to version control:

```
config.connection.json       # Database connection URL with credentials
config.system.json          # AWS keys for S3 access
config.performance.json     # Optional: reserved for future sensitive settings
config.mounts.json          # Storage-proxy mount table (references credential ids, not secrets)
secrets/credentials.json    # Encrypted store: DB URLs, upstream S3/Azure creds, access keys
.env                        # Local development environment variables
.env.local                  # Local overrides (not committed)
secrets.json               # Any secret material (never commit)
```

All these files are protected by `.gitignore` to prevent accidental commits.

### Required Environment Variables

For production deployments, set these environment variables:

```bash
# Database connection (REQUIRED if DB_URL not in config.connection.json)
export DB_URL="mssql+aioodbc://username:password@host:1433/database?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"

# AWS S3 credentials (REQUIRED for S3 access)
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_SESSION_TOKEN="optional_session_token"  # For temporary credentials

# Optional: override default control port
export CONTROL_PORT="9200"
export PORT="9000"
```

### Configuration File Format

Config files contain only **empty** or **non-sensitive** values:

```json
{
  "connection": {
    "db_url": "",            # Populated from DB_URL environment variable
    "db_max_retries": 3,     # OK: non-sensitive setting
    "db_retry_backoff_seconds": 1
  }
}
```

When the application starts:
1. Load config from JSON file
2. Override `db_url` from `DB_URL` environment variable (if set)
3. Validate no hardcoded credentials remain

### How to Set Credentials Locally

#### Windows PowerShell

```powershell
$env:DB_URL = "mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"
$env:AWS_ACCESS_KEY_ID = "your_key"
$env:AWS_SECRET_ACCESS_KEY = "your_secret"

# Then start the application
python -m enterprise.manager
```

#### Linux / macOS Bash

```bash
export DB_URL="mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"

python -m enterprise.manager
```

#### Using .env file (development only)

Create `.env.local` in the project root (never commit):

```bash
DB_URL=mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```

Then load it before running:

```bash
source .env.local          # macOS/Linux
Get-Content .env.local | ForEach-Object { if ($_) { $_ -split '=' | % { $env:$_[0] = $_[1] } } }  # PowerShell
```

### Credential Scrubbing in Logs

All logging automatically scrubs credentials using the `security.credentials` module:

```python
from security import scrub_secrets, scrub_dict

# Scrub individual strings
scrubbed = scrub_secrets("password=mysecret123")  # → "password=[REDACTED]"

# Scrub dictionary recursively
config_copy = scrub_dict(config)  # → all password fields become [REDACTED]
```

**Important**: Never print raw config objects or database URLs to logs. Always use `scrub_dict()` or `scrub_secrets()`.

### Validating Configuration Files

On startup, the application validates that no hardcoded credentials exist:

```python
from security import validate_no_hardcoded_credentials

# Raises ValueError if credentials found
validate_no_hardcoded_credentials(config)
```

This check runs automatically during application startup and will fail loudly if credentials are detected.

## Git History Cleanup

If credentials have been committed to git history, use `git-filter-branch` or `bfg-repo-cleaner` to remove them:

```bash
# Using bfg (simpler, recommended)
bfg --replace-text passwords.txt --no-blob-protection

# Or using git-filter-branch (more control)
git filter-branch --tree-filter 'sed -i "s/mysecret123/[REDACTED]/g"' HEAD
```

After cleaning history, force-push (carefully, on a feature branch only):

```bash
git push origin --force-with-lease
```

## Storage Proxy Security (Phase 4)

When the storage proxy serves **mounted buckets** (existing files from `local` /
`s3` / `azure` backends), the S3 front door is hardened beyond the single-key
POC path. Grounded in [security/access_keys.py](../security/access_keys.py),
[s3/auth.py](../s3/auth.py), [main.py](../main.py) (auth middleware), and
[observability/audit.py](../observability/audit.py).

### Front-door authentication (inbound)

- **AWS SigV4**, verified against **many scoped access keys** (not just one static
  pair). Each key is resolved by the access-key id presented in the request's
  `Credential=` scope. The legacy single `S3_ACCESS_KEY_ID` keeps working as an
  implicit wildcard **until the first access key is created**, then coexists as a
  wildcard key you can remove/rotate.
- **Forced auth on mounts**: `ENFORCE_MOUNT_AUTH` (default `1`) requires a valid
  signature on any mounted bucket **even when `REQUIRE_SIGV4=0`**. A secured mount
  is never served anonymously.
- Not supported inbound: presigned-URL/query-string auth, STS session tokens, SigV2.

### Per-key authorization (ACL)

Each access key carries an authorization scope, stored **encrypted**:

```jsonc
{ "access_key_id": "FSP…", "secret_key": "…", "label": "finance-reader",
  "allowed_buckets": ["secure-nfs", "s3vault"],
  "allowed_prefixes": { "s3vault": ["2026/"] },   // optional finer scope
  "permissions": "read", "enabled": true }
```

- `allowed_buckets` may be `["*"]` (all) or a fixed list; `allowed_prefixes`
  confines a key to sub-paths. Out-of-scope requests are rejected with
  `AccessDenied` (403).
- **Read-only** in v1: write methods (PUT/DELETE/POST) are always denied.
- Manage keys in the Config Builder **Security → Access keys** panel, or via
  `/_config/api/access-keys` (create returns the secret **once**; rotate/delete
  supported).

### Encrypted backup and restore

The Config Builder **Security** area creates portable `.fspbackup` archives. A backup contains
the split configuration files, logical encrypted-store records, scoped access keys, and Open
Mirroring recovery state. It does not copy machine-bound DPAPI or Fernet ciphertext directly:
the archive decrypts those records in memory, protects the complete payload with a password,
and re-encrypts each record with the destination store during restore.

The archive key is derived with scrypt and the payload is authenticated and encrypted with
AES-256-GCM. Use a unique password of at least 12 characters, transfer the archive through a
protected channel, and store its password separately. Restore rejects the wrong password,
tampering, unsupported files, and unsafe state paths before replacing data. Config files,
credentials, and mirror state are restored as one transaction and rolled back on write failure.

Backups do not include source datasets, generated artifacts or caches, logs, environment-only
secrets, external TLS key files, or the contents of a remote Key Vault. Back up those systems
according to their own recovery procedures. Restart the Manager after a successful restore.
See [BACKUP_RESTORE.md](BACKUP_RESTORE.md) for the operator procedure and exact scope.

### Upstream credential mediation (outbound)

Clients **never** see the credentials the proxy uses to reach the upstream S3 or
Azure backend. Those secrets are held in the encrypted credential store
([security/credential_store.py](../security/credential_store.py); DPAPI on Windows,
Fernet elsewhere) and resolved by the mount's `credential` id, never written to
`config.mounts.json`.

- **Outbound S3** modes: `static`, `session` (STS temp), `assume_role`
  (auto-refresh), `web_identity` (OIDC/IRSA), `profile`, `sso`, `instance`
  (default chain), `process`, `anonymous`.
- **Outbound Azure** modes: `connection_string`, `account_key`, `sas`,
  `aad_client_secret` (service principal), `managed_identity`, `default`
  (DefaultAzureCredential), `anonymous`.
- A credential-less mount must declare an explicit `auth` mode (S3:
  `anonymous`/`instance`; Azure: `default`/`managed_identity`/`anonymous`) so
  ambient host credentials are never picked up by surprise.

### Entra ID identity & Azure Key Vault (issue #16)

The proxy can acquire its **own** outbound Azure identity through Entra ID and use
**Azure Key Vault** as a central, RBAC-audited credential store. This is distinct from
the per-mount Azure auth above: it governs how the *proxy itself* authenticates to Key
Vault, and the same identity is reused for Azure storage mounts.

- **Identity mode** (`AUTH_MODE`): `default` (DefaultAzureCredential — managed identity,
  environment, or CLI), `managed_identity`, or `service_principal`. A service-principal
  client secret is read only from `AZURE_CLIENT_SECRET` (environment), never a config
  file. The credential is built once in
  [security/azure_credential.py](../security/azure_credential.py) and shared.
- **Read-through source** (`KEYVAULT_URI`): on a local cache miss the encrypted store
  resolves a secret from Key Vault and caches it, so the DB URL, mount credentials, S3
  secret, admin token, and Manager password can live in the vault. A background loop
  re-pulls on `KEYVAULT_REFRESH_SECONDS`.
- **Cache-first, never-fail:** a Key Vault, Azure, or network outage falls back to the
  local encrypted cache. `KEYVAULT_CACHE_TTL=0` (default) never expires it, so an offline
  or air-gapped deployment runs entirely from the local store. `REQUIRE_KEYVAULT=1` opts
  into failing fast on a cold start with no cache.
- **Write-back — the vault as authoritative store** (`KEYVAULT_WRITE_BACK`, default off):
  the Manager also persists every operator-saved credential into Key Vault — DB URLs,
  mount credentials, the S3 secret / admin token / Manager password, and per-key S3 access
  keys **with their full ACL scope** (allowed buckets/prefixes, permissions, enabled) — and
  soft-deletes the vault secret when a credential is removed. A rebuilt Manager or a fresh
  agent re-populates from the vault. Fail-soft: a Key Vault write failure never blocks the
  local save.
- **RBAC:** the Manager and agents need **Key Vault Secrets User** to read; write-back
  additionally needs **Key Vault Secrets Officer** on the **Manager** identity only (agents
  stay read-only).
- **Secret names:** `db-url` / `db-url-<id>`, `s3-secret-access-key`, `admin-token`,
  `manager-auth-password`, `access-key-<id>`, and mount secrets by id (override per
  deployment).
- **Status:** an advisory `key_vault` block in `/readyz`, a status card in the monitor and
  admin console, and a config-builder **Entra ID & Key Vault** panel with a live **Test**
  button (`GET /_config/api/keyvault`, `POST /_config/api/keyvault/test`). Install the
  optional `keyvault` extra (`pip install '.[keyvault]'`).

| Setting | Default | Purpose |
|---|---|---|
| `AUTH_MODE` | `default` | Outbound Azure identity: `default`, `managed_identity`, `service_principal` |
| `KEYVAULT_URI` | *(unset)* | Key Vault URI; empty disables Key Vault |
| `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` | *(unset)* | Entra tenant / client for a service principal or user-assigned managed identity |
| `AZURE_CLIENT_SECRET` | *(unset)* | Service-principal secret — **environment only**, never a config file |
| `REQUIRE_KEYVAULT` | `0` | Fail-fast on a cold start with no local cache |
| `KEYVAULT_REFRESH_SECONDS` | `300` | Background re-pull cadence (seconds) |
| `KEYVAULT_CACHE_TTL` | `0` | Local-cache TTL; `0` = never expire (offline-friendly) |
| `KEYVAULT_WRITE_BACK` | `0` | Manager persists saved credentials into Key Vault (needs Secrets Officer) |

### Path confinement

Every mounted key is normalized and rejects `..` traversal, and is confined to the
mount's `prefix` subtree, so a request can never escape its backend root (OWASP
A01/A03). This applies to `local`, `s3`, and `azure` backends alike.

### TLS

- SigV4 requests must carry a valid `x-amz-date` matching the credential-scope
  date and within 15 minutes of proxy UTC time. Keep proxy hosts synchronized
  with a trusted time source.
- The SigV4 verifier supports actual-body SHA-256 validation for body-bearing
  requests and rejects missing hashes, mismatches, and `UNSIGNED-PAYLOAD` in
  that mode. The current S3 data plane is read-only; writeback must pass the
  consumed or streaming-verified body into this verifier before enabling writes.
- Terminate HTTPS **at the proxy** by setting **both** `TLS_CERT_FILE` and
  `TLS_KEY_FILE` (wired into uvicorn for [main.py](../main.py) and
  [enterprise/manager.py](../enterprise/manager.py)), or terminate at a fronting load balancer.
- SigV4 read requests use `UNSIGNED-PAYLOAD`, so signatures give **no
  confidentiality** over plain HTTP, enable TLS before turning on auth. The proxy
  logs a startup warning when auth/mounts are on but TLS is not configured.

### Manager and browser access

Standalone and Manager operator surfaces require Basic, local-session, or OIDC
authentication. `MANAGER_AUTH_ENABLED=1` is the default. Disabled authentication or
a blank `MANAGER_AUTH_PASSWORD` prevents startup when `HOST` (standalone) or
`CONTROL_HOST` (Manager) is not loopback. Loopback-only development instances may
start without complete credentials, but operator requests still fail closed with 503.
Configure the password in the systemd environment file or another protected secret source.
This protects `/_admin`, `/_manager`, `/_config`, `/_monitor`, `/agents`, and Manager
control routes. S3 data routes use SigV4 independently. Health and readiness probes
remain unauthenticated.

Browser requests from another origin are blocked unless that origin is listed in
`CORS_ALLOWED_ORIGINS` as a comma-separated list, for example
`https://admin.example.com`. CORS does not replace Basic authentication. Direct
same-origin access to `http://<manager-host>:9200/_manager` and `/_config` does
not require a CORS entry, but it still requires Manager credentials.

### External OIDC identity provider

Install provider support with `pip install -e ".[oidc]"`, then set:

```bash
FSP_OIDC_ISSUER=https://login.microsoftonline.com/<tenant-id>/v2.0
FSP_OIDC_AUDIENCE=<application-client-id-or-api-audience>
FSP_OIDC_USER_CLAIM=oid
```

For an Entra v1 issuer (`https://sts.windows.net/<tenant-id>/`), preserve the
trailing slash in `FSP_OIDC_ISSUER` and configure the tenant key endpoint:

```bash
FSP_OIDC_ISSUER=https://sts.windows.net/<tenant-id>/
FSP_OIDC_JWKS_URL=https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys
```

`FSP_OIDC_JWKS_URL` may override discovery when an identity provider does not
publish keys through `<issuer>/.well-known/openid-configuration`. The override
must be the provider's HTTPS JWKS endpoint.

Operator requests may send `Authorization: Bearer <access-token>`. The proxy
validates the JWT signature, issuer, audience, issued-at time, expiration, and
subject. It then maps `FSP_OIDC_USER_CLAIM` (`sub` by default) to `user_id` in
the existing central user directory. Roles and grants in the token are ignored;
all function rights remain owned by the proxy's central user administration.
Unknown and disabled users fail closed. Keep `FSP_AUTHZ_ENFORCE=1` enabled and
terminate TLS before accepting bearer credentials.

### Audit logging

- With `ENABLE_AUDIT_LOG=1` (default), every mounted-object access emits a
  structured `audit` event, `identity`, `client`, `bucket`, `key`, `backend`,
  `method`, `status`, `bytes`, with secrets scrubbed. Auth and authorization
  **denials are audited too**.
- Events go to the structured logger, an optional append-only file
  (`AUDIT_LOG_FILE`), and an in-memory ring surfaced at `GET /_config/api/audit`.

### Settings summary

| Setting | Default | Purpose |
|---|---|---|
| `ENABLE_STORAGE_PROXY` | `0` | Serve mounted buckets as passthrough |
| `ENFORCE_MOUNT_AUTH` | `1` | Require SigV4 on mounts even if `REQUIRE_SIGV4=0` |
| `REQUIRE_SIGV4` | `1` | Enforce SigV4 on **all** buckets |
| `CORS_ALLOWED_ORIGINS` | *(empty)* | Disable cross-origin browser access unless explicitly configured |
| `AGENT_HOST_ALLOWLIST` | `127.0.0.1,0.0.0.0,::1,::,localhost` | Restrict registered Agent bind and advertised hosts |
| `ENABLE_AUDIT_LOG` | `1` | Audit every mounted-object access |
| `AUDIT_LOG_FILE` | *(unset)* | Optional append-only audit file |
| `TLS_CERT_FILE` / `TLS_KEY_FILE` | *(unset)* | Serve HTTPS at the proxy |

### Operator deployment and recovery

Upgrade from a release before 2.9.1 in this order:

1. Create a random `MANAGER_AUTH_PASSWORD` in the deployment secret store. Set
  `MANAGER_AUTH_ENABLED=1` and `FSP_AUTHZ_ENFORCE=1`.
2. Assign each operator the narrowest built-in role that covers the work:
  `viewer`, `config_operator`, `tokenization_administrator`,
  `security_administrator`, `user_administrator`, or `system_administrator`.
3. Terminate TLS at the process or ingress before exposing an operator port.
4. Deploy one instance, run the verification commands below, then continue the rollout.

For Kubernetes, inject `MANAGER_AUTH_PASSWORD` from a Secret into the `fsp-manager`
Deployment. Rotate it by updating the Secret and restarting the Deployment:

```bash
kubectl -n <namespace> create secret generic fsp-operator-auth \
  --from-literal=MANAGER_AUTH_PASSWORD='<new-random-value>' \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n <namespace> rollout restart deployment/fsp-manager
kubectl -n <namespace> rollout status deployment/fsp-manager
```

Do not disable authentication to recover access. For break-glass recovery, use host
console access or `kubectl` access to replace the protected password, restart only the
affected workload, verify access, and rotate the temporary password again. This does
not create a network-accessible bypass. Record the operator, ticket, and rotation time
in the incident log.

To roll back the application, restore the previous image while retaining the current
operator password, TLS configuration, user directory, and credential store. If the old
release cannot consume the current configuration, first isolate the operator service
from non-loopback networks, restore the pre-upgrade `.fspbackup` described in
[BACKUP_RESTORE.md](BACKUP_RESTORE.md), and start the prior image. Never solve a rollback
failure by publishing an unauthenticated operator port.

### Production verification

Run these checks against both the standalone URL and the Manager URL after deployment.
Use HTTPS in production. A protected endpoint must reject an anonymous request and
accept the configured operator:

```bash
curl -i https://<standalone-host>:9000/_admin/stats
curl -i -u '<operator>:<password>' https://<standalone-host>:9000/_admin/stats
curl -i https://<manager-host>:9200/_manager/api/health
curl -i -u '<operator>:<password>' https://<manager-host>:9200/_manager/api/health
```

The anonymous responses must be 401. The authenticated responses must be 200 for an
authorized role or 403 for an authenticated role that lacks the named permission.
Also confirm `/healthz` and `/readyz` return 200 without credentials, and confirm an S3
request still requires SigV4 rather than Basic credentials. Review the audit log for
the denied operator requests and verify that it contains no password or authorization
header values.

The control design and release criteria are tracked in
[the 2.9.0 security review](../devplan/290_sec_review.md), GitHub Epic #53, and child
issues #54 through #62.

## Deployment Checklist

Before deploying to production:

- [ ] All config JSON files have empty credential fields (`""`)
- [ ] All credentials are set via environment variables
- [ ] `.gitignore` includes `config.*.json` entries
- [ ] Run `git log --all -p` to verify no recent commits contain passwords
- [ ] Test that application starts successfully with only env vars (no JSON creds)
- [ ] Confirm non-loopback startup fails when operator auth is disabled or incomplete
- [ ] Confirm anonymous operator requests return 401 and authorized requests return 200
- [ ] Confirm TLS protects every non-loopback operator endpoint
- [ ] Confirm operator users have least-privilege roles
- [ ] Test password rotation and the rollback procedure in a non-production environment
- [ ] Review logs for any credential leaks using `scrub_secrets()`
- [ ] Store credentials in:
  - Azure Key Vault (recommended for Azure deployments)
  - AWS Secrets Manager (for AWS)
  - HashiCorp Vault (for hybrid/multi-cloud)
  - Or your organization's secure credential store

## Incident Response

If credentials are exposed:

1. **Immediately rotate** all exposed credentials
2. **Purge git history** using the steps above
3. **Audit logs** for unauthorized access patterns
4. **Update code** to use `scrub_secrets()` in relevant places
5. **Run** `git log --all -p | grep -i password` to verify they're gone
6. **Force-push** cleaned history to all branches
7. **Notify** security team and affected parties

## Security Best Practices

1. **Never commit sensitive files**
   - Use `.gitignore` (already configured)
   - Use pre-commit hooks to catch credentials before commit

2. **Use environment variables for all credentials**
   - Easy to rotate without code changes
   - Secrets never stored in git
   - Different secrets per environment (dev/staging/prod)

3. **Scrub logs and tracebacks**
   - Application uses `scrub_secrets()` automatically
   - Verify sensitive fields in error messages are scrubbed
   - Use `scrub_dict()` before logging config objects

4. **Rotate credentials regularly**
   - S3 keys: at least quarterly
   - Database passwords: as per organizational policy
   - API tokens: per service requirements

5. **Audit access logs**
   - Review failed authentication attempts
   - Monitor S3 access patterns for anomalies
   - Check database connection logs for unauthorized access

## Contact

For security issues, **do not** create public GitHub issues. Instead:
- Contact the security team directly
- Use GitHub's private vulnerability disclosure feature
- Follow responsible disclosure timeline (90 days)

---

**Last Updated**: 2026-07-28  
**Status**: ACTIVE - All config files cleaned, credential scrubbing implemented
