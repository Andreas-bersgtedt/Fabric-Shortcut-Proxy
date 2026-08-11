# Chapter 7: Security

This chapter covers the security model you configure before exposing the proxy beyond a
lab: credentials, SigV4 keys and per-key authorization, upstream credential mediation, TLS,
audit, and column tokenization. The authoritative policy is in [SECURITY.md](../SECURITY.md);
the tokenization design is in [TOKENIZATION_PUSHDOWN.md](../TOKENIZATION_PUSHDOWN.md).

## 7.1 Security model at a glance

```mermaid
flowchart LR
  Fabric[Fabric / S3 client]
  subgraph Proxy[Proxy]
    SIG[SigV4 verify + per-key ACL]
    MED[Credential mediation<br/>encrypted store]
    AUD[Audit log]
  end
  SRC[(Source DB)]
  UP[(Upstream S3 / Azure / file share)]

  Fabric -->|SigV4 keys only| SIG
  SIG --> MED
  MED -->|DB password| SRC
  MED -->|upstream secret| UP
  SIG -. denials .-> AUD
```

Two boundaries matter. Inbound, Fabric authenticates with SigV4 keys and nothing else.
Outbound, the proxy holds the database password and the upstream storage secrets and
resolves them by id. Fabric never sees a source or upstream secret. This mediation is what
makes reading private data without a copy safe.

## 7.2 Credential handling

All credentials load from environment variables or the encrypted store, never from
committed files. The gitignored files that may reference secrets are
`config.connection.json`, `config.system.json`, `config.mounts.json` (which references
credential ids, not secrets), and `secrets/credentials.json` (the encrypted store).

Set secrets from the environment in production:

```powershell
$env:DB_URL = "mssql+aioodbc://user:pass@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
$env:S3_ACCESS_KEY_ID = "your_key"
$env:S3_SECRET_ACCESS_KEY = "your_secret"
```

The config builder never writes a database password into a config file unless you opt in,
and a masked (`***`) connection string is never stored, hydrated, or treated as a credential.

## 7.3 SigV4 and per-key authorization

Inbound requests are verified with AWS SigV4. Beyond a single legacy key, the proxy supports
scoped access keys: each key has an allowed set of buckets and prefixes and is read-only.
SigV4 is verified against the presented access-key id, and the verified identity is used for
authorization and audit.

- The legacy single key acts as an implicit wildcard until you create the first scoped key.
- After that, each request is authorized against the presenting key's allowed buckets and
  prefixes.
- Manage keys in the config builder Storage tab or via `/_config/api/access-keys` (create
  returns the secret once; rotate and delete are supported).

Turn on enforcement with `REQUIRE_SIGV4=1`. Mounted buckets are a special case: they are
authenticated even when `REQUIRE_SIGV4=0`, because `ENFORCE_MOUNT_AUTH` defaults on. A
secured mount is never served anonymously.

## 7.4 Upstream credential mediation

Outbound secrets for storage-proxy mounts are held encrypted and resolved by id, never
exposed to clients or written into `config.mounts.json`. The store uses DPAPI on Windows and
Fernet elsewhere (install the `credentials` extra on non-Windows hosts). It also holds
connection strings and access keys, and survives restarts, hydrating `DB_URL` and
`DB_URL_<ID>` at startup.

Supported outbound auth modes:

- **S3:** static, session, assume_role, web_identity, profile, sso, instance, process,
  anonymous.
- **Azure:** connection_string, account_key, sas, aad_client_secret, managed_identity,
  default, anonymous.

A credential-less mount must declare an explicit `auth` mode (for example `anonymous` or
`instance` for S3; `default`, `managed_identity`, or `anonymous` for Azure).

## 7.5 Path safety on mounts

The mount registry normalizes every key and rejects `..` traversal, confining reads to the
mount `prefix` subtree across all backends. This addresses the OWASP path-traversal and
broken-access-control classes for the passthrough path. Mounts are read-only.

## 7.6 TLS

Terminate HTTPS at the proxy by setting both `TLS_CERT_FILE` and `TLS_KEY_FILE`, for the
agent and the Manager control plane. Alternatively, terminate TLS at a fronting load
balancer or reverse proxy and keep the app ports on loopback. The public-internet procedure
with nginx is in [SSL_Deployment.md](../../SSL_Deployment.md).

## 7.7 Audit

With `ENABLE_AUDIT_LOG=1`, every mounted-object access is recorded with the identity,
bucket, key, and byte count, and mount auth denials are recorded too. Recent events are
available at `GET /_config/api/audit`; the file is set by `AUDIT_LOG_FILE`. Behind a private
load balancer, set `forwarded_allow_ips` so the audit log records the real client IP rather
than the load balancer's.

## 7.8 Column tokenization

Tokenization pushes column minimization into the source engine so plaintext PII never leaves
the database. It is a projection-time policy per column, not reversible detokenization and
not a masking service.

Three policies per column:

- **Deterministic token.** A stable, keyed SHA-256 token. Equal normalized inputs produce
  equal tokens, so equality joins, `GROUP BY`, and `DISTINCT` still work without exposing
  the value.
- **Random token.** A fresh token on every read. Equality relationships are intentionally
  lost.
- **Remove.** The column is omitted from the output schema and never appears in Parquet or
  metadata.

The token SQL is native per engine:

| Dialect | Deterministic | Random |
|---|---|---|
| SQL Server | `HASHBYTES('SHA2_256', ...)` | `NEWID()` |
| PostgreSQL | `digest(..., 'sha256')` (requires `pgcrypto`) | `gen_random_uuid()` |
| Oracle | `STANDARD_HASH(..., 'SHA256')` | `SYS_GUID()` |
| Databricks SQL | `sha2(..., 256)` | `uuid()` |

Key handling and guarantees:

- The token key is referenced by a logical name (for example `customer-pii-v1`) and resolved
  only from the environment variable `FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1`. The secret is
  never stored in table configuration.
- Token bind parameters use letter-prefixed `fsp_token_*` names and are redacted from logs;
  SQLAlchemy hides parameter values in exceptions.
- Deterministic tokens reveal equality and frequency. Use a per-column domain so the same
  plaintext gets different tokens in unrelated columns, and share a domain and key version
  only when a cross-table equality join is an explicit requirement.
- Normalization modes are `none`, `trim`, and `trim_lower`. Cross-engine token equality is
  not guaranteed because string encoding and case folding differ per database.

The proxy fails closed: unsupported dialects (SQLite), missing keys, transformed split keys,
and random tokens combined with content-hash refresh are all rejected at startup. Configure
policies in the config builder per-column editor. Validate with the UAT runbooks:
[TOKENIZATION_UAT.md](../TOKENIZATION_UAT.md) (SQL Server) and
[TOKENIZATION_MULTI_DIALECT_UAT.md](../TOKENIZATION_MULTI_DIALECT_UAT.md) (PostgreSQL, Oracle,
Databricks).

## 7.9 Entra ID identity and Azure Key Vault

Beyond per-mount Azure auth, the proxy can take its **own** Entra ID identity and use Azure
Key Vault as a central credential store. `AUTH_MODE` selects the outbound identity —
`default` (DefaultAzureCredential), `managed_identity`, or `service_principal` — and a
service-principal secret comes only from `AZURE_CLIENT_SECRET` in the environment, never a
config file. The same identity is reused for Azure storage mounts.

Set `KEYVAULT_URI` to turn Key Vault on. On a local cache miss the encrypted store resolves
the secret from the vault and caches it, so the DB URL, mount credentials, S3 secret, admin
token, and Manager password can live centrally; a background loop re-pulls on
`KEYVAULT_REFRESH_SECONDS`. Key Vault is never a hard dependency: an outage falls back to the
local encrypted cache, and `KEYVAULT_CACHE_TTL=0` (default) never expires it, so an offline
deployment runs entirely from the local store. Set `REQUIRE_KEYVAULT=1` only if you want a
cold start with no cache to fail fast.

Turn on `KEYVAULT_WRITE_BACK` to make the vault the authoritative store. The Manager then
persists every saved credential into Key Vault as well as the local cache — DB URLs, mount
credentials, the S3 secret, admin token, and Manager password, and per-key S3 access keys
**with their ACL scope** — and soft-deletes the vault secret when you delete a credential. A
rebuilt Manager or a fresh agent re-populates from the vault. Write-back is fail-soft: a Key
Vault failure never blocks the local save. Grant **Key Vault Secrets User** to read and, for
write-back, **Key Vault Secrets Officer** on the Manager identity only; agents stay
read-only. Check status in the config builder's Entra ID & Key Vault panel (with a live Test
button), in `/readyz`, and on the monitor. Install the `keyvault` extra
(`pip install '.[keyvault]'`); the Manager launchers install it by default. Full policy is in
[SECURITY.md](../SECURITY.md).

## 7.10 Hardening checklist

- Give the proxy a read-only database login scoped to the exposed tables.
- Set `REQUIRE_SIGV4=1` and issue scoped access keys; stop relying on the legacy wildcard.
- Keep `ENFORCE_MOUNT_AUTH=1`.
- Keep the control plane (9200) off the data-facing network.
- Terminate TLS at the proxy or a fronting endpoint.
- Turn on the audit log and set `forwarded_allow_ips` behind a load balancer.
- Store all secrets in the environment or the encrypted store; commit none. Centralize them
  in Azure Key Vault (`KEYVAULT_URI`, optionally `KEYVAULT_WRITE_BACK`) where available.

## 7.11 Next

Continue to [Chapter 8: Operations](08-operations.md) for running, monitoring, scaling, and
troubleshooting the service.
