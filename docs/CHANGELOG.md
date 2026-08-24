# Changelog

All notable changes to the Fabric Shortcut Proxy are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- The Config Builder Open Mirror editor now saves target and per-table cleanup retention values.
- The Manager Monitor now includes a Data File Manager for Open Mirror cleanup. It inspects
  Fabric's `_FilesReadyToDelete` folders, applies per-target or per-table retention, defaults to
  dry-run inspection, and deletes only eligible processed folders after explicit confirmation.

## [2.5.3]: 2026-08-24

### C++ serving agent
- The C++ serving agent now enforces bucket routing, rejects traversal and malformed request
  inputs, uses bounded worker concurrency, and supports Linux build and smoke-test validation.
- C++ ListObjectsV2 now supports pagination, delimiter prefixes, continuation-token validation,
  and a persisted sorted object index with configurable periodic refresh.

### Performance
- Large-store listing now reads pages from the persisted index instead of walking and sorting the
  filesystem for every request.

## [2.5.2]: 2026-08-20

### Security
- Manager operator, configuration, monitoring, Agent registration, and control routes
  now fail closed behind HTTP Basic authentication when Manager auth is enabled.
- Agent administrative and configuration routes no longer bypass SigV4, which is enabled
  by default. Missing S3 credentials now stop startup instead of silently exposing the API.
- Agent registration validates ports and restricts bind and advertised hosts to the configured
  `AGENT_HOST_ALLOWLIST`, reducing gateway target injection and SSRF risk.
- Cross-origin browser access is disabled by default and requires explicit
  `CORS_ALLOWED_ORIGINS` entries.

### Changed
- Release metadata and runtime API version strings now report `2.5.2`.
- The installer stores Manager, Agent, and S3 secrets in the protected environment file or
  configured Key Vault backend. Admin password reset accepts a hidden, confirmed password.

## [2.5.1]: 2026-08-19

### Added
- Open Mirror watermark tables now use version 2 state with a composite watermark and key
  cursor, drain multiple pages per cycle, and commit each page after its numbered Parquet file
  exists.
- The Manager checks Fabric mirroring before source extraction. It can start `Initialized`,
  `Paused`, or `Stopped` mirroring with its existing Entra credential, then polls with bounded
  retries and a per-target cooldown.

### Fixed
- Corrupt, unreadable, and unsupported Open Mirror state now stops the table instead of starting
  a full load. Empty initial reads persist `initialized=true`.
- Pending file metadata closes the upload-to-cursor crash window. Restart finalizes an existing
  pending file or retries a missing file at the reserved path.
- CI no longer selects the systemd state directory on GitHub Actions runners. Tests isolate their
  state directory under the test temporary directory.

### Changed
- Publish results now report strategy, reason, cursors, pages, scanned rows, published rows,
  state status and path, query mode, and recovery action. Table reset requires a named target,
  named table, and explicit confirmation.
- Both distributions and their runtime API and agent version metadata now report `2.5.1`.

## [2.5.0]: 2026-08-13

### Added
- **Open Mirroring publisher (`open_mirror` package).** The proxy can now push a source table
  into a Microsoft Fabric **Open Mirroring** landing zone, alongside the existing
  shortcut/virtualization path. Targets live in `config.open_mirror.json` and each binds to an
  **existing source connection** (from `config.connection.json`) — the source database engine
  and connector selection are unchanged; only the sink is new.
  - **Landing-zone writer** conforming to the [landing-zone format](https://learn.microsoft.com/fabric/mirroring/open-mirroring-landing-zone-format):
    per-table folders (optionally under a `<schema>.schema` folder), a `_metadata.json` with
    `keyColumns`, an optional database-level `_partnerEvents.json`, and monotonically numbered
    20-digit Parquet data files.
  - **Incremental change stream.** A local, gitignored snapshot store (kept outside the landing
    zone) diffs the source each cycle to emit `__rowMarker__` **insert/update/delete** rows.
    Retry-safe: the snapshot advances only after the change file is durably written.
  - **Source watermark mode.** A table may declare a monotonic `watermark_column`
    (id/timestamp/rowversion); incremental cycles then read only `WHERE wm > :last ORDER BY wm`
    (per dialect, bounded) and publish **upserts** (`__rowMarker__=4`), storing the last
    watermark (type-tagged so it survives restart). Efficient for large tables; deletes are not
    visible to a watermark query (leave it blank for the full snapshot diff).
  - **OneLake (ADLS Gen2) backend** authenticated with the proxy's **own Entra identity** — the
    same service principal / managed identity / default credential already used for Key Vault
    (issue #16). Local/UNC staging paths are also supported. Azure SDK is the optional `onelake`
    extra.
  - **Runtime trigger.** A background publish loop on the Manager (`OPEN_MIRROR_PUBLISH`, with
    `OPEN_MIRROR_INTERVAL_SECONDS` / `OPEN_MIRROR_MODE` / `OPEN_MIRROR_MAX_ROWS` /
    `OPEN_MIRROR_STATE_DIR`), plus on-demand **Publish now** / **Dry run** in the config builder.
    Per-table failures are quarantined; a table removed from config has its landing-zone folder
    dropped (add/drop reconciliation).
  - **Config-builder Open Mirror tab.** A dedicated tab (separate from the source Connection tab)
    to bind a target to a connection, **pick source tables** from the connection with
    auto-detected key columns, set an optional watermark column, and **browse Fabric workspaces
    and mirrored databases** (via the proxy identity) so the OneLake landing-zone root is filled
    in automatically — no URL pasting.
- **Manager launchers install the `onelake` extra.** `Manager.sh` and `Manager.ps1` now install
  `azure-storage-file-datalake` + `azure-identity` and bump the dependency stamp so existing
  `.venv`s auto-reinstall on next launch.

### Fixed
- **Config builder — settings now reflect saved values.** The Advanced "All Settings" panel
  prefills each field with its current effective value (env / file) instead of always showing
  the built-in default, so persisted overrides no longer look "not sticky" after a reload.
- **Open Mirroring targets may bind to an already-configured connection**, not only connections
  re-sent in the same save payload.

### Changed
- Both distributions and their runtime API / agent version metadata now report `2.5.0`.

## [2.4.0]: 2026-08-11

### Added
- **SPN + Windows authentication for the SQL Server source connector (issue #19).** The MS
  SQL connector now supports, alongside SQL logins: **Windows Authentication** (Integrated
  Security via `Trusted_Connection=yes` — the Manager/agent process identity; Kerberos on
  Linux) and an **Entra ID service principal** (`Authentication=ActiveDirectoryServicePrincipal`
  over an encrypted channel, with the client id/secret as UID/PWD; needs ODBC Driver 18 or
  17.4+). The Config Builder gains an **Authentication** selector (SQL Server only) that swaps
  the credential inputs per method; the SPN secret is encrypted in the Manager credential
  store (and mirrored to Key Vault when write-back is on). SQL Login stays the default and is
  unchanged; connection Test/validation and clear auth-failure errors apply to all three.
- **Reuse the proxy's own Entra identity for SQL Server (issue #19).** A fourth SQL Server
  auth option, **Entra ID — reuse the proxy identity**, authenticates with the identity the
  proxy already uses for Key Vault (issue #16) — no separate credentials. It maps the proxy's
  `auth_mode` onto the ODBC keyword: `service_principal` -> `ActiveDirectoryServicePrincipal`
  (the configured `azure_client_id` + `AZURE_CLIENT_SECRET`), `managed_identity` ->
  `ActiveDirectoryManagedIdentity`, else `ActiveDirectoryDefault`. Grant that identity a SQL
  login + read access on the source. Validated end-to-end against Azure SQL Database.

### Changed
- Both distributions and their runtime API / agent version metadata now report `2.4.0`.

## [2.3.0]: 2026-08-11

### Added
- **Entra ID identity + Azure Key Vault credential store (issue #16).** The proxy can now
  take its **own** outbound Azure identity through Entra ID and use **Azure Key Vault** as a
  central, RBAC-audited credential source — with an optional **write-back** mode that makes
  the vault the authoritative store while the encrypted local store becomes a cache.
  - **Identity modes** (`AUTH_MODE`): `default` (DefaultAzureCredential — managed identity /
    environment / CLI), `managed_identity`, or `service_principal`. A service-principal
    client secret is read only from `AZURE_CLIENT_SECRET` (environment), never a config file.
    Built through one shared `azure-identity` credential and reused by both Key Vault and
    Azure storage mounts.
  - **Read-through source** (`KEYVAULT_URI`): on a local cache miss the encrypted store
    resolves the secret from Key Vault and caches it, so the DB URL, mount credentials, S3
    secret, admin token, and Manager password can live in the vault. A background loop
    re-pulls on `KEYVAULT_REFRESH_SECONDS` (default 300).
  - **Cache-first, never a hard dependency.** A Key Vault / Azure / network outage falls
    back to the local encrypted cache; `KEYVAULT_CACHE_TTL=0` (default) never expires it, so
    an offline or air-gapped deployment runs entirely from the local store. `REQUIRE_KEYVAULT=1`
    opts into failing fast on a cold start with no cache.
  - **Write-back — the vault as authoritative store** (`KEYVAULT_WRITE_BACK`, default off).
    The Manager also persists **every** operator-saved credential into Key Vault: DB URLs,
    mount credentials, the S3 secret / admin token / Manager password, and per-key S3
    **access keys with their full ACL scope** (allowed buckets/prefixes, permissions,
    enabled). Deleting a credential soft-deletes its vault secret. A rebuilt Manager or a
    fresh agent re-populates entirely from the vault. **Fail-soft** — a Key Vault write
    failure never blocks the local save. Needs **Key Vault Secrets Officer** on the Manager
    identity; agents stay read-only **Key Vault Secrets User**.
  - **Secret-name convention:** `db-url` / `db-url-<id>`, `s3-secret-access-key`,
    `admin-token`, `manager-auth-password`, `access-key-<id>`, and mount secrets by id
    (override per deployment).
  - **Observability:** an advisory `key_vault` block in `/readyz`, a Key Vault status card in
    the monitor and admin console, and a config-builder **Entra ID & Key Vault** panel with a
    live **Test** button (`GET /_config/api/keyvault`, `POST /_config/api/keyvault/test`).
  - New optional `keyvault` extra (`azure-keyvault-secrets`, `azure-identity`); the Manager
    launchers install it by default.

### Changed
- Both distributions and their runtime API / agent version metadata now report `2.3.0`.

## [2.2.0]: 2026-08-10

### Added
- **Resilient agent startup — table quarantine + background retry.** A warehouse
  table whose source is unreachable/misconfigured is now **quarantined** (logged,
  excluded from the served set) instead of exiting `EX_CONFIG (78)` and taking every
  healthy table **and every storage-proxy mount** down with it. The agent comes up
  serving all healthy tables + mounts, and a background loop retries quarantined
  tables (default every 60s, `TABLE_RETRY_SECONDS`) so they self-heal when the source
  recovers. Per-table `enabled` flag in `config.tables.json` to disable a table
  manually. New `QUARANTINE_FAILED_TABLES` (default on; set false for the legacy
  fail-fast). Quarantined tables surface in `/readyz` and `GET /_admin/quarantine`.
- **Object Store Tokenizer (issue #12).** Mounts that point at an existing Delta
  Lake or Apache Iceberg table can now serve a **tokenized copy**: the proxy reads
  the source, applies per-column policies in memory (deterministic keyed SHA-256
  token, random UUID token, or drop), and serves a masked Delta table over the S3
  endpoint — so PII never reaches Fabric. Contained entirely in the storage mount
  path; the SQL→Iceberg/Delta engine is untouched. Readers: Delta (delta-rs) on
  `local`, `s3`, and `azure` (ADLS Gen2 / Blob — account key, SAS, connection
  string, or service principal), Iceberg (pyiceberg) on `local`, behind the
  optional `[objectstore]` extra. Tokenized output is materialized to a byte-stable cache keyed by policy
  hash + a one-way key fingerprint (key rotation invalidates it). Config Builder
  gains a per-mount "Tokenize this table" editor with schema inspection and the
  Keep / Deterministic / Random / Remove column controls, and the format +
  tokenizer capability is surfaced in `/readyz` and the monitor summary. An
  `output_format` mount option (`delta` default, `auto` = mirror source, or
  experimental `iceberg`) selects the served table format.
  **Note:** unlike SQL pushdown, this path reads source plaintext into the proxy to
  mask it (object stores have no engine to push down to); it is never written to
  config or served.
- **Targeted S3 access diagnostics for Direct Lake (issue #11).** New
  `S3_ACCESS_LOG` flag (default on) emits structured `s3_object_response` logs for
  ranged reads and `_delta_log` commits (key, kind, status, requested/resolved
  range, bytes served, ETag), `s3_list_delta_log` for the commits a reader
  discovers, and `delta_log_commit_missing` when an expected `NN.json` commit is
  absent. The `upn claim is not present` trace line is a benign app-only-token log,
  not the root cause.
- **Preinstalled source-driver bundle.** The standard Windows and Linux Manager
  bootstraps now install PostgreSQL, Oracle, Redshift, Teradata, and Impala Python
  drivers through the aggregate `[drivers]` extra, plus the object-store tokenizer
  readers (delta-rs + pyiceberg) through `[objectstore]`. Individual extras remain
  available for minimal installs. Redshift, Teradata, and Impala live workload validation is complete.
- **Expanded source platforms (preview): Amazon Redshift, Teradata, and Apache Impala
  (issue #9).** Optional extras `[redshift]`, `[teradata]`, and `[impala]`; SQLAlchemy
  URL and default-port registration (5439 / 1025 / 21050); per-flavor split-query
  dialects (Redshift `%`/`LIMIT`, Teradata `MOD` with `QUALIFY` row caps, Impala
  backtick/`LIMIT`); conservative capability flags (sync-threadpool execution; no
  tokenization, statistics histogram, fast row estimate, or freshness probe); Config
  Builder source entries with capability-driven token controls; and source flavor plus
  execution mode in `/readyz` and the monitor summary. Nested or composite reflected
  types (SUPER, VARIANT, ARRAY/MAP/STRUCT, PERIOD) fail closed instead of stringifying.
  Live source-driver validation is complete for Redshift, Teradata, and Impala.

### Changed
- **Per-table `enabled` control in the Config Builder.** Each table row carries an
  "on" checkbox and each source group an "enabled" checkbox that cascades to its
  tables; disabling persists as `enabled: false` in `config.tables.json`.
- Both distributions and their runtime API / agent version metadata now report
  `2.2.0`.

## [2.1.1]: 2026-08-05

### Added
- **PostgreSQL, Oracle, and Databricks SQL tokenization pushdown.** Deterministic
  SHA-256 and random token policies now render each engine's native SQL:
  `digest`/`gen_random_uuid` (requires `pgcrypto`), `STANDARD_HASH`/`SYS_GUID`, and
  `sha2`/`uuid`. Startup validation and the Config Builder gate token policies on
  the per-dialect capability matrix, so all four supported engines expose Keep,
  Deterministic token, Random token, and Remove.
- `TOKENIZATION_MULTI_DIALECT_UAT.md` runbook with engine-specific setup, token
  format expectations, and negative checks.

### Changed
- Token bind parameters use portable `fsp_token_*` names (letter-prefixed) so
  named binds work across Oracle and the other drivers. Both `fsp_token_*` and the
  legacy `__token_*` names are redacted from logs.
- Runtime API and agent version metadata now report `2.1.1`.

### Fixed
- Databricks range-split queries now use `LIMIT` instead of the unsupported
  SQL Server `TOP` syntax.

## [2.1.0]: 2026-08-05

### Added
- **SQL Server tokenization pushdown.** Column policies can produce deterministic
  SHA-256 tokens with `HASHBYTES`, random UUID tokens with `NEWID`, or omit source
  columns before data leaves SQL Server. Deterministic tokens support equality
  joins, grouping, and distinct operations without exposing plaintext values.
- **Config Builder policy controls.** Reflected columns can be marked Keep,
  Deterministic token, Random token, or Remove. Existing explicit schemas and
  transformations survive Config Builder reloads and apply operations.
- Source/output column aliases, environment-backed token key references,
  normalization modes (`none`, `trim`, `trim_lower`), and per-dialect capability
  reporting.

### Changed
- **Split into two distributions.** The single-node core continues to ship as
  `fabric-shortcut-proxy` (Lite); the scale-out cluster code moves into a new
  `enterprise/` package published as `fabric-shortcut-proxy-enterprise`, pinned to
  the matching core version. The Manager control plane, agent link, retention GC,
  and the external-LB renderer now live in the enterprise package. A Lite-only
  install runs the standalone proxy unchanged; the cluster hooks in `main.py` import
  the enterprise package lazily and report a clear install hint when it is absent.
  - Lite: `pip install fabric-shortcut-proxy`, then `python main.py`.
  - Cluster: install both (`pip install -e . -e ./enterprise` for a dev checkout),
    then `python -m enterprise.manager`.
  - CI splits into a Lite suite (core only, enterprise excluded) and an Enterprise
    suite; a new import-contract test fails if the Lite core imports the enterprise
    surface.
- Both distributions and their runtime API/agent version metadata now report
  `2.1.0`.

### Security
- Token key values are resolved only from `FSP_TOKENIZATION_KEY_*` environment
  variables. Table configuration stores non-secret key references.
- Token bind parameters are redacted from structured logs, and SQLAlchemy hides
  parameter values in exception output.
- Unsupported dialects, missing token keys, transformed split keys, malformed
  policy schemas, and random-token/content-hash refresh conflicts fail closed.

### Validation
- Fabric UAT confirmed deterministic tokens remain stable after rematerialization
  with the same key and retain matching `GROUP BY` values and counts across reads.

## [2.0.0]: 2026-07-30

Major release. Headlined by the **secured storage proxy**: the same S3 front door
can now serve *existing* files/objects as read-only byte passthrough, **alongside**
the database→Iceberg/Delta path, plus multi-connection sources, an encrypted
credential store, and per-key authorization / TLS / audit.

### Added
- **Storage proxy, secured file/object passthrough (Phases 1–4).** An additive
  mount table (`config.mounts.json`): a bucket with a mount streams bytes straight
  from its backend; every other bucket resolves through Iceberg/Delta unchanged.
  - `local` backend, a filesystem path (an OS-mounted **NFS/SMB** share); streamed
    ranged reads and a one-level (non-recursive) folder browse.
  - `s3` backend, native **S3 / MinIO / S3-compatible** buckets with ranged
    streaming and internal list pagination (`pip install '.[s3proxy]'`).
  - `azure` backend, native **Azure Blob / ADLS Gen2** containers, flat blob and
    hierarchical namespace (`pip install '.[azureblob]'`).
  - Config-builder **Storage** tab: per-backend mount editor with a live *Test*
    probe; mounted buckets are advertised in `ListBuckets`.
- **Proxy access keys + per-key authorization.** Scoped SigV4 access keys (allowed
  buckets/prefixes, read-only) stored encrypted; managed via the Storage → *Access
  keys* panel and `/_config/api/access-keys` (create returns the secret once;
  rotate/delete supported).
- **Outbound credential mediation.** Upstream S3/Azure secrets are held encrypted
  and resolved by id, never exposed to clients or written to `config.mounts.json`.
  Broad auth-mode coverage, S3: static, session, assume_role, web_identity,
  profile, sso, instance, process, anonymous; Azure: connection_string,
  account_key, sas, aad_client_secret, managed_identity, default, anonymous.
- **Encrypted Manager credential store** (DPAPI on Windows, Fernet elsewhere) that
  survives restarts and hydrates `DB_URL` / `DB_URL_<ID>`; also holds access keys.
- **Multi-connection / multi-dialect source support** across the config builder and
  monitor.
- **Size-weighted split assignment** (`shard_strategy=weighted`).
- **TLS termination at the proxy** (`TLS_CERT_FILE` / `TLS_KEY_FILE`) for both the
  agent and the Manager control plane.
- **Audit logging** of every mounted-object access (`ENABLE_AUDIT_LOG`,
  `AUDIT_LOG_FILE`, recent events at `GET /_config/api/audit`).
- Optional dependency extras: `s3proxy`, `azureblob`, `credentials`, `postgres`,
  `oracle`.

### Changed
- SigV4 verification resolves the signing secret by the presented **access-key id**
  (multi-key) and returns the authenticated identity; the legacy single key remains
  an implicit wildcard until the first scoped key is created.
- Mounted buckets require authentication even when `REQUIRE_SIGV4=0`
  (`ENFORCE_MOUNT_AUTH`, default on), a secured mount is never served anonymously.
- `ListObjectsV2` XML now carries pagination fields (`MaxKeys`, `IsTruncated`,
  `NextContinuationToken`).
- Documentation: README architecture diagram + Storage Proxy section; `SECURITY.md`
  storage-proxy security; `CONFIGURATION.md` §14; Roadmap and FAQ updated.
- Version bumped `1.0.0 → 2.0.0`.

### Fixed
- Folder browse uses a one-level directory listing instead of a recursive walk
  (no hang on large shares).
- Mounted buckets are advertised in `ListBuckets` (`GET /`).
- Monitor dashboard is available in the authenticated Manager console
  (`/_manager` operator console tab), with selectable cluster health history.
- Supervisor no longer crash-loops on permanent source-DB configuration errors;
  startup emits a clear, redacted message when a source DB connection fails.
- Config builder: prevents duplicate table names and undefined-connection
  references, and mirrors the live config authoritatively.

### Security
- Removed a hardcoded DB credential; added credential scrubbing and startup
  validation.
- The config builder never writes DB passwords into config files (inline DB
  credentials are opt-in); a masked (`***`) DB URL is never stored, hydrated, or
  emitted as a credential.
- Every mount key is normalized and rejects `..` traversal, confined to the mount
  `prefix` subtree, across all backends (OWASP A01/A03).

## [1.0.0]: 2026-07-25

Initial release, the proof-of-concept virtualization gateway that makes relational
data appear as shortcut-readable table objects in Microsoft Fabric.

### Added
- S3-compatible endpoint (`GET` / `HEAD` / `ListObjectsV2`, ranged reads, S3-style
  XML and error bodies) fronting a relational source.
- Iceberg v2 metadata virtualization (`metadata.json` + Avro manifest list/files +
  `version-hint.text`).
- Native **Delta** output mode (`TABLE_FORMAT=delta`, `_delta_log`) with no
  Iceberg→Delta conversion on Fabric's side.
- SQL pushdown → on-demand **Parquet** generation (PyArrow) via a parameterized,
  bounded, retrying async executor.
- Deterministic **content-addressed splits**, snapshot/version lineage, and split
  pinning (byte-identical snapshot data files).
- Data freshness / **auto-refresh** (dialect probe / content-hash / TTL strategies).
- In-memory LRU plus optional on-disk Parquet cache for warm restarts.
- Multi-table serving under canonical paths
  (`db/<server>/<database>/<schema>/<object>`) with legacy aliases.
- Auto-schema **reflection** from the source; SQL dialects: SQLite, PostgreSQL,
  SQL Server.
- Opt-in AWS **SigV4** verification (`REQUIRE_SIGV4`).
- Observability: `/healthz`, `/readyz`, `/metrics`, `/_admin`, a Fabric
  request-timeline trace, and per-request query-lag stats.
- Config-builder UI (`/_config`) and a read-only monitoring dashboard (`/_monitor`).
- **Manager/Agent** control plane: table/snapshot registry, agent supervisor,
  gateway round-robin, heartbeats, leader-lease HA, rolling restart, retention GC.

[2.5.3]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.5.2...2.5.3
[2.5.2]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.5.1...2.5.2
[2.5.1]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.5.0...2.5.1
[2.5.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.4.0...2.5.0
[2.4.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.3.0...2.4.0
[2.3.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.2.0...2.3.0
[2.2.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.1.1...2.2.0
[2.1.1]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.1.0...2.1.1
[2.1.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.0.0...2.1.0
[2.0.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/1.0.0...2.0.0
[1.0.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/releases/tag/1.0.0
