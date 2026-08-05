# Changelog

All notable changes to the Fabric Shortcut Proxy are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **PostgreSQL, Oracle, and Databricks SQL tokenization pushdown.** Deterministic
  SHA-256 and random token policies now use each engine's native SQL functions.
  Config Builder exposes token policies for all four supported tokenizing
  dialects.

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
- Monitor dashboard is served on the Manager (`/_monitor` operator console tab).
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

[2.1.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/2.0.0...main
[2.0.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/compare/1.0.0...2.0.0
[1.0.0]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/releases/tag/1.0.0
