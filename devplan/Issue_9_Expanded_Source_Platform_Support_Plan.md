# Issue 9: Expanded Source Platform Support Plan

Status: Planned

Issue: [#9 Add Expanded source platform support](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/9)

Branch: `feature/issue-9-expanded-source-platforms`

## 1. Goal

Add Python runtime and Config Builder support for these relational SQL sources:

- Amazon Redshift
- Teradata
- Cloudera Apache Impala

A source is supported only when the proxy can connect, discover tables, reflect a
usable schema, generate bounded split queries, materialize rows to Parquet, and
report unsupported optional features through the capability matrix.

## 2. Scope boundaries

The first delivery covers the Python data path. It does not promise native C++
ingestion for the three sources. The C++ agent has separate PostgreSQL, SQLite,
and generic ODBC source implementations; each new platform needs its own driver
and result-type validation before it can be added there.

The first delivery also does not claim these features until they pass a live
platform test:

- deterministic or random tokenization pushdown
- catalog-based freshness tokens
- optimizer-histogram split planning
- fast catalog row estimates
- async-native query execution

The existing manual, TTL, content-hash, range, NTILE, and exact-count fallbacks
remain available where the capability matrix permits them.

## 3. Current extension points

| Concern | Owning code | Required change |
| --- | --- | --- |
| Driver extras | `pyproject.toml` | Add one optional extra per source and an aggregate `sources` extra if packaging supports self-references cleanly. |
| Structured URL construction | `db/reflect.py` | Add allowlisted driver names, defaults, source-specific query fields, and missing-driver hints. |
| Runtime mode | `db/executor.py` | Keep all three drivers on the existing sync-threadpool path initially; prove engine creation, named binds, pooling, cancellation behavior, and disposal. |
| Flavor policy | `db/capabilities.py` | Add normalized flavors with conservative capability flags. Avoid treating Redshift as PostgreSQL based on substring matching. |
| Query SQL | `planner/dialects.py` | Add explicit Redshift, Teradata, and Impala adapters for quoting, modulo, row limits, ranges, and row-number fallback. |
| Reflection | `db/reflect.py`, `db/executor.py` | Verify Inspector support and add narrowly scoped metadata fallbacks only where the selected plugin lacks an API. |
| Type mapping | `db/executor.py` | Test vendor-specific reflected types and add mappings only for values that do not normalize to existing SQLAlchemy types. |
| Config Builder | `configbuilder/index.html`, `configbuilder/router.py` | Add source choices, ports, authentication fields, capability display, and actionable driver errors. |
| Freshness | `iceberg/freshness.py` | Return no dialect token initially; add source-specific probes only with documented catalog permissions and live tests. |
| Docs and examples | `connection_config.py`, `config.connection.example.json`, `docs/CONFIGURATION.md`, `FAQ.md`, installation docs | Document URL forms, extras, TLS/authentication, execution mode, and known limits. |

## 4. Driver decision gate

Complete this gate before changing planner SQL. Each candidate must install on
Python 3.11 and current SQLAlchemy 2.x, create an engine, execute a bound scalar,
and expose enough Inspector metadata for the Config Builder.

### 4.1 Amazon Redshift

Proposed baseline:

- SQLAlchemy dialect: `sqlalchemy-redshift>=1.0`
- DBAPI: `redshift-connector`
- URL: `redshift+redshift_connector://user:password@host:5439/database`
- execution: synchronous threadpool fallback

`sqlalchemy-redshift` 1.0 added current SQLAlchemy 2.0 and Python 3.10+ support.
The dedicated dialect contains Redshift reflection behavior that the generic
PostgreSQL dialect does not. The spike must compare `redshift_connector` with
`psycopg2-binary` on engine creation, reflection, bound parameters, and result
types. Select one DBAPI and document only that URL in the Config Builder.

Do not route `redshift://` through `PostgresDialect` implicitly. Redshift can
reuse PostgreSQL query-building methods where verified, but it needs its own
flavor and capability record because PostgreSQL catalog, pgcrypto, histogram,
and freshness assumptions do not apply.

### 4.2 Teradata

Proposed baseline:

- SQLAlchemy dialect and DBAPI integration: `teradatasqlalchemy>=20.0`
- URL: `teradatasql://user:password@host/`
- default database: `database` connection query parameter
- default port: `dbs_port=1025` connection query parameter
- execution: synchronous threadpool fallback

The spike must verify URL construction because the Teradata driver documents
`database` and `dbs_port` as connection parameters rather than standard URL path
and port fields. It must also test `tmode=ANSI`, TLS/data encryption settings,
LDAP, and basic password authentication. The first release should expose only
settings that can be represented without placing secrets in saved JSON.

### 4.3 Apache Impala

Proposed baseline:

- SQLAlchemy connector: `impyla>=0.24`
- URL: `impala://host:21050/database`
- execution: synchronous threadpool fallback

The spike must cover unsecured/NOSASL or PLAIN development access plus the
production modes the project will claim: LDAP, TLS certificate verification,
and Kerberos if a test environment is available. Kerberos remains an optional
extra because it requires OS Kerberos libraries. The Config Builder must not
default TLS to an unverified certificate mode.

### 4.4 Required spike assertions

For each selected package and URL:

1. `create_engine()` and `SELECT :value` work through SQLAlchemy `text()`.
2. `inspect()` lists schemas, tables, views, columns, and primary keys or returns a documented unsupported result.
3. `SELECT`, range predicates, `ROW_NUMBER()`, modulo partitioning, and a bound row limit execute.
4. Integer, decimal, string, binary, boolean, date, timestamp, and null values reach `sqlalchemy_type_to_iceberg()` and PyArrow correctly.
5. Pool pre-ping, disposal, timeout handling, and retry classification do not leak connections.
6. Driver exceptions are scrubbed before they reach Config Builder responses or logs.

Record the tested server version, driver version, authentication mode, and URL
shape in the integration test module and operator documentation.

## 5. Implementation phases

### Phase 1: Connection and capability registration

1. Add `redshift`, `teradata`, and `impala` optional dependencies in `pyproject.toml`.
2. Extend `_DRIVERS` and `_DEFAULT_PORTS` in `db/reflect.py`. Use a source-specific URL builder hook where query parameters cannot be represented by the generic host/port/database mapping.
3. Add exact scheme handling in `flavor_from_db_url()`. Match Redshift before PostgreSQL and avoid broad substring aliases.
4. Add conservative `FlavorCapabilities` entries. Start with sync execution, no tokenization, no statistics histogram, no fast estimate, and no freshness claim.
5. Extend missing-plugin error cleanup with the matching installation extra.

Exit check: URL and capability unit tests pass without importing optional drivers.

### Phase 2: Query dialects

Add `RedshiftDialect`, `TeradataDialect`, and `ImpalaDialect` in
`planner/dialects.py`. Keep selection explicit in `get_dialect()`.

For every dialect, test:

- escaped single and qualified identifiers
- integer casts
- modulo split predicate
- contiguous range predicate
- deterministic `ROW_NUMBER()` fallback for non-integer keys
- bound maximum-row syntax
- aliases in outer row-number projections

Redshift may subclass `PostgresDialect` for core selection syntax after the live
spike passes. It must override transform behavior so PostgreSQL's `pgcrypto`
expressions are not inherited accidentally.

Teradata requires live confirmation of `TOP` versus `FETCH FIRST` for bound row
limits and `MOD()` for modulo splits. Impala requires live confirmation of
backtick quoting, `%` or `pmod`, and bound `LIMIT`. SQLAlchemy should translate
named binds to each DBAPI's parameter style; do not add manual string
substitution.

Exit check: generated SQL unit tests pass, and every SQL form executes against
the corresponding integration environment.

### Phase 3: Reflection and data types

1. Exercise the selected SQLAlchemy Inspector before adding custom SQL.
2. Add platform-specific table/view discovery only for Inspector operations that fail. Keep identifiers quoted through the selected dialect.
3. Treat absent primary-key metadata as a capability outcome. Require an explicit `key_column` when stable key discovery is unavailable.
4. Add vendor type mappings only after a reflected type fails the existing generic mapping.
5. Test tables, views, mixed-case names, reserved words, non-default schemas/databases, nullable columns, and unsupported complex types.
6. Define behavior for Redshift `SUPER`, Teradata ARRAY/JSON/XML/period types, and Impala complex types. The default should reject an unsafe materialization with a clear message rather than silently stringify nested values.

Exit check: Config Builder discovery and one split-to-Parquet materialization pass
for a representative table on each source.

### Phase 4: Config Builder and configuration files

1. Add the three dialects and default ports to `configbuilder/index.html`.
2. Replace the current fixed Databricks-only advanced-field collection with per-source connection field definitions.
3. Add Teradata database, port, transaction/authentication settings and Impala auth/TLS settings supported by the driver gate.
4. Preserve the existing rule that saved configuration contains masked or credential-free URLs; secrets remain in environment variables or the credential store.
5. Use capability flags, not a hard-coded flavor list, to enable tokenization controls.
6. Add source-specific missing-package, TLS, Kerberos, and authentication hints in `configbuilder/router.py`.

Exit check: browser-built URLs match `build_url()`, saved multi-source
configuration round-trips, and unsupported controls stay disabled.

### Phase 5: Runtime fallbacks and observability

1. Verify the existing sync query path in `db/executor.py` for all query APIs, including streaming callers that currently fall back to buffered reads.
2. Keep `supports_streaming_query=False` until a bounded streaming implementation is measured with the selected driver.
3. Keep `probe_change_token()` returning `None` for the new flavors. Document the resulting behavior for `auto`, `dialect_probe`, TTL, manual refresh, and full-pull settings.
4. Use exact row counts only where operator settings allow them. Do not label a catalog estimate as supported until a permission-safe query is tested.
5. Include flavor and execution mode in existing health, monitor, and connection-test output.

Exit check: startup validation, eager materialization, lazy regeneration, refresh,
retry, and shutdown pass for one table per source.

### Phase 6: Documentation and rollout

Update:

- `connection_config.py` URL examples
- `config.connection.example.json`
- `docs/CONFIGURATION.md`
- `docs/installation/Linux_Deployment.md`
- `docs/installation/Windows_Deployment.md`
- `docs/manual/README.md`
- `FAQ.md`
- `docs/CHANGELOG.md` when implementation ships

Document package installation, server/network prerequisites, authentication,
TLS verification, supported types, reflection permissions, fallback behavior,
and tested server/driver versions. Correct the issue's `Terrada` spelling to
`Teradata` in all repository content.

Roll out in this order: Redshift, Teradata, then Impala. Each source can merge
independently after its driver gate and integration suite pass.

## 6. Test plan

### Unit and contract tests

- `tests/test_config_builder.py`: URL defaults, encoding, required fields, source-specific queries, aliases, and missing-driver messages.
- `tests/test_capabilities.py`: exact scheme normalization and conservative capability flags.
- `tests/test_dialects.py`: quoting plus modulo, range, row-number, and maximum-row SQL for all three dialects.
- `tests/test_executor_sync_fallback.py`: engine dispatch, bound parameters, retries, timeouts, result mappings, and disposal using mocked or contract DBAPIs where needed.
- Type-mapping tests for every vendor type explicitly accepted or rejected.
- Config Builder tests for per-source field visibility, credential handling, capability-controlled tokenization, and multi-connection output.

### Environment-gated integration tests

Create one integration module per platform. Skip unless its environment
variables are present. Each module must test:

1. connection and server version
2. schema, table, view, column, and key reflection
3. integer-key modulo and range splits
4. non-integer-key row-number split
5. row-limit enforcement
6. representative type conversion and Parquet generation
7. startup schema validation and source ping
8. credential redaction on a forced authentication failure

Integration tests must use read-only fixtures or a dedicated disposable schema.
Do not point destructive setup at a production catalog.

### Regression checks

- Run the focused config, capability, dialect, executor, reflection, planner, and Parquet tests.
- Run the full Python suite and compare failures with the branch baseline.
- Run C++ Tier 1 tests unchanged. Add C++ dialect names only if native execution support is included in a later scope.
- Open the Config Builder on desktop and mobile widths and verify connection authoring, field visibility, long errors, and table discovery.

## 7. Acceptance criteria

For each new platform:

- The documented optional extra installs on Windows and Linux with Python 3.11.
- The Config Builder creates a valid, credential-safe URL and lists accessible tables and views.
- Auto-schema reflects a supported table and either identifies a key or asks for one explicitly.
- Eager and lazy materialization produce Parquet that passes the existing reference-reader checks.
- Modulo, range, and row-number splits are bounded, deterministic, and do not overlap.
- Unsupported tokenization, freshness, statistics, and streaming features are disabled or fall back according to the capability matrix.
- Logs, errors, generated config, and monitor output contain no passwords, tokens, Kerberos material, or TLS private data.
- Existing SQLite, PostgreSQL, SQL Server, Oracle, and Databricks tests retain their baseline behavior.
- Documentation names the tested driver/server versions and all known limitations.

Issue #9 is complete when all three platform rows meet these criteria. A
platform may ship earlier as an independently reviewed pull request.

## 8. Open decisions

Resolve these during the driver gate and record the answers in this document:

| Decision | Evidence required |
| --- | --- |
| Redshift DBAPI: `redshift_connector` or `psycopg2-binary` | SQLAlchemy 2 reflection, bind handling, type coverage, packaging, and authentication tests. |
| Teradata URL representation | Successful engine creation with database, `dbs_port`, `tmode`, TLS, and selected authentication modes. |
| Impala production authentication scope | Available test environments for LDAP, TLS verification, and Kerberos plus cross-platform install results. |
| Per-platform modulo and row-limit syntax | Executed split queries with bound values on supported server versions. |
| Complex vendor type policy | PyArrow round-trip evidence or an explicit rejection contract for each type. |
| Tokenization pushdown | Verified SQL functions with stable output and null/normalization tests; otherwise remain unsupported. |
| Native C++ support | Separate compatibility results for libpq or ODBC drivers, parameter binding, and Arrow conversion. |

## 9. Suggested pull request sequence

1. Driver spikes and recorded decisions, with no user-facing support claim.
2. Redshift connection, dialect, reflection, integration tests, UI, and docs.
3. Teradata connection, dialect, reflection, integration tests, UI, and docs.
4. Impala connection, dialect, reflection, integration tests, UI, and docs.
5. Optional follow-up for tokenization, freshness probes, statistics, streaming, or native C++ support backed by separate tests.
