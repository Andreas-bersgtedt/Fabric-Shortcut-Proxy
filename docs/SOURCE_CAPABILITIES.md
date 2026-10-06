# SQL source capability matrix and live gates

The runtime source-of-truth is `db/capabilities.py`; this table is parity-tested
against `capability_matrix()`. A `yes` means the implementation is available,
not that every server version or object has suitable metadata. `beta` means the
conservative capability contract and offline release gate pass, while live
source certification remains pending. `preview` requires source-specific
evidence before wider use. Oracle retains its existing `supported` status.

| Source | Status | Views | PK reflection | Explicit key required | Range/date bounds | Modulo | NTILE | Stats histogram | Fast row estimate | Freshness probe | Native deterministic | Native random | Bounded-memory streaming |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| oracle | supported | yes | yes | no | yes | yes | yes | no | yes | no | yes | yes | no |
| databricks | beta | yes | no | yes | yes | yes | yes | no | no | no | yes | yes | no |
| redshift | beta | yes | yes | no | yes | yes | yes | no | no | no | no | no | no |
| teradata | beta | yes | yes | no | yes | yes | yes | no | no | no | no | no | no |
| impala | supported | yes | no | yes | yes | yes | yes | no | no | no | no | no | no |

## Beta release gate

Databricks, Redshift and Teradata are ready for a beta release when all of the
following checks pass:

- the focused issue #94 regression suite passes;
- documentation matches the runtime capability matrix;
- all three report no bounded-memory streaming, freshness probe, statistics
  histogram or source snapshot provider;
- Databricks requires an explicit split key;
- Redshift and Teradata do not advertise native tokenization and require an
  explicit Arrow fallback;
- the release notes identify the missing live-source, reconnect, timeout,
  mutation, scale and publication evidence.

`tests/test_capabilities.py::test_issue_94_beta_release_gate_is_conservative`
locks the status and safety constraints. Passing this gate permits a beta
release. It does not satisfy the live evidence required to promote a dialect
from `beta` to `supported`.

## Important limits and fallbacks

- **Streaming:** these five dialects use synchronous SQLAlchemy drivers. The
  `stream_split_query` compatibility API fetches the complete result before
  yielding batches. `STREAMING_PARQUET=1` can stream Parquet encoding after that
  fetch, but does **not** bound source-result memory. Use smaller split ranges,
  lower row targets, or a row cap; proxy memory can still grow with the full
  split result.
- **Query timeouts:** the proxy applies `QUERY_TIMEOUT_SECONDS` while awaiting
  each query. With synchronous drivers, cancelling that wait cannot interrupt
  the worker thread or guarantee cancellation at the database; the query can
  continue using a source connection until the driver returns. Use native
  database statement/connect timeouts where available, and keep splits bounded.
- **Databricks and Impala split keys:** PK reflection is not relied on. Choose
  `key_column` explicitly from the reflected source columns. The Config Builder
  leaves this choice blank and blocks apply until the chosen column is present.
  Use a stable, sortable key appropriate for the configured strategy; the live
  gate uses a non-null integer key.
- **Oracle quoted identifiers:** lowercase names are treated as conventional
  unquoted Oracle identifiers and normalized to uppercase. Preserve a
  case-sensitive quoted schema, table or column by including its double quotes
  in the configured identifier, for example `"sales"."orders"`.
- **Fast estimates:** only Oracle advertises a catalog estimate among these
  sources. Oracle statistics can be absent or stale, so the estimate may be
  `null` or approximate. For sources without a fast estimate, the fallback is
  exact `COUNT(*)`; this can scan the object and load the source. NTILE
  quantiles may also scan and sort key values. Equal-span ranges avoid the
  quantile scan but can be badly imbalanced under key skew.
- **Freshness:** no dialect in this set has an implemented, reliable catalog
  change-token probe. Configure manual/TTL refresh, or explicitly enable the
  full-content-read refresh path when its source cost is acceptable.
- **Arrow tokenization:** only Oracle and Databricks currently advertise native
  deterministic and random tokenization. Other dialects reject these transforms
  unless `TOKENIZATION_FALLBACK=arrow` is explicitly selected. Arrow fallback
  routes plaintext source values through proxy memory and network; it emits the
  structured `arrow_tokenization_fallback` warning, a monitor counter, and
  per-table fallback details. Neither logs nor metrics contain values or keys.
- **Snapshots and histograms:** these five sources have no source-snapshot
  provider and no stats-histogram reader. Independent split reads are best
  effort and may observe different commits. Count-balanced planning uses NTILE
  where enabled; a failed/empty quantile query falls back to equal-span ranges.

## Opt-in live integration gate

The gate is in `tests/test_integration_source_capabilities.py`. It requires
read-only credentials and a small, dedicated fixture table per dialect. The
table must have a unique, non-null integer key, a non-null date/timestamp column, and
few enough rows to fit within the configured per-split row cap. A view must
also be visible to the connection. The gate performs real discovery, column and PK reflection, row-estimate
behavior, key/date bounds, NTILE planning, range and modulo reads, exact
coverage counts, and a pool-dispose/reconnect check. Separate opt-in tests
exercise source reads through content-hash materialization and refresh. The
Arrow gate exercises source reads through Parquet generation and checks the
fallback log and monitor counter on Redshift, Teradata, and Impala. Exact counts
and NTILE can be expensive; do not point these gates at a large production table.

Set the explicit opt-in and source connection values before running:

```powershell
$env:FSP_RUN_SOURCE_CAPABILITY_GATES = "1"
# Set the variables for each dialect you intend to exercise:
$env:ORACLE_HOST = "..."
$env:ORACLE_PORT = "1521"
$env:ORACLE_DATABASE = "..."
$env:ORACLE_USERNAME = "..."
$env:ORACLE_PASSWORD = "..."
$env:INTEGRATION_ORACLE_TABLE = "FSP_GATE.TEST_ROWS"
$env:INTEGRATION_ORACLE_VIEW = "FSP_GATE.TEST_ROWS_VIEW"
$env:INTEGRATION_ORACLE_INTEGER_KEY = "ID"
$env:INTEGRATION_ORACLE_DATE_COLUMN = "EVENT_DATE"
$env:INTEGRATION_ORACLE_PK_COLUMN = "ID"
# Repeat with REDSHIFT and TERADATA prefixes. Impala quickstart uses an
# unauthenticated connection; set IMPALA_HOST/PORT/DATABASE only. For Databricks use
# DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH, optional
# DATABRICKS_CATALOG / DATABRICKS_SCHEMA, and INTEGRATION_DATABRICKS_* values.
.\.venv\Scripts\python.exe -m pytest tests/test_integration_source_capabilities.py -q
```

### Local Oracle Free / Impala fixture

The repository fixture initializer creates a small table and view, with four
unique integer IDs, ascending timestamps, a NULL email, and a Unicode email. It
drops/recreates only the named test objects. Run it only against isolated local
instances. Drivers are in the optional `oracle` and `impala` extras.

For Oracle Free, use the PDB service name `FREEPDB1` and a dedicated local test
account with create-table/view privileges and quota on a non-system default
tablespace. SQLAlchemy's Oracle table discovery excludes the `SYSTEM` and
`SYSAUX` tablespaces, even for user-owned tables. Oracle Free Lite can initially
have only system, undo, and temporary tablespaces. An administrator must create
an application tablespace, set it as the fixture account's default, and grant
quota before seeding. The verified local fixture used `FSP94_DATA` with a 100 MB
quota. Move or recreate existing fixture tables if they were seeded in `SYSTEM`;
changing the account's default alone does not move them. Do not disable the
driver's tablespace exclusion to make a fixture pass.

```powershell
$env:ORACLE_HOST = "127.0.0.1"
$env:ORACLE_PORT = "29421"
$env:ORACLE_DATABASE = "FREEPDB1"
$env:ORACLE_USERNAME = "fsp94"
$env:ORACLE_PASSWORD = "<the dedicated test user's password>"
.\.venv\Scripts\python.exe tests\fixtures\source_capabilities\seed_local_fixture.py oracle
```

For the Apache Impala quickstart, publish the coordinator's Beeswax port as
`29450` on localhost; its default NOSASL connection needs no username/password:

```powershell
$env:IMPALA_HOST = "127.0.0.1"
$env:IMPALA_PORT = "29450"
$env:IMPALA_DATABASE = "default"
.\.venv\Scripts\python.exe tests\fixtures\source_capabilities\seed_local_fixture.py impala
```

The Impala seeder creates its objects in `issue94_gate`; use
`IMPALA_DATABASE=issue94_gate` for the subsequent gates.

Each seed command prints the matching `INTEGRATION_<SOURCE>_*` assignments and
the gate opt-in. Copy those into the same PowerShell session, then run only the
local gates:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_integration_source_capabilities.py -q -k "oracle or impala"
```

The source materialization/refresh test uses the same variables and is
selected by `-k source_materialization_and_refresh_live_gate`. Arrow fallback
materialization is gated separately for Redshift, Teradata, and Impala; set the
`INTEGRATION_<SOURCE>_TOKEN_COLUMN`, `NULL_ROW_KEY`, and `UNICODE_ROW_KEY`
values printed by the seeder, then select
`-k arrow_fallback_live_materialization_gate`.

Impala support-graduation work is tracked separately from the small fixture
gate in issue #109. Set `FSP_RUN_IMPALA_SUPPORT_GATES=1` and provide an explicit
`IMPALA_USERNAME`, three comma-separated `IMPALA_COORDINATORS`, and the
environment-specific table names before running:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_impala_support_gates.py -q
```

The support gate covers the richer type matrix, one-million-row skew fixture,
and every configured coordinator. The timeout check uses Impala's
`EXEC_TIME_LIMIT_S` execution limit; `QUERY_TIMEOUT_S` applies to idle queries
and does not bound active execution. Timeout and source-mutation checks require
the additional `FSP_RUN_IMPALA_TIMEOUT_GATE=1` and
`FSP_RUN_IMPALA_MUTATION_GATE=1` flags. Coordinator restart testing requires
`FSP_RUN_IMPALA_RESTART_GATE=1` plus Cloudera Manager control URL, cluster,
coordinator, username, and password variables. The test discovers the role name
at runtime and restores the role in cleanup. The scoped CDP 7.1.7 / Impala 3.4
support claim requires dedicated LDAP authentication, TLS hostname validation,
network and coordinator recovery, materialization and S3 verification, and
three consecutive clean runs.

Set `FSP_RUN_IMPALA_MATERIALIZATION_GATE=1` to materialize the documented
one-million-row skew fixture into four Parquet splits. The gate verifies exact
row coverage, Parquet metadata, unchanged-refresh deduplication, elapsed time,
peak RSS, ListObjectsV2, HEAD, full GET, range GET, ETag consistency, and
SHA-256 consistency. Optional byte and time limits are supplied through
`IMPALA_MAX_MATERIALIZATION_RSS_BYTES` and
`IMPALA_MAX_MATERIALIZATION_SECONDS`. The gate sets `split_target_rows` to one
million so the effective per-split cap can contain the prepared 800,000-row hot
partition. Production tables with skew must set the target above the largest
expected split; the global query cap otherwise remains an intentional safety
limit.

The Azure network-fault harness is
`tests/fixtures/source_capabilities/run_impala_network_fault_gate.py`. It
requires all resource names, prefixes, pod details, priority, and endpoint
values through environment variables. It creates a uniquely named temporary
deny rule and removes it in `finally`, then requires the HS2 connection to
recover. Do not store environment values in the script or test output.

To include Oracle native null/Unicode tokenization, also set
`INTEGRATION_TOKENIZATION_KEY` to an ephemeral test key and use the
`INTEGRATION_ORACLE_TOKEN_COLUMN`, `INTEGRATION_ORACLE_NULL_ROW_KEY`, and
`INTEGRATION_ORACLE_UNICODE_ROW_KEY` values printed by the seed command:

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_integration_source_capabilities.py -q -k "native_tokenization and oracle"
```

For Databricks and Impala, no `*_PK_COLUMN` is needed; the integration check
verifies that reflection does not infer one and that the explicitly selected
integer key is reflected. For Oracle, Redshift, and Teradata, set
`INTEGRATION_<SOURCE>_PK_COLUMN` to a reflected PK column.

The separate Oracle/Databricks native-tokenization gate requires the same source
connection variables plus these fixture values:

```powershell
$env:INTEGRATION_TOKENIZATION_KEY = "<ephemeral test key>"
$env:INTEGRATION_ORACLE_TOKEN_COLUMN = "EMAIL"
$env:INTEGRATION_ORACLE_NULL_ROW_KEY = "1"
$env:INTEGRATION_ORACLE_UNICODE_ROW_KEY = "2"
```

Repeat with the `DATABRICKS` prefix. The specified null row must have a null
token column; the Unicode row must contain non-ASCII text. The test verifies
null preservation, Unicode input, deterministic repeatability, and output
different from the source string. The key is supplied only through the test
process environment and must not be committed.

The older
`tests/test_integration_oracle_databricks.py` remains a minimal connection
smoke test. It does not satisfy the capability gate and does not constitute
graduation evidence.

## Local evidence and pending acceptance

On 2026-10-02, the parent-run isolated container gates produced:

| Source/version | Result | Coverage | Persisted JUnit report |
| --- | --- | --- | --- |
| Official Oracle Free Lite 26ai, `FREEPDB1`, fixture in `FSP94_DATA` | 3 passed | Discovery/reflection/planner reads and pool reopening; native deterministic/random tokenization with NULL and Unicode; content-hash materialization/refresh deduplication | `issue94-oracle-live.xml` |
| Official Apache Impala 4.5.0, unauthenticated HS2 | 4 passed | Gate environment contract; discovery/reflection/planner reads and pool reopening; content-hash materialization/refresh deduplication; random Arrow fallback materialization, log, and counter | `issue94-impala-live.xml` |

The reports are session artifacts, not committed certification fixtures. Both
sources used four-row table/view fixtures with integer keys, timestamps, one
NULL email, and one Unicode email. These are functional checks, not scale,
memory, throughput, or production-certification benchmarks.

Promotion from `beta` to `supported`, and closure of issue #94, remain pending:

- Actual Databricks, Redshift, and Teradata endpoints and passing per-source
  gates. Other databases or Spark substitutes do not certify these sources.
- Fault-induced reconnect and real driver/database timeout tests. Disposing
  a healthy pool and reopening it verifies pool recreation, not recovery from
  network failure, server restart, or database-side cancellation.
- Representative nullable split keys, decimals, binary values, skewed keys,
  and larger fixtures. The current seed does not cover these cases.
- Refresh after actual source changes and external serving/publication checks.
  The current refresh gate verifies repeated unchanged reads and in-process
  snapshot/Parquet state, not an external catalog or object-store publication.
- Native/Arrow tokenization equivalence across supported normalization and
  encoding cases; current native tests verify NULL, Unicode, repeatability,
  and random variation, not cross-engine byte equivalence.

No dialect in this set claims bounded-memory streaming. A future streaming
implementation requires measured large-result memory evidence before that
capability or a graduation decision changes.
