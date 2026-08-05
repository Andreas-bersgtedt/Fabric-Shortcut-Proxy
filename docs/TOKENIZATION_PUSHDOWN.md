# Pushdown tokenization investigation

Status: implemented for SQL Server through the Python agent and Config Builder; UAT pending

The implemented scope includes deterministic SHA-256 tokens, random UUID tokens,
column omission, source/output aliases, key resolution from environment variables,
startup validation, parameter redaction, capability reporting, and Config Builder
policy controls. PostgreSQL, Oracle, Databricks, SQLite cryptographic transforms,
and the standalone C++ native publisher remain future work.

## Decision

Add column-level projection policies to the table schema and render them through
the existing SQL dialect adapters. Support three initial policies:

1. `drop`: do not advertise or select the source column.
2. `deterministic_hash`: produce a stable, one-way token suitable for equality,
   `GROUP BY`, `DISTINCT`, and equality joins.
3. `random_token`: produce an unrelated token for every extraction. Equality
   relationships are intentionally lost.

Do not accept free-form SQL expressions in configuration. A policy allowlist
keeps identifiers quoted, secrets parameterized, output types predictable, and
dialect behavior testable.

This feature belongs in the projection path. Today,
`planner.split_planner.build_split_query()` renders every `ColumnDef` as a quoted
identifier. Split predicates and ordering operate on the source key separately,
so projection policies do not need to change modulo, range, or row-number split
planning.

## Current behavior

The current implementation already supports column removal when an explicit
schema omits the column. Auto-derived schemas include every reflected source
column, so removal requires an explicit schema today.

Two contracts need to change before transformed columns work:

- `ColumnDef.name` currently serves as both the source identifier and Iceberg
  output name. Add `source` so a transformed source can be aliased back to the
  stable output name.
- `db.executor.validate_source_schema()` validates output names against source
  metadata. It must validate `source or name` instead.

The Parquet path already reads result dictionaries by output name and coerces
values to the declared Iceberg type. A projection such as
`... AS [customer_token]` therefore fits the downstream contract without a
post-query transformation.

## Proposed configuration

```json
{
  "name": "customers_safe",
  "source_table": "dbo.customers",
  "key_column": "customer_id",
  "schema": [
    {
      "field_id": 1,
      "name": "customer_id",
      "type": "long",
      "nullable": false
    },
    {
      "field_id": 2,
      "name": "email_token",
      "source": "email",
      "type": "string",
      "transform": {
        "kind": "deterministic_hash",
        "key_ref": "customer-pii-v1",
        "domain": "customer-email",
        "normalization": "trim_lower"
      }
    },
    {
      "field_id": 3,
      "name": "support_token",
      "source": "support_note",
      "type": "string",
      "transform": {
        "kind": "random_token"
      }
    }
  ]
}
```

Column removal should remain omission from `schema`. A `drop` marker is useful
only in the config builder when starting from reflected columns; the persisted
runtime schema should omit dropped fields. This avoids advertising a column in
Iceberg metadata that can never contain a value.

`key_ref` identifies a secret in the credential store or environment. The JSON
must never contain the secret itself. Version the reference, for example
`customer-pii-v1`, because changing it changes every token and therefore changes
join keys and snapshot contents.

## SQL Server rendering

Recommended SQL Server output is a 64-character uppercase hexadecimal SHA-256
value. A string is easier for Fabric clients to inspect than driver-specific
`varbinary`, and it has a fixed Iceberg `string` contract.

```sql
CASE
  WHEN [email] IS NULL THEN NULL
  ELSE CONVERT(varchar(64), HASHBYTES(
    'SHA2_256',
    CONCAT(
      :token_key,
      N'|customer-email|',
      LOWER(LTRIM(RTRIM(CONVERT(nvarchar(max), [email]))))
    )
  ), 2)
END AS [email_token]
```

`HASHBYTES` returns 32 bytes for `SHA2_256`. SQL Server 2016 and later deprecates
the older algorithms, so only `SHA2_256` should be emitted. SQL Server 2014 and
earlier limits the input to 8,000 bytes; support for those releases should either
reject oversized source types or be declared unsupported.

This construction is a keyed/peppered hash, not HMAC. SQL Server does not expose
a built-in HMAC equivalent through `HASHBYTES`. It protects low-cardinality PII
from an attacker who has only the extracted tokens, provided the key remains
secret, but it is not a substitute for a reviewed tokenization service where
HMAC, key rotation, revocation, or reversible detokenization is required.

Use a domain string to prevent the same plaintext from receiving the same token
in unrelated columns. Use the same domain and key version only when cross-table
equality joins are an explicit requirement.

For non-deterministic output, SQL Server can render a fresh UUID:

```sql
CASE
  WHEN [support_note] IS NULL THEN NULL
  ELSE CONVERT(varchar(36), NEWID())
END AS [support_token]
```

This value changes on every source read. It breaks grouping, joins, deduplication,
content-hash freshness checks, and stable Parquet output. It should be rejected
for split keys and disabled for tables using content-based freshness. In most PII
cases, `drop` is preferable because a random token retains no analytical value.

## Native dialect matrix

| Dialect | Deterministic primitive | Random primitive | Initial support |
| --- | --- | --- | --- |
| SQL Server | `HASHBYTES('SHA2_256', ...)` | `NEWID()` | Full |
| PostgreSQL | `digest(..., 'sha256')` from `pgcrypto` | `gen_random_uuid()` | Probe extension, then enable |
| Oracle | `STANDARD_HASH(..., 'SHA256')` | `SYS_GUID()` | Full after driver test |
| Databricks | `sha2(..., 256)` | `uuid()` | Full after SQL warehouse test |
| SQLite | No cryptographic hash in core | `randomblob(16)` | `drop` only by default |
| Generic | Unknown | Unknown | `drop` and pass-through only |

The dialect capability matrix should report each transform independently. There
must be no silent proxy-side fallback for PII: startup should fail when a table
requests a transform that its source cannot perform.

Native functions do not guarantee identical bytes across products. String
encoding, Unicode normalization, case folding, whitespace, date formatting,
decimal formatting, and null handling must be specified. The first release
should support string sources only and define these normalization modes:

- `none`: hash the database string value as represented by the dialect.
- `trim`: remove leading and trailing database whitespace.
- `trim_lower`: trim and lowercase using the database function.

Cross-dialect token equality is out of scope until byte-level canonicalization
test vectors pass against every supported engine. Database `LOWER` behavior can
vary with collation and locale.

## Security properties

Deterministic tokens reveal equality and frequency. An observer can tell that
two rows have the same input and can infer common values from their frequency.
Unkeyed SHA-256 is unsuitable for email addresses, phone numbers, national IDs,
postal codes, and other enumerable domains because an attacker can hash guesses.

The source query principal still has access to plaintext and the database engine
computes over plaintext. Pushdown protects the proxy, Parquet objects, and Fabric
consumers from receiving the source value; it does not protect PII from the
source database administrator or database execution environment.

Bind parameters keep secrets out of generated SQL text, but database tracing and
driver diagnostics can capture parameter values. Query logging must log policy
names and key references, never rendered parameters. Secret resolution should
return short-lived in-memory values and should use the existing credential-store
boundary.

Dynamic Data Masking is not a tokenization fallback. It changes query results for
principals without `UNMASK`, leaves source data unchanged, and Microsoft documents
that exhaustive queries can infer underlying values. Always Encrypted is useful
when the source column is already encrypted. Deterministic Always Encrypted allows
equality grouping, while randomized encryption intentionally prevents grouping;
neither is a projection-time replacement for the policy above.

## Implementation plan

### Phase 1: SQL Server deterministic hashing and removal

1. Add a frozen `ColumnTransform` model and optional `source` and `transform`
   fields to `ColumnDef`.
2. Parse and validate the allowlisted JSON shape. Require transformed output to
   use Iceberg `string`; reject transforms on `key_column`.
3. Add `Dialect.render_projection(column, parameter_namespace)` returning SQL and
   bind parameters. The base dialect supports pass-through only; SQL Server adds
   deterministic hashing.
4. Build the projection list and merge its parameters in `build_split_query()`.
5. Validate source names, preserve output aliases, and redact transform parameters
   from logs.
6. Let the config builder mark reflected columns as keep, deterministic token, or
   remove.

The C++ agent has a separate dialect and query-generation implementation under
`agent-cpp/tier1`. Either add the same policy contract there before declaring the
feature generally available, or reject transformed table definitions in the C++
agent with a clear startup error.

### Phase 2: random tokens and more dialects

Add `random_token` only after freshness behavior is explicit. Then add PostgreSQL,
Oracle, and Databricks adapters behind integration tests against real engines.
SQLite should remain unsupported for deterministic PII hashing unless a vetted
cryptographic extension is explicitly installed and detected.

### Phase 3: key lifecycle

Add key-version metadata, rotation tooling, and a migration mode that can expose
old and new tokens in separate columns during a controlled transition. Never
rotate a deterministic key in place: doing so invalidates historical equality
joins between snapshots.

## Tests and acceptance criteria

- Existing pass-through SQL is byte-for-byte unchanged.
- SQL Server emits `HASHBYTES('SHA2_256', ...) AS [output_name]` with the key as a
  bind parameter, not a SQL literal.
- Equal normalized non-null inputs produce equal tokens across splits and pulls.
- Different inputs and different domains produce different tokens.
- Null input remains null and empty string remains distinct from null.
- No plaintext transformed value appears in result rows or generated Parquet.
- Omitted columns appear in neither `SELECT`, Parquet, nor Iceberg metadata.
- Schema validation checks source names and reports the output/source mapping.
- Transform requests fail closed on unsupported dialects and missing key refs.
- Transforms on split keys are rejected.
- Query, exception, audit, and effective-config logs contain no token key.
- Random-token tables reject content-hash freshness and document that repeated
  reads produce different objects.
- Python and C++ agents produce the same policy outcome or the unsupported agent
  rejects the configuration at startup.

## Recommendation

Implement Phase 1 first. It covers the analytical requirement for stable grouping
and the stronger minimization option of column removal without introducing
unstable snapshots. Treat random tokens as an opt-in second phase; for a column
that no longer supports equality analysis, removal is simpler and exposes less
data.

## References

- [Microsoft: HASHBYTES (Transact-SQL)](https://learn.microsoft.com/sql/t-sql/functions/hashbytes-transact-sql)
- [Microsoft: Always Encrypted](https://learn.microsoft.com/sql/relational-databases/security/encryption/always-encrypted-database-engine)
- [Microsoft: Always Encrypted cryptography](https://learn.microsoft.com/sql/relational-databases/security/encryption/always-encrypted-cryptography)
- [Microsoft: Dynamic Data Masking](https://learn.microsoft.com/sql/relational-databases/security/dynamic-data-masking)
