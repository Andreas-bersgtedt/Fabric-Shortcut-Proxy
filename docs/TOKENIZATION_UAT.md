# SQL Server pushdown tokenization UAT

## Scope

This UAT exercises the Python agent against SQL Server. It covers deterministic
hashing with `HASHBYTES`, random UUID tokens with `NEWID`, and source-column
removal through an explicit output schema.

Use SQL Server 2016 or later. The first implementation supports the `mssql`
dialect only. Configure policies through the Config Builder or edit
`config.tables.json` directly.

## 1. Create source data

Run this in a disposable SQL Server database:

```sql
DROP TABLE IF EXISTS dbo.customers_pii;

CREATE TABLE dbo.customers_pii (
    customer_id bigint NOT NULL PRIMARY KEY,
    email nvarchar(320) NULL,
    support_note nvarchar(1000) NULL,
    ssn varchar(11) NULL
);

INSERT dbo.customers_pii (customer_id, email, support_note, ssn)
VALUES
    (1, N'Alice@Example.com',  N'First case',  '111-11-1111'),
    (2, N' alice@example.com ', N'Second case', '222-22-2222'),
    (3, N'bob@example.com',    N'Third case',  '333-33-3333'),
    (4, NULL,                  NULL,           '444-44-4444');
```

Grant the proxy principal `SELECT` on this table. No write or schema permission
is required.

## 2. Configure the table

In the Config Builder, connect to SQL Server and select `dbo.customers_pii`.
Open **Per-Table Configuration**, expand the SQL Server source, then select
**Edit column policies**:

1. Leave `customer_id` as **Keep**. Split-key policies are locked.
2. Set `email` to **Deterministic token**, output name `email_token`, key
  reference `customer-pii-v1`, domain `customer-email`, and normalization
  `trim_lower`.
3. Set `support_note` to **Random token** and output name `support_token`.
4. Set `ssn` to **Remove**, then apply the table configuration and restart.

The key reference produces the environment variable name
`FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1`. The key value is not stored in the
table configuration.

For a direct JSON setup, add this entry to the `tables` array in
`config.tables.json`. Remove other table entries for the simplest UAT.

```json
{
  "name": "customers_safe",
  "source_table": "dbo.customers_pii",
  "key_column": "customer_id",
  "num_splits": 2,
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

The `ssn` source column is removed because it is absent from the output schema.
The published Iceberg table contains only `customer_id`, `email_token`, and
`support_token`.

## 3. Set the key and start

Set a disposable UAT key in the same PowerShell process that starts the Manager:

```powershell
$env:FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1 = "replace-with-a-long-random-uat-value"
$env:AUTO_REFRESH = "0"
.\Manager.ps1 -SkipInstall -DbUrl "mssql+aioodbc://user:password@server/UatDb?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
```

Replace the connection string with the UAT SQL Server. Do not place the
tokenization key in `config.tables.json` or the database URL.

Startup must complete without a configuration error. SQL execution logs should
contain `HASHBYTES('SHA2_256', ...)`, while `__token_key_*` and
`__token_domain_*` parameter values appear as `[REDACTED]`.

## 4. Query through Fabric

Create or refresh the Fabric shortcut for `customers_safe`, then run:

```sql
SELECT customer_id, email_token, support_token
FROM customers_safe
ORDER BY customer_id;

SELECT email_token, COUNT(*) AS customer_count
FROM customers_safe
GROUP BY email_token
ORDER BY customer_count DESC;
```

Expected results:

- Rows 1 and 2 have the same `email_token` because `trim_lower` normalizes both
  email values to `alice@example.com`.
- Row 3 has a different 64-character hexadecimal `email_token`.
- Row 4 has null `email_token` and null `support_token`.
- Every non-null `support_token` is a 36-character UUID.
- `email`, `support_note`, and `ssn` are absent from the Fabric table schema.

Search generated Parquet and query results for `Alice@Example.com`, `First case`,
and `111-11-1111`. None of those plaintext values should be present.

## 5. Stability and rotation

Record the `email_token` values, restart the Manager with the same key, and
rematerialize the table. The deterministic tokens must remain unchanged.

Change the environment key, restart, and rematerialize. Every non-null
`email_token` must change. Restore the original key after this check if old and
new snapshots need to remain equality-compatible.

Random `support_token` values are expected to change whenever the source is read
again. Random-token tables cannot use `REFRESH_STRATEGY=content_hash`, or `auto`
with `REFRESH_ALLOW_FULL_PULL=1`, because every read would look like new content.

## 6. Negative checks

Run each check separately and confirm startup fails before reading source rows:

1. Remove `FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1`. The error names the missing
   environment variable.
2. Point the transformed table at SQLite or PostgreSQL. The error states that
   this release supports `mssql` transforms only.
3. Apply a transform to `customer_id`. The error rejects transforms on the split
   key.
4. Enable content-hash auto-refresh while retaining `random_token`. The error
   reports the freshness conflict.

## 7. Automated precheck

From the repository virtual environment:

```powershell
python -m pytest tests/test_config_file.py tests/test_dialects.py tests/test_phase2.py tests/test_hardening.py tests/test_capabilities.py -q
```

This check does not replace SQL Server UAT. It verifies generated T-SQL,
parameter binding, redaction, configuration parsing, and failure behavior without
a live SQL Server.

## Known limitations

- The deterministic construction is a secret-keyed SHA-256 input to
  `HASHBYTES`, not HMAC and not reversible tokenization.
- Deterministic tokens reveal equality and frequency.
- Normalization follows SQL Server collation and `LOWER` behavior.
- The standalone C++ native publisher does not consume `config.tables.json` and
  is outside this UAT path.
- Transform configuration is manual JSON in this release.
