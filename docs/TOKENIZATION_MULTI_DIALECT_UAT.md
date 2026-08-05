# Multi-dialect tokenization UAT

Use this runbook for PostgreSQL, Oracle, and Databricks SQL. SQL Server has a
separate completed runbook in `TOKENIZATION_UAT.md`.

The automated tests verify generated SQL, bind parameters, null handling, aliases,
and split-query structure. They do not execute against these database engines.
Record the engine version, driver version, and results when running this UAT.

## Expected behavior

Each source must expose these logical rows:

| customer_id | email | support_note |
| --- | --- | --- |
| 1 | `Alice@Example.com` | `First case` |
| 2 | ` alice@example.com ` | `Second case` |
| 3 | `bob@example.com` | `Third case` |
| 4 | null | null |

Configure `email` as a deterministic token with `trim_lower`, key reference
`customer-pii-v1`, and domain `customer-email`. Configure `support_note` as a
random token. The output schema must contain `customer_id`, `email_token`, and
`support_token` only.

Set the key in the process that starts the Manager:

```powershell
$env:FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1 = "replace-with-a-long-random-uat-value"
$env:AUTO_REFRESH = "0"
```

For every engine:

- Rows 1 and 2 must have the same 64-character uppercase hexadecimal
  `email_token`.
- Row 3 must have a different `email_token`.
- Row 4 must have null tokens.
- Re-reading with the same key must preserve deterministic tokens and replace
  non-null random tokens.
- Changing the key must replace every non-null deterministic token.
- Generated SQL and logs must not contain the key value.

Tokens are stable within one engine, key, domain, and normalization policy. They
are not guaranteed to match between database products because string encoding,
case folding, and type formatting differ.

## PostgreSQL

Deterministic tokens require the `pgcrypto` extension. Install it once in the UAT
database using a role with extension privileges:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS public.customers_pii;
CREATE TABLE public.customers_pii (
    customer_id bigint PRIMARY KEY,
    email text,
    support_note text
);
INSERT INTO public.customers_pii VALUES
    (1, 'Alice@Example.com', 'First case'),
    (2, ' alice@example.com ', 'Second case'),
    (3, 'bob@example.com', 'Third case'),
    (4, NULL, NULL);
```

Connect through Config Builder using the PostgreSQL source, select
`public.customers_pii`, and apply the policies above. The generated query must
contain `DIGEST(..., 'sha256')` and `gen_random_uuid()`. Deterministic output is
converted with `ENCODE(..., 'hex')` and uppercased.

Negative check: remove `pgcrypto` in a disposable database or revoke access to
`digest`. The materialization must fail; the proxy must not return plaintext or
fall back to proxy-side hashing.

## Oracle

Use Oracle 12c or later so `STANDARD_HASH(..., 'SHA256')` and the existing split
query syntax are available.

```sql
BEGIN
  EXECUTE IMMEDIATE 'DROP TABLE CUSTOMERS_PII';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE != -942 THEN RAISE; END IF;
END;
/

CREATE TABLE CUSTOMERS_PII (
    CUSTOMER_ID NUMBER(19) PRIMARY KEY,
    EMAIL VARCHAR2(320),
    SUPPORT_NOTE VARCHAR2(1000)
);
INSERT ALL
  INTO CUSTOMERS_PII VALUES (1, 'Alice@Example.com', 'First case')
  INTO CUSTOMERS_PII VALUES (2, ' alice@example.com ', 'Second case')
  INTO CUSTOMERS_PII VALUES (3, 'bob@example.com', 'Third case')
  INTO CUSTOMERS_PII VALUES (4, NULL, NULL)
SELECT 1 FROM DUAL;
COMMIT;
```

Select `CUSTOMERS_PII` through Config Builder and apply the common policies. The
query must contain `RAWTOHEX(STANDARD_HASH(..., 'SHA256'))` and
`RAWTOHEX(SYS_GUID())`. Oracle random tokens are 32 hexadecimal characters.
Oracle treats an empty string as null; do not use empty strings to test null
separation on this engine.

## Databricks SQL

Run this setup in a disposable catalog and schema attached to the SQL warehouse:

```sql
CREATE OR REPLACE TABLE customers_pii (
  customer_id BIGINT NOT NULL,
  email STRING,
  support_note STRING
) USING DELTA;

INSERT OVERWRITE customers_pii VALUES
  (1, 'Alice@Example.com', 'First case'),
  (2, ' alice@example.com ', 'Second case'),
  (3, 'bob@example.com', 'Third case'),
  (4, NULL, NULL);
```

Connect Config Builder to the SQL warehouse with its HTTP path, select the table,
and set `customer_id` explicitly if primary-key reflection is unavailable. The
query must contain `sha2(concat(...), 256)` and `uuid()`. Random tokens are
36-character UUID strings.

Exercise both modulo and range splits. Range SQL must end in `LIMIT :max_rows`
and must not contain `TOP`.

## Fabric verification

Refresh the shortcut and run:

```sql
SELECT customer_id, email_token, support_token
FROM customers_safe
ORDER BY customer_id;

SELECT email_token, COUNT(*) AS customer_count
FROM customers_safe
GROUP BY email_token
ORDER BY customer_count DESC;
```

Confirm the normalized Alice rows form one group with count 2. Search query
results and generated Parquet for the source email and note values. No transformed
plaintext value should be present.

## Automated precheck

Run before each live UAT:

```powershell
python -m pytest tests/test_dialects.py tests/test_capabilities.py tests/test_config_file.py tests/test_hardening.py tests/test_config_builder.py -q
```
