# Chapter 15: Audited re-identification

Re-identification is an optional, disabled-by-default module for resolving one
configured source value from a durable SHA-256 token. It is intended for a
controlled Auditor workflow. It is not token decryption, bulk export, or a
general-purpose search endpoint.

## 15.1 Enable the module

The module requires both controls below. Changes take effect after a Manager
restart.

1. In **Operational > System configuration > Optional modules**, select
   `reidentification` and save the desired profile.
2. In **Admin and observability** settings, set `enable_reidentification` to
   `true` and save the system configuration.
3. Install the selected profile if the deployment requires it, then restart
   the Manager.

The route is absent when either control is off. The module has no additional
Python package dependency, but the source database driver and credentials must
already be available.

## 15.2 Configure a mapping

Mappings are secret-free metadata. The default file is
`config.reidentification.json` under `FSP_CONFIG_DIR`. The Config Builder
Security tab can create, list, and delete mappings for a system administrator.

```json
{
  "mappings": [
    {
      "policy_id": "customer-pii-v1",
      "table_id": "customers_safe",
      "column_id": "email_token",
      "lookup_column": "email_token_lookup",
      "clear_text_column": "email",
      "primary_key_column": "customer_id"
    }
  ]
}
```

The mapping must reference an enabled durable `sha256` policy, an enabled table,
and a table column assigned to that policy. `clear_text_column` must match the
configured source column. `lookup_column` must be a source-maintained indexed
token column. The proxy validates identifiers and never accepts SQL, connection
strings, keys, raw tokens, or clear-text values in the mapping file.

The first release supports SQL Server, PostgreSQL, Oracle, and Databricks SQL.
SQLite, random tokens, BLAKE2b, Arrow fallback, and bulk lookup are rejected.

## 15.3 Request a lookup

Only the `auditor` role receives `tokenization.reidentify`. System administrators
do not receive this permission automatically. Every request must include exactly
these fields:

```http
POST /_reidentify/api/v1/lookup/customer-pii-v1/customers_safe/email_token
Content-Type: application/json

{"token":"<64 uppercase hex characters>","reason_code":"audit","case_reference":"CASE-123"}
```

Allowed reason codes are `audit`, `investigation`, and `legal_hold`. The token
is submitted to the source as a bound parameter. The query selects only the
configured primary key and clear-text column and limits the source result to
two rows.

One match returns the primary key and value. Zero or multiple matches return
the same generic `not_found_or_ambiguous` outcome. The browser Config Builder
does not provide a token entry or result viewer.

## 15.4 Audit and quotas

Set both `ENABLE_AUDIT_LOG=1` and `AUDIT_LOG_FILE` before enabling lookups.
The audit record is written before a clear-text response. It contains the
request ID, identity, outcome, reason, policy/table/column identifiers, case
reference, match count, latency, and a token fingerprint. It does not contain
the raw token, clear-text value, SQL, bind values, key, or credential.

Configure the in-process limits with:

```text
REIDENTIFICATION_REQUESTS_PER_MINUTE=5
REIDENTIFICATION_REQUESTS_PER_DAY=50
```

The limits are per identity. In a multi-process or multi-Agent deployment,
treat these as per-process limits until a shared quota backend is configured.
An unavailable audit sink fails closed and prevents the lookup response.

## 15.5 Disable and roll back

To roll back, set `enable_reidentification` to `false`, remove
`reidentification` from the desired module profile, and restart the Manager.
Confirm that `/_reidentify/api/v1/lookup/...` is no longer mounted and retain
the audit file according to the deployment retention policy. Do not delete the
source lookup column or policy until dependent audit and investigation records
are no longer needed.

## 15.6 Validation

Use the focused automated coverage in `tests/test_reidentification.py` and the
repository test suite before deployment. Live UAT must cover one, zero, and
multiple matches, an indexed query plan, local and OIDC Auditor identities,
quota boundaries, audit-sink failure, disabled-module rollback, and each
approved source dialect.

Return to [Chapter 7: Security](07-security.md) for identity and audit policy,
or [the manual index](README.md) for the other operating procedures.