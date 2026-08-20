# Open Mirror Tokenization UAT

Use this runbook after the automated tests and before enabling tokenized Open Mirror
publishing against a production mirrored database.

## Automated precheck

From the repository root:

```powershell
python -m pytest tests/test_config_file.py tests/test_open_mirror_builder.py `
  tests/test_open_mirror_source.py tests/test_open_mirror_incremental.py `
  tests/test_dialects.py tests/test_capabilities.py tests/test_objectstore_tokenizer.py -q
```

The suite must pass. `tests/test_hardening.py` also requires either
`REQUIRE_SIGV4=0` or test values for `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`.

## Test data

Use a disposable source table with a non-null key, a monotonic watermark, a PII string,
and a second ordinary column:

| id | modified_at | email | status |
|---:|---|---|---|
| 1 | 2026-08-20T10:00:00Z | Alice@Example.com | active |
| 2 | 2026-08-20T10:01:00Z |  alice@example.com  | active |
| 3 | 2026-08-20T10:02:00Z | bob@example.com | active |
| 4 | 2026-08-20T10:03:00Z | null | inactive |

Configure the Open Mirror table with:

- `key_column`: `id`
- `watermark_column`: `modified_at`
- `columns`: `id`, `email_token`, and `status`
- `email_token.source`: `email`
- `email_token.transform.kind`: `deterministic_hash`
- `normalization`: `trim_lower`
- `key_ref`: a versioned reference such as `customer-pii-v1`
- `domain`: `customer-email`

The watermark is intentionally omitted from `columns`; it must be selected under a
private `__om_control_*` alias and must not be published.

Set the key only in the Manager environment:

```powershell
$env:FSP_TOKENIZATION_KEY_CUSTOMER_PII_V1 = "replace-with-a-long-random-value"
$env:OPEN_MIRROR_ENCRYPT_STATE = "1"
```

## Deterministic token checks

1. Run an initial Open Mirror publish.
2. Read the landing-zone Parquet file with a trusted inspection tool.
3. Confirm the published columns are exactly `id`, `email_token`, `status`.
4. Confirm rows 1 and 2 have equal tokens after `trim_lower` normalization.
5. Confirm row 3 has a different token and row 4 remains null.
6. Search the Parquet, `_metadata.json`, state JSON, and logs for the source email values.
   No source email or token key may appear.
7. Confirm the local state contains an encrypted sensitive-state envelope when
   `OPEN_MIRROR_ENCRYPT_STATE=1`.

## Incremental and delete checks

1. Update row 2 and advance its watermark. Insert a new row with a higher watermark.
2. Publish the next cycle and confirm update/upsert markers and the committed cursor.
3. Delete a source row in snapshot mode and confirm a delete marker contains only the key.
4. Confirm the omitted watermark is never present in landing-zone Parquet.
5. Repeat the same cycle without source changes and confirm it is a no-op.

## Random-token recovery checks

Use a disposable table and a `random_token` output column. Do not combine it with a
content-based shortcut refresh policy.

1. Start a publish and interrupt it after the pending state is written but before the
   landing-zone file is available.
2. Confirm the state-directory `.pending.parquet` sidecar exists.
3. Restart publishing with the same state directory.
4. Confirm the sidecar is replayed, the landing-zone file is written, the cursor commits,
   and the sidecar is deleted.
5. Corrupt the sidecar and repeat. Publishing must fail closed with a digest mismatch;
   it must not re-extract the source page and generate a different random token.

## Key rotation checks

1. Publish with `customer-pii-v1` and key A.
2. Change the environment value for the same key reference to key B.
3. A normal incremental publish must fail with a projection-change/reset diagnostic.
4. Use the explicit initial/reset workflow, then confirm the state fingerprint changes and
   the new tokens are not equal to the old tokens.

## Fabric verification

Against a disposable Fabric mirrored database, verify:

- `_metadata.json` retains the configured key columns.
- Numbered Parquet files are consumed in order.
- Incremental row markers apply inserts, updates, upserts, and deletes correctly.
- The omitted watermark is not materialized as a Fabric column.
- Deterministic equality joins work for normalized equal inputs.
- Random-token values are replaced only when a new source extraction is intentionally
  published, not during pending-batch recovery.

## Evidence to retain

Record the source engine/version, proxy commit, dialect, config projection (without key
material), key reference/version, Fabric mirrored-database id, publish results, and the
landing-zone file names. Never retain the token key or source PII in the test artifacts.