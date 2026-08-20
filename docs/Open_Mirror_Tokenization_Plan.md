# Open Mirror Tokenization Plan

Branch: `Open-Mirror-Tokenizer`

Current kickoff status: deterministic projections, column omission, builder
preservation, dialect SQL reuse, and projection-state safety are implemented.
`random_token` recovery now persists a prepared Parquet sidecar before the landing-zone
write and replays that sidecar after an interrupted publish.
Sidecar paths are constrained to the deterministic state-file location, and an explicit
`initial` load replaces a changed projection fingerprint.
Config Builder and startup validation now reject invalid control columns, duplicate
output names, missing deterministic key references, and unsupported source dialects.

## Goal

Give Open Mirror tables the same allowlisted column tokenization policies already
available to shortcut warehouse tables:

- `deterministic_hash` with a credential-store/environment key reference, domain,
  and normalization;
- `random_token` where the tracking mode can preserve its semantics; and
- column omission by leaving a source column out of the published projection.

The feature must keep source plaintext out of landing-zone Parquet, published table
metadata, logs, and local change-tracking state wherever the existing policy allows.
It must preserve Open Mirror's numbered-file, watermark, snapshot-diff, pending-batch,
and delete semantics.

## Current boundary

Shortcut tokenization is already implemented as a `ColumnDef` projection policy:

- `config.ColumnTransform` validates the allowlist and key reference;
- `planner.dialects.Dialect.render_projection()` emits dialect-native SQL and bind
  parameters;
- `db.executor` redacts token parameters from diagnostics; and
- `parquet/generator.py` consumes rows by output column alias.

Open Mirror currently derives a reflected schema, constructs its own source `SELECT`
lists in `open_mirror/source.py`, and passes the returned column names directly to
`open_mirror/publisher.py`. Its target model has no output projection policy yet.

The implementation should reuse the existing `ColumnDef` and dialect projection
contract instead of adding a second tokenizer or hashing rows in Python.

## Proposed contract

Add an optional `columns` array to each Open Mirror table target. Its entries use the
same column shape as the shortcut table schema:

```json
{
  "name": "email_token",
  "source": "email",
  "type": "string",
  "transform": {
    "kind": "deterministic_hash",
    "key_ref": "customer-pii-v1",
    "domain": "customer-email",
    "normalization": "trim_lower"
  }
}
```

The exact field-id and type parsing should use the existing `ColumnDef` loader. When
`columns` is absent, current reflected pass-through behavior remains unchanged.

Rules for the first implementation:

1. The source key columns must remain present and untransformed. They are required by
   Fabric `keyColumns`, snapshot state, and delete rows. Reject a transform or omission
   of a configured key column rather than silently changing identity semantics.
2. The watermark column must remain available as an internal raw source expression for
   cursor predicates and cursor advancement. It may be excluded from the published
   projection only if the extraction layer can carry it in a private internal alias;
   otherwise reject that configuration explicitly. Never hash a watermark used as the
   cursor.
3. Transformed output columns use their configured output aliases. Source names are
   used only in quoted SQL expressions and validation.
4. Deterministic hashing is supported for watermark and snapshot modes. It must use
   the existing dialect capability checks and bound key/domain parameters.
5. `random_token` is supported only with prepared-payload recovery. Snapshot rereads
   would create a change on every cycle, and watermark retries can regenerate different
   values for the same pending source page.
6. Pending-batch recovery remains deterministic: the prepared Parquet payload is stored
   as a sidecar, verified by its SHA-256 digest, replayed if needed, and deleted only
   after the state commit succeeds. A missing or corrupt sidecar still fails closed.

## Implementation phases

### Phase 1: model and configuration

1. Extend `OpenMirrorTableTarget` with an optional output-column definition while
   retaining the existing `schema` string field for the Fabric schema folder.
2. Parse `columns` through the same `ColumnDef`/transform validation used by shortcut
   tables. Preserve backward compatibility when the field is absent.
3. Add config-builder load/save support and validation messages for Open Mirror column
   policies. Do not expose or persist token key material; persist only `key_ref`.
4. Add an example with a deterministic token and an omitted source field.

### Phase 2: source projection and schema

1. Refactor Open Mirror source reads to build projections through the selected dialect's
   `render_projection()` API.
2. Keep raw source expressions for key and watermark control values in private aliases
   when they are not published. Ensure cursor ordering and predicates use quoted source
   identifiers, not token aliases.
3. Return the published `ColumnDef` list and control-column values separately so the
   publisher never receives accidental plaintext columns.
4. Make initial, watermark, and snapshot reads use the same projection builder, including
   pagination and dialect-specific row limits.
5. Ensure token parameters are merged into the existing bind map and remain redacted in
   query, exception, audit, and effective-config logs.

### Phase 3: change tracking and publishing

1. Hash only the published row representation plus the required key identity for snapshot
   state; never hash a discarded source value that was returned solely for control logic.
2. Build delete rows from key columns only, preserving Fabric row-marker semantics.
3. Ensure `_metadata.json` describes the published key columns and output schema without
   source-only or omitted fields.
4. Include transformed output columns in Parquet with their declared types and aliases.
5. Include token policy identity (kind, key reference, domain, normalization) in the
   state/policy fingerprint. This fingerprint is now persisted without secret key
   material; a policy change requires an explicit initial/reset load rather than silently
   comparing old and new token values.
6. Keep pending-batch digest and recovery behavior fail-closed. The prepared-payload
   sidecar now provides the recovery contract for random-token batches.

### Phase 4: testing and rollout

1. Add parser/config-builder tests for pass-through, deterministic, random, omitted,
   missing-key, invalid-type, and unsupported-policy cases.
2. Add dialect SQL tests for SQL Server, PostgreSQL, Oracle, and Databricks. Assert that
   token keys and domains are bind parameters and never appear in SQL text or logs.
3. Add Open Mirror tests covering initial load, watermark pagination, snapshot diff,
   update/delete markers, pending recovery, state-policy changes, and table metadata.
4. Assert that source plaintext is absent from generated Parquet, pending state, state
   hashes, exceptions, and structured logs when the source field is transformed or
   omitted.
5. Add deterministic stability tests across pages and repeated runs. Add explicit random
   token tests only after the recovery contract is implemented.
6. Run the existing tokenization UAT for each supported dialect, then add a Fabric Open
   Mirror landing-zone verification using a disposable mirrored database.

## Configuration and compatibility decisions

- Existing Open Mirror configs remain valid and retain reflected pass-through behavior.
- The Open Mirror table `schema` property continues to mean the Fabric schema-folder
  name; the output column list is a separate `columns` property.
- Key references continue to resolve through `FSP_TOKENIZATION_KEY_<KEY_REF>` and the
  existing credential boundary. No secret values belong in `config.open_mirror.json`.
- Tokenization capability failures are startup/configuration errors. There is no
  proxy-side plaintext fallback for an unsupported source dialect.
- Changing a deterministic key, domain, normalization, or output alias changes the
  published row representation. Require an explicit reset/initial load and document
  the effect on downstream equality joins and historical data.

## Risks and open decisions

### Raw control values in local state

Watermark cursors currently store the last source watermark and key values. If a
watermark is itself sensitive, this is local state containing source plaintext even
when the published column is tokenized. Decide whether to document that boundary,
encrypt the state, or support a separate non-sensitive cursor column before claiming
full plaintext exclusion.

### Random-token recovery

Random tokens intentionally change on extraction. Open Mirror now persists the prepared
Parquet payload in a state-directory sidecar before recording the pending batch. Recovery
replays and verifies that sidecar instead of re-extracting the source page. Sidecar
permissions and state-directory durability remain deployment responsibilities.

### Key rotation and Fabric history

Rotating a deterministic token key changes values and can create a large update batch.
The rollout must define whether old and new token columns are published side by side,
or whether the operator performs a controlled reset and accepts the historical join break.

## Completion criteria

The feature is ready when a configured Open Mirror table can publish a deterministic
tokenized column on all supported tokenization dialects, with no source value in the
published Parquet or metadata, while watermark and snapshot behavior remains correct.
Existing unconfigured Open Mirror targets must produce byte-compatible pass-through
queries and equivalent landing-zone output. Unsupported or unsafe combinations must
fail closed with actionable diagnostics.

## References

- [TOKENIZATION_PUSHDOWN.md](TOKENIZATION_PUSHDOWN.md)
- [TOKENIZATION_MULTI_DIALECT_UAT.md](TOKENIZATION_MULTI_DIALECT_UAT.md)
- [UsecasesAndScenarios.md](UsecasesAndScenarios.md)
- [Open Mirror configuration](../config.open_mirror.example.json)