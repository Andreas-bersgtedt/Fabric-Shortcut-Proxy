# Direct Lake "upn claim is not present" — investigation (issue #11)

## Symptom

Creating a Direct Lake semantic model over a shortcut that resolves to the proxy
fails in Fabric with a generic "unknown error". The reporter associated it with
the trace line `upn claim is not present in AuthenticationContext` and suspected
mangled S3 API calls or a non-conformant Delta table.

## Evidence

Source: server-side Fabric/PbiDedicated + Analysis Services (Picasso) trace,
`a-export (1) (1).csv`, 2026 rows, capture window 11:20:29–11:22:11 UTC.

1. **The UPN line is benign.** `upn claim is not present in AuthenticationContext`
   is emitted by MISE token validation for **app-only service principal tokens**,
   which have no `upn` (user) claim by design. Every occurrence is immediately
   followed by successful authentication
   (`[AccessFilteringResult-Passed] ... ServicePrincipalAadAuthenticationContext`)
   and `StatusCode: [200]`. It also appears inside the **successful** framing
   session, so it does not correlate with the failure.

2. **The Delta read succeeded in the trace.** The engine framed the table:
   - `DatasourceDetection ... Kind AzureDataLakeStorage, PartitionType: 5`
   - `Successfully populated TMID mapping ... framing:1 ... 1 tables and 24 columns`
     for delta table `SO_Header`
   - Execution stats: `PartitionsSynced=1, ColumnsSynced=24, AffectedFiles=1,
     RefreshTabularModel=1`

   So the `_delta_log` and Parquet were read and interpreted correctly.

3. **The user-facing error time is not in the read path.** The reported error at
   12:22:08 BST (11:22:08 UTC) maps to `Workload.ExecuteAutomaticWorkloadRequestAsync`
   calls that all return 200. The actual failing stack is not present in this
   trace.

## Conclusion

The named error is a red herring, and this trace does **not** substantiate the
mangled-S3 / non-conformant-Delta hypothesis: the delta framing worked. Localizing
the real failure requires the proxy-side access log for the read window (S3
`ListObjectsV2` / ranged `GET` on `_delta_log/*.json` and data Parquet), which the
existing logging did not capture at sufficient resolution.

The Delta emitter (`delta/log.py`) was reviewed: protocol reader 1 / writer 2,
`metaData`, content-addressed add/remove diffs, `stats` with `numRecords`, no
deletion vectors. No concrete conformance defect was found.

## Change in this branch

Added targeted, opt-in S3 access diagnostics so the next reproduction captures the
exact request chain (`system_config.S3_ACCESS_LOG`, env `S3_ACCESS_LOG`, default
on):

- `s3_object_response` — per-response log for every ranged read and every
  `_delta_log` commit: key, kind, status (200/206/416), requested range, resolved
  start/end/total, bytes served, ETag.
- `s3_list_delta_log` — the exact `_delta_log/` commit files a reader discovers on
  a list, so a framing gap is visible.
- `delta_log_commit_missing` — a requested `NN.json` commit that is absent is
  surfaced at warning with the commits that do exist (a missing `_last_checkpoint`
  stays at debug, since it is an expected probe).

Set `S3_ACCESS_LOG=false` (env or `config.system.json`) to quiet these logs.

## Next step

Reproduce the Direct Lake model creation with `S3_ACCESS_LOG` on and collect the
proxy log for the read window. Compare the `_delta_log` commits served and the
ranged Parquet reads against a known-good native Delta table to find the first
divergent request.
