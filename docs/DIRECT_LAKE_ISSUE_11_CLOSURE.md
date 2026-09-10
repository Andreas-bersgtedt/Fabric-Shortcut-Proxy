# Direct Lake issue #11 closure

Date: 2026-09-10

## Result

Direct Lake semantic model creation now works for the test table `Address` in the Fabric workspace `Issue_11_FSP_test`. The native Fabric wizard created semantic model `gggg` and opened its model view.

## Root cause

Fabric reached the proxy, but the S3 object metadata contract was incomplete during schema discovery and Parquet framing:

- `ListObjectsV2` returned content-hash ETags while `HEAD` omitted `ETag`.
- Fabric sent object requests with trailing slashes, and those paths were not normalized to the canonical S3 key.
- Database-backed `HEAD` responses omitted RFC 1123 `Last-Modified`.
- Full and ranged `GET` responses also omitted `Last-Modified`.

The resulting Fabric message was the generic `Something went wrong` error. The reported UPN claim message was not the root cause.

## Fixes

- PR #76 added `HEAD` ETag consistency and trailing-slash key normalization.
- PR #77 added `Last-Modified` to `HEAD` responses.
- PR #78 added `Last-Modified` to full and ranged `GET` responses.

## Validation

- `tests/test_delta.py`: 12 tests passed.
- PR #78: all 9 checks passed.
- VM `fabricproxy001`: merge commit `9fccc0fc85923474fdf9d0ce7e8360af01a3cf79`.
- `fabric-shortcut-proxy`: active/running.
- Live proxy logs showed successful Delta-log discovery, commit `HEAD` requests, trailing-slash probes, and ranged Parquet `GET` responses returning `206`.

The detailed chronological investigation remains in the local ignored file `devplan/DirectLake_investigation.md`. The plaintext secrets previously found in `config.system.json` on the VM remain a separate security issue and are not part of this closure.
