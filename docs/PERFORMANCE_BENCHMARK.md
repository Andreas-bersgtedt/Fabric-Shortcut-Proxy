# Distributed topology performance benchmark

This document records the #156 acceptance run on 2026-10-13. Machine-readable
results are in
[`benchmarks/results/phase3-topology-2026-10-13.json`](../benchmarks/results/phase3-topology-2026-10-13.json).

## Environment

- Source: `Sales_DW::SalesLT.Customer`, 847 rows.
- Central topology: materializer, SQL source, and published Blob account in
  Sweden Central.
- Distributed topology: materializer in North Europe with the SQL source and
  published Blob account in Sweden Central.
- Image:
  `sha256:1e2b6c5513e0772a344033a0b8d183f56d055473cc566d96fa41741462c4f9e0`.
- Signed policy revision 2.
- Row target: 250,000.
- Compressed byte target: 134,217,728.
- Estimated compressed bytes per row: 100.
- Source and pool concurrency limits: 4.

The distributed trial deliberately crosses the regional boundary twice:
source rows travel Sweden Central to North Europe and Parquet travels North
Europe to Sweden Central. It tests the cost of incorrect placement for this
dataset; it does not represent a source-local North Europe database.

## Materialization scenarios

The benchmark runner records source wait, estimated source bytes, encoding CPU
and wall time, Parquet bytes, upload time, failed bytes, and total duration.
Published rows per second uses rows divided by scenario wall time.

| Scenario | Sweden Central wall | Sweden rows/s | North Europe wall | North Europe rows/s |
| --- | ---: | ---: | ---: | ---: |
| One split, concurrency 1 | 2.098 s | 403.8 | 2.423 s | 349.6 |
| Four splits, intended concurrency 4 | 3.665 s | 231.1 | 2.901 s | 292.0 |
| Two logical datasets in different pools | 2.982 s | 284.0 | 3.783 s | 223.9 |
| Eight splits, pool saturated at 4; peer idle | 2.871 s | 295.1 | 5.039 s | 168.1 |

The two-pool scenario ran both regional commands concurrently. It used two
logical benchmark dataset labels over the same table, projection, snapshot,
and object store so differences reflect pool location rather than source data.
Combined throughput was 447.8 rows/s.

The four-split output was 140,633 Parquet bytes from an estimated 338,625
source-result bytes, a 2.408:1 ratio. For the distributed topology, estimated
cross-region traffic was 479,258 bytes per refresh: 338,625 source bytes plus
140,633 Parquet bytes. The central topology crossed no regional boundary for
these stages.

## Worker failure and retry

Each injected failure retried the same deterministic Parquet output and
completed with the expected SHA-256.

| Failure point | Failed bytes | Sweden wall | North Europe wall |
| --- | ---: | ---: | ---: |
| 25% of upload | 23,078 | 2.340 s | 2.030 s |
| 75% of upload | 69,235 | 2.340 s | 2.061 s |
| After upload completion | 92,314 | 2.697 s | 2.253 s |

The test records retry cost rather than claiming those small-object timings
predict production failure recovery.

## Private-link artifact throughput

Each location ran three verified 64 MiB uploads and downloads against the
published Blob account.

| Materializer location | Upload p50 | Upload p95 | Download p50 | Download p95 |
| --- | ---: | ---: | ---: | ---: |
| Sweden Central | 97.34 MB/s | 105.32 MB/s | 130.85 MB/s | 134.10 MB/s |
| North Europe | 31.31 MB/s | 31.85 MB/s | 17.01 MB/s | 19.21 MB/s |

All six upload/download trials succeeded. Azure Storage metrics for the
one-hour measurement window contained 386,071 successful transactions and 191
`ClientOtherError` transactions from total account activity. No
`ClientThrottlingError` or `ServerBusyError` series appeared.

These measurements support a source limit of 4 and pool limit of 4 for this
acceptance workload. Production-sized tables need a new run before raising
either limit.

## Fabric serving scenario

Both regional readers were restarted before the cold probe. Ten warm requests
per method then ran through the existing OneLake shortcuts.

| Shortcut | Cold HEAD | Cold full GET | Cold 64 KiB range | Warm range p50 | Warm range p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sweden Central | 6,146 ms | 3,866 ms | 3,459 ms | 2,420 ms | 3,479 ms |
| North Europe | 4,596 ms | 3,862 ms | 1,105 ms | 3,211 ms | 3,583 ms |

Every full GET returned 92,314 bytes and every range returned 65,536 bytes.
The serving measurements include OneLake/Fabric shortcut overhead and are not
direct S3 endpoint latency.

## Product-path verification

After the benchmark, the normal Manager queue created generation 57 with the
new streaming path. Materializer logged `parquet_streamed_to_store` for 92,314
bytes and 847 rows. Publication completed with zero failures and zero
intermediate pointer exposure; North Europe replication completed in 13.294
seconds against the 60-second SLO.

## Production decision

Keep `Sales_DW::SalesLT.Customer` materialization in Sweden Central. Its source
and published store are both there, so North Europe adds two WAN transfers.
The 64 MiB North Europe upload p50 was 31.31 MB/s versus 97.34 MB/s in Sweden
Central.

Use these accepted settings for the current workload:

- `source_max_concurrency: 4`;
- pool `max_concurrency: 4`;
- `SPLIT_TARGET_ROWS=250000`;
- `SPLIT_TARGET_BYTES=134217728`;
- `SPLIT_ESTIMATED_BYTES_PER_ROW=100`.

The source table is small. Absolute throughput and Fabric latency must be
remeasured on production-sized data before capacity planning.
