# Federated publication runtime

Phase 4 is tracked by #141. Catalog-backed publication, verified replication
and regional S3 serving are opt-in runtime paths. They have local acceptance
coverage, but the Azure/Fabric production topology has not been deployed or
accepted. Existing non-federated behavior remains the default.

## Data stores and control stores

[cloud_artifact_store.py](../src/fabric_shortcut_proxy/runtime/cloud_artifact_store.py)
implements writable S3 and GCS data stores. Install `s3-artifacts` or
`gcs-artifacts`, then use `build_cloud_data_store` with a bucket, confinement
prefix and, for S3, region. SDK imports are lazy. The factory uses the SDK's
credential chain; it does not accept access keys or service-account JSON in
the profile. Operators must configure workload identity and remove static
credentials from that chain at federated sites.

The stores support streamed uploads, immutable create-if-absent, ranged reads,
SHA-256 verification, paginated listings, deletion and single-object CAS.
Uploads spool to a temporary file before sending data, allowing length and
digest validation before committing an object. Configure temporary storage
inside the site's residency boundary, with enough disk space and the same
protection as other data caches. RAM use is bounded by the stream and upload
chunks, not total object size.

S3 uses conditional multipart completion. Failed multipart uploads are
aborted; abort failures are logged and the original upload failure propagates.
GCS uses generation preconditions and resumable uploads. Identical immutable
retries succeed; different bytes at the same key raise `ObjectConflict`.
Verification reads bytes incrementally in the regional process.

S3 and GCS are **not** added to `ARTIFACT_STORE_BACKEND`. The current Manager
requires cross-object fenced writes and guarded batches. An independent lease
check followed by an S3/GCS object write would not provide those guarantees.
Unsupported control mutations therefore raise `NotImplementedError`, rather
than falling back to an unsafe write. Single-object CAS is suitable for the
catalog record described below; it is not a general multi-object transaction.

[test_cloud_artifact_conformance.py](../tests/test_cloud_artifact_conformance.py)
runs the same data-plane/CAS checks against memory, local, Azure Blob, S3 and
GCS implementations. Cloud tests use SDK doubles, not live cloud services.
The dedicated CI matrix installs all three cloud SDK extras on Python 3.11
and 3.12. Separate backend tests cover multipart cleanup, reader bounds,
response closure, native SDK preconditions and error propagation.

## Residency configuration

`AGENT_PLACEMENT_CONFIG` accepts two optional registries:

- `stores`: named profiles with `provider` and `location`.
- `serving_endpoints`: named HTTPS endpoints with `url`, `location` and
  `storage_profile`. Credentials, query strings and fragments are rejected.

A constrained connection/table policy uses:

| Field | Meaning |
|---|---|
| `residency_locations` | Exact allowed location identifiers. Use cloud-qualified identifiers such as `azure:swedencentral` and `gcp:europe-north1`. |
| `required_pool` | Primary materializer pool. Every fallback is checked even when it is offline. |
| `required_storage_profile` | Published data store. |
| `staging_storage_profile` | Staging store; defaults to the published store. |
| `serving_endpoints` | Assigned regional endpoints. |
| `replica_storage_profiles` | Stores allowed to hold published replicas. |
| `cache_storage_profiles` | Declared data-cache stores. |
| `freshness_target_ms` | Optional positive integer freshness budget for a connection or table, in milliseconds. Unset targets emit no dataset freshness series. |

Pool locations, pool storage profiles, staging, published objects, declared
caches, replicas and serving locations must all be inside the allowed set.
Each endpoint must read the published store or a declared replica in its own
location. Unknown references fail closed.

For a constrained connection, table overrides inherit its policy and cannot
remove or widen its residency set. Unconstrained table overrides retain the
existing full-replacement behavior.

The Manager binds its actual artifact backend to a declared profile through
`FSP_ARTIFACT_STORE_PROFILE`, rendered by Helm's
`manager.artifactStoreProfile`. The profile provider must match
`ARTIFACT_STORE_BACKEND`. A missing binding rejects constrained startup.
The active work queue still supports one staging/published store. Policies
requiring a different staging store, published store or pool store are rejected
at startup and rechecked before dispatch, with `residency_policy_violation`.
This restriction prevents declaring a chain that the current runtime cannot
route. The request path also rechecks the policy before queue creation.
Queued dispatch violations appear in the Manager's placement status and
placement audit events, without claiming the task.

These checks validate operator declarations and existing queue routing.
They do not provision regional endpoints, enforce cloud IAM, discover the
physical location of a volume, or relocate an Agent's cache. Physical Azure IAM and deployed no-row-egress evidence remain required for #146.

## Catalog and regional verification

[federation.py](../enterprise/control/federation.py) separates a metadata-only
`GlobalCatalog` from the `RegionalReplicaVerifier`.
[catalog_app.py](../enterprise/control/catalog_app.py) mounts authenticated
create, inspect, handover, takeover, renewal and resolution routes. The Helm
chart can deploy this service with `federation.deployCatalog`.

When `FSP_FEDERATION_ENABLED=1`, the regional Manager builds a
`FederatedPublicationCoordinator`. Durable snapshots are converted to exact
generation descriptors, verified from streamed bytes, and advertised using
the current catalog owner fence. A Manager whose ownership or location changed
can still finish local staging, but cannot advertise that generation. Owner
leases are renewed by the running Manager.

Federation requires the durable materialization queue. It does not write the
legacy local visible snapshot record: the dataset catalog CAS is the sole
visibility commit. Each descriptor includes an immutable snapshot document,
which queue readers load through that region's committed catalog pointer.
If local request bookkeeping fails after CAS, restart still finds the committed
snapshot and replication still finds its durable work item. A handover that
wins CAS prevents the former owner from exposing a snapshot.

Iceberg metadata, manifests and version hints use request-scoped immutable
publication paths. Metadata includes only the current snapshot, so historical
references cannot escape the descriptor's verified object set. Avro sync
markers are deterministic to preserve identical immutable retries. Delta
publications are standalone version-zero logs with absolute S3 references to
the verified data files, rather than relocated relative paths into old logs.
These paths need actual Fabric compatibility evidence before exit.

The catalog keeps one CAS record per dataset. Owner ID, owner location,
monotonically increasing fence, expiry, generation identities and complete
replica pointers are committed in that record. Handover, expired-owner takeover,
renewal and advertisement all use CAS. An old owner cannot overwrite a handover
that wins the race. A caller must reload after a CAS conflict, not retry using
an old fence.

Each generation descriptor contains store ID, key, size and SHA-256 for every
object and its generation manifest, plus sequence and production timestamp.
The regional verifier hashes every object's streamed bytes, even if a store's
metadata reports a matching digest. It also checks the manifest bytes. The manifest
must describe exactly the descriptor's objects. Replication copies immutable
objects first and the manifest last, then verifies the destination. A failed
copy does not change the catalog's previous pointer.

`FSP_FEDERATION_REPLICA_STORES` wires the Manager's reconciliation loop to
declared Azure replica profiles with managed/workload identity. The catalog's
primary and replica pointers are the durable work list; restart re-verifies
immutable copies and advances only complete replicas under the original owner
fence. The loop logs storage failures and retries on the renewal cadence.
HA standby Managers do not initiate reconciliation or owner renewal.
Managers still publish to the policy's one primary staging/published store.
Changing that physical primary during handover is not implemented.

`python -m enterprise.regional_serving` runs a database-free S3 reader. Its
bucket-to-dataset assignments and endpoint/store binding are checked at startup
without relaxing work-queue dispatch restrictions. GET, HEAD, Range and listing
read one catalog-selected regional descriptor per request; unadvertised objects
are not exposed. SigV4 and the existing access-key scope checks are required.
The serving workload has no source Secret and needs a separate Blob Data Reader
identity for local publications and catalog records. The Helm workload and
network policy are disabled by default. TLS ingress must be configured separately.

Reads verify the committed object's size and SHA-256 before serving. This
currently incurs a full integrity read even for Range and HEAD, followed by the
requested read; account for that Blob traffic before production rollout.
Federated staging pruning is disabled because a lagging region can still need
an older generation. Retention/garbage collection requires a catalog-aware
reference policy; do not remove prior publication objects manually during drills.

The catalog receives only names, sizes, digests, generation timestamps and
verification receipts, never the source rows or table-metadata body. Receipts are sealed with a process-local ephemeral key to reject accidental
construction or modification without verification. Verification and
advertisement intentionally run in the same regional Manager process; receipts
are not transported to the catalog HTTP service. A future independently
deployed replication worker still needs authenticated portable receipts.

`resolve` requires an assigned endpoint and returns that endpoint's last
complete generation. A lagging region does not borrow a newer generation from
another region. Staleness is calculated from the generation's production time
and the dataset freshness target, not the replication time or owner lease.
Expiry does not delete the last published objects.

Object keys must remain immutable and store identities must not be reassigned
to different physical stores. The Phase 4 infrastructure plan adds a private
RA-GZRS StorageV2 account and Blob container for catalog records, with private
endpoints in both Manager VNets and identity-based container access. This
provides geo-redundant recovery, not active-active Blob writes; storage failover
and its RPO must be included in the regional recovery runbook.

## Disconnected pools

Pools can set positive integer `heartbeat_ms`, `heartbeat_miss_limit` and
`claim_lease_seconds`. Unset values retain the Manager/queue defaults.
Registration advertises the pool heartbeat interval. Liveness and the
Prometheus heartbeat timeout use the pool's miss budget.

Claims persist their chosen lease duration. Heartbeat renewal preserves that
duration and caps expiry at the request deadline. A heartbeat arriving after
claim expiry cannot revive the claim. Existing claim expiration/requeue logic
then handles reassignment. The heartbeat alert compares age against the
exported timeout rather than assuming a six-second limit for WAN pools.

The running work-queue publication path now persists `dataset_id` and
`produced_at_ms` in the durable snapshot manifest object. The production clock
uses the request's start time, a conservative lower bound on source acquisition,
not the publication, replication or heartbeat time. Retrying publication cannot
refresh that timestamp, and supplied timestamps or dataset IDs must match the
request. These fields describe generation metadata; they do not add provider
headers to each Parquet object or claim a database-level consistent read point.
Older manifests still deserialize with absent fields.

On every Prometheus scrape, the Manager reads publication metadata and
recalculates configured dataset freshness. It does not download source rows or
Parquet objects. The same status is returned by `GET /control/work-queue` under
`dataset_freshness`. Metrics are labelled by `dataset_id` and `location`:

- `fsp_dataset_age_seconds`
- `fsp_dataset_freshness_target_seconds`
- `fsp_dataset_stale`
- `fsp_dataset_production_timestamp_known`

A missing, legacy or future-dated production timestamp is explicitly unknown:
the timestamp-known gauge is zero, the age series is absent and stale is one.
A known generation becomes stale when its age exceeds the target. Retired
targets remove their metric series. No target preserves the existing behavior.
`FspDatasetFreshnessMissed` in the existing observability rule set alerts after
30 seconds of staleness. Freshness status and alerts never delete, expire or
invalidate the last published objects. Operators must configure regular
Prometheus scrapes and apply the rule set.

Tests exercise the actual Manager metrics/status endpoints, restart and late
republication, the exact freshness threshold, metadata-only monitoring and
retaining readable published bytes during a simulated site outage. This is not
a live multi-region site-disconnection drill. Additional signed regional S3
tests exercise last-good reads, replication catch-up and corruption rejection.

## Acceptance boundaries

| Issue | Implemented and tested here | Remaining exit evidence or integration |
|---|---|---|
| #142 | Metadata-only CAS catalog, authenticated service/API, Helm workload and private RA-GZRS infrastructure are implemented and tested locally. | Deploy both regional instances and execute storage failover/recovery evidence. |
| #143 | Catalog CAS is the sole federated snapshot visibility commit. A concurrent handover denies publication without exposing a local snapshot. Tests cover both owner directions, expiry and post-commit bookkeeping failure. | Deploy the second Manager against the declared primary publication store and execute live bidirectional handover. Physical primary-store relocation is not implemented. |
| #144 / #171 | S3/GCS data stores, common data-plane/CAS conformance and a cloud-SDK CI matrix. | Production AWS/GCP integration and live provider conformance are deferred beyond v3.1.0 to #171. Cross-object control mutations are deliberately unsupported. |
| #145 | Entra managed identity; additive VM VNet peerings and Blob private DNS link; container-scoped staging identity; packaged-checkout installer with private DNS/identity/readiness preflight and async SQLite. | Assign the API app role, attach identity, deploy the Azure-IaaS Agent and capture authentication and negative-scope evidence. Trusted mutual-TLS certificate-to-pool fallback remains unimplemented. |
| #146 | Whole-chain declaration validation, inherited constraints and dispatch rejection; regional reader binds its local profile/provider/endpoint without enabling cross-store dispatch. | Deployed IAM/location/cache and no-row-egress evidence. Multi-store materialization dispatch is not implemented. |
| #147 | Durable asynchronous Manager reconciliation, immutable copying, verified advertisement, restart/catch-up tests, signed regional S3 GET/HEAD/Range/list reads, deployed replica/reader identities and TLS endpoints, and actual Fabric reads through both regional endpoints. | Capture replication timing under the agreed acceptance load. Portable receipts are needed only if verification moves to an independent worker. |
| #148 | Per-pool renewal, durable production timestamps, running Manager freshness/status/alerts, catalog-backed serving and tested retention of the prior replica during copy failure. | Deployed alert validation and a live site-disconnection/reconnection drill. |

Phase 4 is not at exit criteria until the Azure resources and workloads above
are deployed and exercised. No region-loss RTO is claimed, the Azure-IaaS
simulated on-premises workload has not yet produced live authentication
evidence, and replication timing has not yet been accepted. Production
AWS/GCP support is outside Phase 4 and v3.1.0 and is tracked by #171.

On 2026-10-12, Fabric workspace `Demo_Dev`, lakehouse `bronze`, read the same
92,314-byte Parquet object through `https://fsp-swc.x-dev.online` and
`https://fsp-neu.x-dev.online`. Both reads matched catalog SHA-256
`b5e731463c3ea890b692d55c805e3bdaccec9318886d35d16c2b683ed5864dd1`.
The regional ingress logs recorded Fabric service source `20.240.128.211`
issuing successful HEAD and GET requests to each endpoint. This completes the
regional Fabric-read evidence but does not substitute for the remaining
handover, region-loss, disconnection, or replication-timing drills.

The renewal-startup, expired-owner and immutable Avro retry defects were
reproduced by failing tests before implementation. Local acceptance tests cover
CAS handover races, queue restart, post-commit bookkeeping failure, retained
staging, verified replication recovery, immutable Iceberg/Delta references,
signed S3 reads, corrupt-replica rejection and Helm rendering. These results
are local evidence, not deployed acceptance.
The integrated full regression passed with **1,527 passed and 53 skipped**.
After the final reader workload-identity fallback and lifecycle ordering
changes, focused runtime, residency, identity, hybrid automation and Helm
coverage passed with **102 passed and 1 skipped**. Ruff's `F` checks and
`git diff --check` passed. Hybrid Bicep compilation, Bash syntax and PowerShell
parsing passed; Bicep retains a `no-hardcoded-env-urls` warning. No security
scan of the final infrastructure changes or live acceptance result is claimed.
The Azure deployment approval prompt received no user response; the local
infrastructure plan is draft and no live deployment was performed.

## Deployment acceptance sequence

1. Review the additive infrastructure what-if and acknowledge deployment risks.
   Apply private catalog/VM connectivity and container-scoped identities.
2. Assign `FSP.Agent` to the hybrid managed identity and attach it to the existing
   VM. Package this checkout with the hybrid installer; unpushed code is not
   assumed to exist at a Git ref. Require private Blob DNS and Manager registration.
3. Deploy both Managers, each configured with the declared primary published
   profile, and the replica store map. Deploy each reader with its own local
   profile and read-only identity. Verify unexpected source/store access is denied.
4. Publish a generation, record descriptor hashes and both replica pointers,
   and issue real Fabric reads through each assigned HTTPS endpoint. Do not
   substitute a signed test-client request for Fabric evidence.
5. Handover in both directions while publication is in flight. Confirm the old
   owner cannot advance any visible pointer and time recovery to the agreed RTO.
6. Disconnect the hybrid site, then the replica writer. Confirm the last complete
   regional generation remains readable, the exact freshness budget is exceeded,
   and `FspDatasetFreshnessMissed` fires after its configured 30-second hold.
   Restore connectivity and measure catch-up without moving a pointer backwards.
7. Restart catalog/Managers during interrupted copying, then exercise catalog
   geo-recovery. Record measured RPO/RTO and integrity checks, not assumed RA-GZRS
   guarantees. Capture mutual-TLS fallback evidence once that path is implemented.
