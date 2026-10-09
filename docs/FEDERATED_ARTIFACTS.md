# Federated artifact primitives

These are the first implementation pieces for Phase 4, tracked by #141.
They do not enable the federated production topology. The existing Manager,
work queue and serving path remain the default.

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
physical location of a volume, or relocate an Agent's cache. Those deployment
and serving integrations remain required for #146.

## Catalog and regional verification

[federation.py](../enterprise/control/federation.py) separates a metadata-only
`GlobalCatalog` from the `RegionalReplicaVerifier`. Neither is mounted as a
production HTTP service or attached to the default publication path yet.

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

The catalog receives only names, sizes, digests, generation timestamps and
verification receipts, never the source rows or table-metadata body. Receipts
are sealed with a process-local ephemeral key to reject accidental construction
or modification without verification. They are not portable between processes
or restarts and are not a cross-cloud identity protocol. Authenticated,
pool-bound receipt transport must be implemented before splitting the regional
verifier and catalog into independently deployed services.

`resolve` requires an assigned endpoint and returns that endpoint's last
complete generation. A lagging region does not borrow a newer generation from
another region. Staleness is calculated from the generation's production time
and the dataset freshness target, not the replication time or owner lease.
Expiry does not delete the last published objects.

Object keys must remain immutable and store identities must not be reassigned
to different physical stores. Catalog state needs a separately selected
geo-redundant deployment and recovery model. Merely constructing the catalog
with a local store does not provide regional disaster recovery.

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

## Acceptance boundaries

| Issue | Implemented and tested here | Remaining exit evidence or integration |
|---|---|---|
| #142 | Metadata-only CAS catalog record and one-owner publication races. | Catalog service, authenticated API and geo-redundant deployment/recovery. |
| #143 | Catalog handover in both directions and expired-owner takeover, with stale-fence rejection. | Regional Manager/queue publication integration, second regional Manager and live bidirectional handover. |
| #144 | S3/GCS data stores, common data-plane/CAS conformance and a cloud-SDK CI matrix. | Live provider conformance and integration with regional store profiles. Cross-object control mutations are deliberately unsupported. |
| #145 | SDK identity-chain factories; existing Phase 3 Entra Agent authentication remains unchanged. | EKS/GKE/Arc federation, issuer-less mTLS fallback, prefix-scoped short-lived on-prem credentials and IAM-negative tests. |
| #146 | Declared whole-chain validation, inherited constraints and apply/request/dispatch rejection. | Physical serving/cache bindings, multi-store dispatch and live no-row-egress evidence. |
| #147 | Manifest/object verification, immutable copying and retaining a lagging region's complete generation. | Production asynchronous replication worker, catalog receipt transport and actual Fabric regional reads. |
| #148 | Per-pool timeout/renewal behavior and catalog freshness without deleting the previous generation. | Production timestamp metadata, periodic freshness metrics/alerts and a live site-disconnection drill. |

Phase 4 is not at exit criteria. No region-loss RTO is claimed, no EKS/GKE/Arc
workload has been authenticated, and no Fabric regional-read test has been
executed by this change. The existing Phase 3 AKS workloads and DNS are unchanged.
