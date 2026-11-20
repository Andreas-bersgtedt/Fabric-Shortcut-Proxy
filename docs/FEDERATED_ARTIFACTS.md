# Federated publication runtime

Phase 4 is tracked by #141. Catalog-backed publication, verified replication
and regional S3 serving are opt-in runtime paths. The Azure/Fabric acceptance
topology was deployed and exercised on 2026-10-12. Existing non-federated
behavior remains the default.

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
protection as other data caches. A non-empty `TMPDIR` is used directly;
an inaccessible directory fails the upload instead of falling back elsewhere.
The spool file remains visible during the upload so kubelet directory scans can
account for it. It is removed when closed normally, but abrupt process
termination can leave it behind. RAM use is bounded by the stream and upload
chunks, not total object size.

The Helm Manager can opt into a writable, size-limited `emptyDir` by setting
`federation.replicaUploadSpool.enabled` and a positive whole-number `sizeLimit`
such as `20Gi`. The spool uses node ephemeral storage, so its placement and
encryption must satisfy the site's residency policy. Size it for concurrent
uploads and set appropriate ephemeral-storage requests and limits in
`manager.resources`. Verify that the node supports ephemeral-storage
measurement and monitor disk usage: kubelet accounting and eviction are not a
hard disk quota. `emptyDir` is removed with the Pod but does not guarantee
secure erasure.

S3 uses conditional multipart completion. Failed multipart uploads are
aborted; abort failures are logged and the original upload failure propagates.
GCS uses generation preconditions and resumable uploads. Identical immutable
retries succeed; different bytes at the same key raise `ObjectConflict`.
S3 and GCS verification compare object size and the SHA-256 metadata recorded
at upload; they do not rehash the object on every verification. Restrict writes
to both object data and metadata.

S3 and GCS are **not** added to `ARTIFACT_STORE_BACKEND`. The current Manager
requires cross-object fenced writes and guarded batches. An independent lease
check followed by an S3/GCS object write would not provide those guarantees.
Unsupported control mutations therefore raise `NotImplementedError`, rather
than falling back to an unsafe write. Single-object CAS is suitable for the
catalog record described below; it is not a general multi-object transaction.

[test_cloud_artifact_conformance.py](../tests/test_cloud_artifact_conformance.py)
runs the same data-plane/CAS checks against memory, local, Azure Blob, S3 and
GCS implementations. Default cloud tests use SDK doubles, not live services.
The dedicated CI matrix installs all three cloud SDK extras on Python 3.11
and 3.12 and tests regional replication and serving with doubles. Separate
backend tests cover multipart cleanup, reader bounds, response closure, native
SDK preconditions and error propagation.

### Opt-in live conformance

The same shared checks can run against live Blob, S3, or GCS. They have not
been run for this source patch. Use dedicated non-production containers or
buckets and an identity allowed to list, read, write, and delete within a test
prefix. Never point the gate at production data. Each test creates a random
subprefix and deletes only objects under that subprefix, including Azure's
mutation lock and uncommitted blocks. A failed cleanup fails the test and
reports its prefix. Interruptions can leave objects behind; inspect the test
prefix afterward. The large-object immutable retry test attempts three uploads
of just over 8 MiB per provider. Bucket versioning, soft delete, legal holds, and retention
policies can preserve deleted data. The gate does not delete versions or
override those policies.

Install the `azureblob`, `s3-artifacts`, and `gcs-artifacts` extras for the
selected providers. Azure uses `DefaultAzureCredential`, S3 uses the AWS SDK
credential chain, and GCS uses Application Default Credentials. Do not put
credentials in these variables. Select only configured providers:

```powershell
$env:FSP_LIVE_ARTIFACT_PROVIDERS = "azure,s3,gcs"
$env:FSP_LIVE_ARTIFACT_PREFIX = "fsp-conformance"
$env:FSP_LIVE_AZURE_ACCOUNT_URL = "https://<test-account>.blob.core.windows.net"
$env:FSP_LIVE_AZURE_CONTAINER = "<dedicated-test-container>"
$env:FSP_LIVE_S3_BUCKET = "<dedicated-test-bucket>"
$env:FSP_LIVE_S3_REGION = "<test-region>"
$env:FSP_LIVE_GCS_BUCKET = "<dedicated-test-bucket>"
$env:FSP_RUN_LIVE_GCP_GATES = "1"
.\.venv\Scripts\python.exe -m pytest tests\test_cloud_artifact_conformance.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_integration_gcp.py -q
Remove-Item Env:\FSP_LIVE_ARTIFACT_PROVIDERS
```

The prefix must be a non-root path of letters, digits, underscores, hyphens,
and optional internal slashes; do not end it with a slash. Leave the provider
variable unset for ordinary local and CI tests. These checks do not replace
workload identity, IAM isolation, regional Fabric reads, or failover drills.

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
These paths need actual Fabric compatibility evidence before production promotion.

The catalog keeps one CAS record per dataset. Owner ID, owner location,
monotonically increasing fence, expiry, generation identities and complete
replica pointers are committed in that record. Handover, expired-owner takeover,
renewal and advertisement all use CAS. An old owner cannot overwrite a handover
that wins the race. A caller must reload after a CAS conflict, not retry using
an old fence.

Each generation descriptor contains store ID, key, size and SHA-256 for every
object and its generation manifest, plus sequence and production timestamp.
The regional verifier calls the store's verification method for each object.
Azure, S3, and GCS compare size and stored SHA-256 metadata; the default local
store implementation hashes streamed bytes. The verifier also hashes the
bounded manifest bytes, which must describe exactly the descriptor's objects.
Replication copies immutable
objects first and the manifest last, then verifies the destination. A failed
copy does not change the catalog's previous pointer.

## Cross-cloud beta release gate

The beta is for opt-in evaluation with non-production data: the Manager
replicates to S3 or GCS, and a regional reader serves verified replicas through
the SigV4 API. The primary publication store and catalog stay on Azure.

Release as beta when all of the following pass on the release candidate:

- Offline cloud SDK contract tests pass for both providers.
- Replication and regional serving regressions pass, including interrupted
  copies that leave the prior complete replica advertised and signed reads of
  verified replicas.
- Helm lint and render tests pass for both providers, including opt-in
  configuration, profile validation and the Manager upload spool.
- Operator docs and release notes identify the opt-in settings, image
  requirements, and limits of the beta.

Live provider conformance, EKS/GKE workload identity, pool-scoped IAM,
Fabric shortcut reads, residency, egress, and failover drills do not block
this beta. They still block production promotion under [#144][issue-144]
and [#171][issue-171]. SDK-double and chart tests do not certify a live
deployment. S3/GCS cannot act as the Manager control store.

On 2026-11-19, the GCS part of issue #144 reached live conformance evidence:
the shared live artifact conformance suite passed 8 GCS variants, the GCS mount
integration gate passed read/write, range, size, SHA-256, conflict, prefix
isolation, and denied-404 checks, and the cloud regional environment test passed
GCS replication and signed regional-read behavior. The dedicated GCS bucket was
deleted after the evidence run. This completes the GCS provider/store portion,
not the issue-wide live Blob/S3 provider conformance or production promotion
work.

### GKE/GCS live proof 2026-11-20

Issue #182 used a short-lived GKE Standard zonal UAT topology in
`europe-west1-b`. The cluster ran Kubernetes `1.36.4-gke.2377000` with one
Spot `e2-medium` node and a 20 GiB standard persistent disk. Workload Identity
was enabled with workload pool
`project-86992797-5864-4dd6-901.svc.id.goog`. The dedicated GCS bucket
`fsp182-gke-gcs-project-86992797-5864-4dd6-901` was in `EUROPE-WEST1`.

The live proof used Kubernetes service accounts `fsp182-manager` and
`fsp182-reader`, backed by Google service account
`fsp182-gke@project-86992797-5864-4dd6-901.iam.gserviceaccount.com`.
The GSA received `roles/storage.objectAdmin` on the bucket and
`roles/storage.legacyBucketReader` for bucket metadata reads. The Manager and
reader pod identities were able to refresh GCS credentials through GKE metadata
and read the catalog and replica prefixes.

The catalog contained `erp::sales` owned by `manager-gke` in
`gcp:europe-west1`. One 802-byte Parquet fixture named
`published/1/fsp182-gke-gcs.parquet` was published and replicated under
`fsp-replica/`. The replica list contained the data object, generation snapshot,
and manifest. The live catalog resolve returned the GCS replica generation.

Signed regional-reader checks passed:

- GET returned 200 and 802 bytes.
- HEAD returned 200 and content length 802.
- Range `bytes=0-3` returned 206 and `PAR1`.
- An unsigned GET returned 403.
- A valid key with the wrong secret returned 403.
- An out-of-prefix object and an unadvertised generation returned 404.
- PUT and DELETE returned 403.

Deleting and recreating the regional-reader Pod preserved GCS catalog and
replica access. Residency remained in `gcp:europe-west1`; no Cloud NAT,
persistent external Load Balancer, Cloud CDN, Cloud Armor, Cloud logging or
Cloud monitoring resources were provisioned for the proof.

The GKE cluster, GCS bucket, catalog and replica objects, GSA, Artifact Registry
repository, namespace, PVC and Cloud Build staging bucket were deleted after the
evidence run. The billing project remains enabled, but the available CLI session
could not read the billing account's total. A direct console cost query returned
`£0.00` for 1-20 November 2026, against `£225.52` free-trial credit and 89 days
remaining. This is below the £50 incremental-spend guardrail, but it is a console
read rather than a billing-export reconciliation.

The live GCS and WIF proof was recreated on 2026-11-20 after the Fabric session
became authenticated. The recreated cluster used Kubernetes
`1.37.1-gke.2097000`, and the reader served a 797-byte Parquet fixture.
GET, HEAD, Range, wrong-secret, unsigned, unadvertised-key and mutation-negative
checks passed. The recreated GCS, cluster and registry resources were deleted
after this pass.

The Fabric capacity became active during the final proof. The existing
`Issue_11_FSP_test` Lakehouse remained accessible and returned HTTP 200 through
the Fabric Lakehouse REST API. The remaining live shortcut gate is HMAC key
creation: GSA HMAC creation failed under
`constraints/iam.disableServiceAccountKeyCreation`, and no HMAC key exists.
The Fabric connection list contains no GCS connection that can replace the
missing credential.

Issue #182 therefore has live GKE, Workload Identity, GCS catalog/replica,
SigV4, negative-IAM, residency, reader-restart, active-capacity and billing
evidence. The live Fabric GCS shortcut read remains blocked by the organization
service-account key policy.

[issue-144]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/144
[issue-171]: https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/171

### Replica configuration

`FSP_FEDERATION_REPLICA_STORES` wires the Manager's reconciliation loop to
declared Azure, S3, or GCS replica profiles. Azure profiles use managed/workload
identity. S3/GCS profiles accept only `bucket`, optional `prefix`, and optional
S3 `region`; their SDK credential chains must be configured separately. The
catalog's primary and replica pointers are the durable work list; restart re-verifies
immutable copies and advances only complete replicas under the original owner
fence. The loop logs storage failures and retries on the renewal cadence.
HA standby Managers do not initiate reconciliation or owner renewal.
Managers still publish to the policy's one primary staging/published store.
Changing that physical primary during handover is not implemented. S3/GCS
replica and reader wiring is [beta (unreleased)](#cross-cloud-beta-release-gate).

For example, after declaring `aws-replica` and `gcp-replica` with their
providers and locations in placement, a Manager can set:

```powershell
$env:FSP_FEDERATION_REPLICA_STORES = '{"aws-replica":{"bucket":"<s3-bucket>","prefix":"fsp-replica","region":"eu-west-1"},"gcp-replica":{"bucket":"<gcs-bucket>","prefix":"fsp-replica"}}'
```

The policies must name those profiles as replicas and allow their exact
locations. The Manager still uses its Azure or local fenced control store;
`ARTIFACT_STORE_BACKEND` does not accept `s3` or `gcs`.

`python -m enterprise.regional_serving` runs a database-free S3 reader. Its
bucket-to-dataset assignments and endpoint/store binding are checked at startup
without relaxing work-queue dispatch restrictions. GET, HEAD, Range and listing
read one catalog-selected regional descriptor per request; unadvertised objects
are not exposed. SigV4 and the existing access-key scope checks are required.
The serving workload has no source Secret. An Azure reader needs a separate
Blob Data Reader identity for local publications and catalog records. The Helm
regional reader is disabled by default and supports Azure, S3, or GCS. For S3
or GCS, set `federation.regionalServing.cloudStore` to the replica bucket and
prefix, with `region` for S3 only; leave the Azure regional artifact-store
settings empty. Enable `workloadIdentity.enabled`, configure the Azure catalog
identity, and use `serviceAccountAnnotations` for provider identity annotations
as needed.
Cloud roles, federated trust, identity webhooks and egress must be provisioned
separately. TLS ingress must be configured separately.

For example, the S3-specific portion of a regional reader's Helm values can be:

```yaml
federation:
  regionalServing:
    cloudStore:
      bucket: "<replica-bucket>"
      prefix: "fsp-replica"
      region: "eu-west-1"
    serviceAccountAnnotations:
      eks.amazonaws.com/role-arn: "arn:aws:iam::<account-id>:role/<reader-role>"
```

For GKE, omit `region` and use the `iam.gke.io/gcp-service-account`
annotation if the workload uses service-account impersonation. These settings
do not grant permissions or configure the Azure catalog identity.

For an S3/GCS reader running outside Helm, set
`FSP_ARTIFACT_STORE_PROFILE` and `FSP_REGIONAL_SERVING_ENDPOINT_ID` to the
matching regional profile and endpoint, and set
`FSP_REGIONAL_SERVING_CLOUD_STORE` to the same bucket, prefix, and S3 region
used by the Manager for that replica. Configure
`FSP_CATALOG_STORE_ACCOUNT_URL` and `FSP_CATALOG_STORE_CONTAINER` for the
shared Azure catalog, its Azure identity, and
`FSP_REGIONAL_SERVING_BUCKETS` for the bucket-to-dataset mapping. Do not set
remote `FSP_FEDERATION_REPLICA_STORES` on a reader. A missing cloud profile
or shared catalog fails startup. Install the matching cloud SDK extra in both
Manager and reader environments. The default Python image does not include
the Enterprise package or S3/GCS artifact extras; see the
[Helm image build example](../deploy/helm/fabric-shortcut-proxy/README.md#prerequisites).

Reads verify the committed object's size and SHA-256 before serving. Azure, S3,
and GCS use object properties and stored digest metadata for this check; local
stores may stream the object to verify it. Cloud Range and HEAD requests do not
perform a separate full-object integrity read.
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
| #142 | Metadata-only CAS catalog, authenticated service/API, Helm workload and private RA-GZRS infrastructure are deployed. A planned Sweden Central→Sweden South failover and planned failback preserved the exact 12,478-byte catalog hash, generation pointers and fencing. Private Endpoints were recreated after each swap, both Fabric shortcuts were verified, RA-GZRS was restored in Sweden Central, and stale-owner renewal/publication remained rejected at fence 6. | Acceptance criteria complete. |
| #143 | Catalog CAS is the sole federated snapshot visibility commit. Live bidirectional handover advanced fences 1→2→3, the North Europe owner published generation 6, and its stale renewal and publication authority were rejected after handback. Tests also cover both owner directions, expiry and post-commit bookkeeping failure. | Physical primary-store relocation is not implemented. |
| #144 / #171 / #182 / #183 | GCS live shared-conformance and mount conformance passed in 3.2.1 UAT. GCS regional replication/serving integration passed the SDK-backed environment test. Recreated GKE/GCS proof passed catalog, replica, signed-reader and negative-IAM checks on 2026-11-20. Live S3/Blob provider evidence and AWS deployment remain. Cross-object S3/GCS control mutations are unsupported. | GCS provider/store evidence is complete. #171 remains the umbrella; #182 owns GCP/GKE/GCS and #183 owns AWS/EKS/S3. The live GKE/GCS proof is complete. The live Fabric shortcut read is blocked by inactive workspace capacity and the service-account key policy. |
| #145 | Entra managed identity; additive VM VNet peerings and Blob private DNS link; container-scoped staging identity; packaged-checkout installer with private DNS/identity/readiness preflight and async SQLite; deployed Azure-IaaS Agent authenticated with the `FSP.Agent` app role and no static shared secret; live proof that its identity can write/delete only in `fsp-hybrid-staging` while catalog, published and replica writes return 403; and a deployed mTLS fallback that re-verifies the CA-signed certificate at Manager, maps its SHA-256 fingerprint to `hybrid-site-01` and exactly the `hybrid-site` pool, while rejecting missing, unknown and Agent-mismatched certificates. | Acceptance criteria complete. |
| #146 | Whole-chain declaration validation, inherited constraints and dispatch rejection; regional reader binds its local profile/provider/endpoint without enabling cross-store dispatch; actual regional reader identities can read assigned objects/catalog but receive 403 on writes; catalog and reader workloads mount minimized Secrets with no `DB_URL`; NetworkPolicies expose no SQL egress; catalog storage contains JSON metadata plus an empty lease-lock blob and no Parquet; and a live out-of-boundary replica policy returns `residency_policy_violation`. | Multi-store materialization dispatch is not implemented, but it is outside the current single-primary routing contract. |
| #147 | Durable asynchronous Manager reconciliation, immutable copying, verified advertisement, restart/catch-up tests, signed regional S3 GET/HEAD/Range/list reads, deployed replica/reader identities and TLS endpoints, actual Fabric reads through both regional endpoints, a measured Sweden-loss recovery read in 23.478 seconds, and live proof that a lagging replica serves the older complete generation while the unadvertised generation returns 404. A 15-generation SQL-backed run completed with zero failures, verified every advertised object and manifest, exposed no intermediate pointer, and recorded a 15.310-second maximum against the 60-second SLO. | Acceptance criteria complete. Portable receipts are needed only if verification moves to an independent worker. |
| #148 | Per-pool renewal, durable production timestamps, running Manager freshness/status/alerts, catalog-backed serving, retained last-good reads during live replica disconnection, a deployed Prometheus alert firing after the configured threshold and hold, 0.720-second reconciliation catch-up after reconnect, a fresh recovery generation, and alert resolution. | Acceptance criteria complete; production monitoring ownership and alert routing remain an operational rollout decision. |

The deployed Azure/Fabric topology meets the Phase 4 exit criteria listed
above. Production AWS/GCP support is outside Phase 4 and v3.1.0 and is tracked
by #171.

On 2026-10-12, Fabric workspace `Demo_Dev`, lakehouse `bronze`, read the same
92,314-byte Parquet object through `https://fsp-swc.x-dev.online` and
`https://fsp-neu.x-dev.online`. Both reads matched catalog SHA-256
`b5e731463c3ea890b692d55c805e3bdaccec9318886d35d16c2b683ed5864dd1`.
The regional ingress logs recorded Fabric service source `20.240.128.211`
issuing successful HEAD and GET requests to each endpoint. This completes the
regional Fabric-read evidence but does not substitute for the remaining
disconnection and sustained replication-timing drills.

On 2026-10-12, ownership handed Sweden→North Europe in 11.273 seconds,
North Europe published generation 6, and ownership handed back under fence 3.
The former North Europe owner was rejected for both lease renewal and
publication authority. Ownership then moved to North Europe at fence 4 and the
Sweden AKS cluster was stopped. Fabric read the 92,314-byte generation-6 object
through the North Europe shortcut after 23.478 seconds, within the agreed
five-minute RTO. Sweden was restarted, ownership returned at fence 5, both
replica pointers matched generation 6, and both Fabric shortcuts returned the
catalog SHA-256. Materializers remain registered with their regional Managers;
this avoids cross-region dependency on public load-balancer hairpin routing.

On 2026-10-12, generation 8 was fully replicated and the freshness alert was
inactive. Sweden Manager egress to the North Europe replica endpoint was then
removed and generation 9 published only to `published`; the `replica` pointer
remained on generation 8. Fabric continued reading the 92,314-byte generation-8
object through North Europe with the catalog SHA-256, while the generation-9
key returned 404. With a temporary 60-second target,
`FspDatasetFreshnessMissed` became pending and then firing after its 30-second
hold. Restoring egress advanced the replica pointer to generation 9 in 0.720
seconds after the Manager rollout; generation 10 then published and replicated,
Fabric read it through North Europe, and the alert resolved to inactive. The
temporary Prometheus/Alertmanager release, copied auth Secret and monitoring
namespace were removed; the 300-second target and replica egress were restored.

On 2026-10-12, the deployed Sweden and North Europe reader identities read
their assigned generation-10 object and catalog record, then received Azure
403 responses for attempted writes. Their broad `fsp-source` mounts were
replaced with S3-only reader Secrets; the catalog uses an operator-auth-only
Secret. Neither workload contains `DB_URL`, and their egress policies permit
only private Blob endpoints and Entra HTTPS ranges, not SQL. The catalog held
one 12,478-byte JSON dataset record plus the empty artifact-store lease lock,
with no Parquet bytes. Mutating the deployed policy model so the replica moved
to `aws:us-east-1` raised `residency_policy_violation`.

The private Azure-IaaS VM identity received the `FSP.Agent` application role
and registered `hybrid-site-01` into exactly the `hybrid-site` pool through
Entra. Its service environment contains no Agent token, client secret or S3
static credential. Using an IMDS token, it listed, wrote and deleted an object
in `fsp-hybrid-staging` (200/201/202) while writes to catalog, published and
replica returned 403. Credentials exposed during validation were rotated in
both clusters and both encrypted Fabric gateway connections; both Fabric
shortcuts were revalidated after rotation.

The same hybrid site also exercised the optional mTLS fallback while Entra
remained the preferred, continuously heartbeating path. ingress-nginx validated
a short-lived client certificate against `fsp-agent-client-ca` and forwarded
it to Manager; Manager independently revalidated its CA signature and validity,
computed fingerprint
`a96f55870e5f24e09f8355712755fc40b4958f2fd2f5ce338731b5757894bead`,
and mapped it to `hybrid-site-01`. A mapped certificate with matching Agent ID
reached the materialization handler (404 for the deliberately unknown key).
No certificate, the mapped certificate with a different Agent ID, and an
unknown CA-signed certificate each returned 401. The normal Entra service
remained active with 200 heartbeats. Local CA private and unknown-client key
material were deleted after the drill.

On 2026-10-12, both Managers were quiesced and Azure last-sync advanced 43.521
seconds beyond the final write. Planned failover moved the RA-GZRS account from
Sweden Central to Sweden South in 1,037.050 seconds. The storage control plane
remained healthy, but Private Endpoints had to be recreated against the new
primary; end-to-end reader/Fabric recovery took 1,392.891 seconds. The catalog
hash remained
`cb758c7ecbb03beebcc28364e0889af424701e911ce29a7c4832c9c7e6610e4b`.
Planned failback completed in 794.844 seconds, and Private Endpoint recreation
restored both regional readers after 1,261.414 seconds. The account returned to
`Standard_RAGZRS` with Sweden Central primary and Sweden South secondary,
both regions available and failover-ready. Managers resumed with
`manager-swc` as the single owner at fence 6; the stale North Europe owner was
rejected for renewal and publication, and both Fabric shortcuts returned the
generation-10 object hash.

On 2026-10-12, the sustained replication run published 15 consecutive
SQL-backed generations in 316.657 seconds, or 2.842 generations per minute.
Replication latency was 2.065 seconds minimum, 11.338 seconds median, 15.310
seconds p95 and 15.310 seconds maximum against a 60-second SLO. Every replica
pointer advance was followed by streaming SHA-256 and size verification of
all three advertised objects plus exact manifest verification. The run
recorded zero failures and zero unexpected intermediate pointers. Two defects
found by earlier load attempts were fixed before the accepted run: publication
now retains each split's canonical object key instead of nesting prior staging
paths, and queue read-after-write verification retries transient stale reads
for up to 200 milliseconds. Both regional workloads ran image digest
`sha256:3bfc303585f57e7b7e904573ae65a043f5cd493893c3cbb70245c2385d36ccc5`.
After the run, both Fabric shortcuts returned the current generation-39
92,314-byte Parquet object with SHA-256
`b5e731463c3ea890b692d55c805e3bdaccec9318886d35d16c2b683ed5864dd1`.
The hybrid Agent remained active with an 8.0-second heartbeat age, and ACR
public network access was disabled with the default network action set to
deny.

The renewal-startup, expired-owner and immutable Avro retry defects were
reproduced by failing tests before implementation. Local acceptance tests cover
CAS handover races, queue restart, post-commit bookkeeping failure, retained
staging, verified replication recovery, immutable Iceberg/Delta references,
signed S3 reads, corrupt-replica rejection and Helm rendering. These results
are local evidence, not deployed acceptance.
The final integrated regression passed with **1,535 passed and 53 skipped**.
After the final reader workload-identity fallback and lifecycle ordering
changes, focused runtime, residency, identity, hybrid automation and Helm
coverage passed with **102 passed and 1 skipped**. Ruff's `F` checks and
`git diff --check` passed. Hybrid Bicep compilation, Bash syntax and PowerShell
parsing passed; Bicep retains a `no-hardcoded-env-urls` warning. No security
scan of the final infrastructure changes is claimed. CI-equivalent Ruff `F` checks and both regional Helm lints also passed. Live
Azure deployment, regional Fabric reads, bidirectional handover, Sweden-loss
recovery, hybrid authentication, negative-scope, disconnection and sustained
replication timing evidence are recorded above.

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
