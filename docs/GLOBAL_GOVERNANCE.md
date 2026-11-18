# Global governance and operations

Phase 5 is tracked by #149. The implementation is opt-in and keeps legacy
single-tenant placement, unrestricted Agent release strings, and inline
placement JSON when the new policy fields are absent.

Phase 5 covers signed policy bundles, tenant isolation, measured egress
reporting, Agent release rings, and recovery drills, with an estimated effort
of 3 to 5 engineer-weeks. The edge Manager and local journal in #154 are
separate backlog and are not v3.1.0 exit criteria.

## Signed policy bundles

Regional Managers can load an Ed25519-signed placement bundle from HTTPS or a
private Azure Blob using managed identity or workload identity. Manager
verifies the signature, pinned key ID, complete placement policy, monotonic
revision, and immutable revision content before replacing its persistent
cache.

Connection failures, timeouts, HTTP 408/429, HTTP 5xx, or a missing private
Blob use the verified last-known-good cache. HTTP 4xx, invalid signatures,
unsigned bundles, invalid placement, rollback, and revision reuse with
different content fail startup. The Helm chart mounts only the public key.

## Tenant boundary

A configured tenant owns each pool, connection, store profile, serving
endpoint, dataset task, manifest, catalog record, Agent registry record, and
regional access audit. Table policies inherit their connection tenant.
Cross-tenant pool, store, replica, cache, and serving references fail policy
validation.

Scheduler validates the durable task tenant before contract, pool, location,
storage, fallback, or capacity eligibility. A mismatch remains queued with
`tenant_isolation_violation`. The audit API supports exact-tenant export:

```text
GET /_config/api/audit?tenant_id=<tenant-id>
```

## Measured egress reports

Accepted materializer results record measured bytes when the pool and store
locations differ. Replication records the verified bytes for every copied
object and manifest. Immutable event IDs make Agent retries and reconciliation
idempotent.

Monthly reports group integer bytes by tenant, dataset, pool, source region,
destination region, and direction:

```text
GET /control/egress-report?month=YYYY-MM
```

Pricing is optional operator input in currency units per GiB for an exact
`source->destination` boundary. Reports keep `priced_bytes` and
`unpriced_bytes` separate so missing rates cannot hide traffic.

## Agent release rings

An optional policy declares current and previous major/minor Agent release
lines and an ordered set of rollout rings. Pool membership and ring are
operator-owned. An unsupported release or a release not enabled for its pool
returns HTTP 409 with `agent_version_unsupported` before registry mutation.
Tasks remain queued. Durable registry restore rechecks the current policy and
drops registrations that are no longer allowed.

## Live acceptance on 2026-10-13

Both Azure regional Managers loaded signed revision 1 for tenant `demo-dev`
from private Blob using workload identity. Live negative checks returned
`tenant_isolation_violation` and `agent_version_unsupported`. A 15-generation
SQL-backed run completed with zero failures and zero intermediate pointer
exposure; replication p95 and maximum were 16.374 seconds against the
60-second SLO.

The global policy blobs were then removed and both Managers restarted.
Sweden Central became ready in 22.283 seconds and North Europe in 55.622
seconds. Both reported revision 1 from verified regional caches. While the
global source remained absent, generation 56 published and replicated in
22.735 seconds. Both Fabric shortcuts returned the 92,314-byte Parquet object
with SHA-256
`b5e731463c3ea890b692d55c805e3bdaccec9318886d35d16c2b683ed5864dd1`.

The October report contained 1,728,642 measured cross-boundary bytes for
`demo-dev`: 276,942 materializer-upload bytes and 1,451,700 replication bytes.
No prices were configured, so all bytes remained explicitly unpriced.

The policy blobs were restored and both Managers restarted in remote-backed
mode. All regional workloads finished on image digest
`sha256:82e657a33d2dc289e553f19b04b71ce4631fba6c06f97666036b114e1cab4f06`.
Both Fabric shortcuts returned the expected object after the final rollout.
ACR public access remained disabled with default action `Deny`. The signing
private key was stored in the existing FSP Key Vault, the temporary data-plane
role was removed, and local private key material was deleted.

The Phase 4 region-loss drill remains applicable to the Phase 5 recovery
target: North Europe restored Fabric reads in 23.478 seconds against the
five-minute regional RTO. The new global-layer drill stayed below the
one-hour RTO and retained policy revision 1, giving zero observed policy RPO.

## Experimental Edge Manager

#154 is outside Phase 5 and v3.1.0. A documented lab scenario can supply the
schedule, local-store, outage-duration, and replay assumptions and provide
evidence for the issue's publication criteria. A real site is not required
for issue closure. The existing Azure-IaaS hybrid site and in-memory disconnect
tests do not demonstrate disconnected refresh or ordered journal replay.
Lab evidence does not establish suitability for a production site's constraints.
The experimental edge Manager can refresh one Iceberg dataset into a local
artifact store and record its manifests in a SQLite journal. It requires one
modulo split, disabled cache write-through, and `PIN_MATERIALIZED_SPLITS=0`.
The edge sends each generation over HTTPS to the owning regional Manager,
which verifies the recorded data and metadata digests before its catalog
commit. The edge acknowledges only a matching response after that commit.
The catalog cursor rejects gaps, other streams, and conflicting retries.

Enable the receiver on a **single non-HA regional Manager** with federation
and the work queue enabled. Set `FSP_EDGE_BINDINGS` to an identity-to-dataset
map, for example:

```json
{"site-1":{"dataset_id":"source::sales","table":"sales","key_prefix":"db/sales"}}
```

The identity must be included in the placement configuration and authenticated
with a unique identity-specific Agent token (`AGENT_AUTH_MODE=required`) or
an Entra Agent identity (`AGENT_AUTH_MODE=entra`). Shared Agent tokens and
operator Basic credentials cannot publish edge generations. The Manager must
see an HTTPS request; if TLS terminates at an ingress, configure trusted proxy
headers rather than trusting arbitrary clients. The binding's prefix must
match the edge table path. Keep the edge principal's access restricted to its
source and do not grant it write access to the regional artifact store or
catalog.

With the same table and connection configuration loaded on the edge, run:

```text
python -m enterprise.control.edge_service --table sales --journal <journal-path> --store-dir <local-artifact-directory> --manager-url https://<regional-manager> --identity site-1 --max-store-bytes <bytes>
```

Set `AGENT_AUTH_MODE` and the corresponding Agent credentials on the edge.
`--refresh-seconds`, `--retry-seconds`, and `--max-pending` control the
schedule, reconnect interval, and queued generation limit. The journal and
artifact directory must be separate. The service holds process locks for both
and stops scheduling refreshes when the pending limit is reached.

The HTTP request is limited to 16 MiB including base64 and JSON overhead.
The artifact limit applies to files in the local store, not the journal or
temporary materialization memory. There is no automatic garbage collection;
acknowledged snapshots and partial writes consume space until an operator
removes them after confirming they are no longer needed. A source change
during copying can leave an invalid immutable destination object requiring
operator intervention. Earlier experimental manifest-only journals lack
metadata digests and are rejected; preserve them rather than replaying them
without verification.

The Azure lab in commit `9a31542` established both documented acceptance
criteria for synthetic data. The edge journal acknowledged 179 generations.
The regional catalog retained 160 ordered replay generations and a stable
edge replay cursor after a process restart and reconnect. This evidence is
not live Iceberg or Fabric data evidence and does not establish
production readiness for a real site.
