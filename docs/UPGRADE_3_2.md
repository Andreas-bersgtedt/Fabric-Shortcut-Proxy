# Upgrade to 3.2

Version 3.2.1 includes the 3.2.0 operator API contract and adds live GCS mount
and BigQuery beta support, plus opt-in S3/GCS replica and
regional-reader support. S3/GCS support is beta for non-production evaluation;
live-provider acceptance is still open.

## Before upgrading

- Back up the configuration and encrypted credential store using the
  [backup guidance](BACKUP_RESTORE.md). Keep the 3.1.0 images and chart
  available for rollback.
- Upgrade Lite and Enterprise together. Enterprise 3.2.1 requires exactly
  `fabric-shortcut-proxy==3.2.1`. Roll out matching Manager, Python
  materializer, catalog, and regional-reader images.
- The chart defaults to Python and C++ image tags `3.2.1`. Publish and verify
  those images before deploying, or configure the intended immutable image
  digests. The C++ serving Agent has its own versioning.
- Quiesce configuration writes during a mixed-version rollout. The 3.1.0
  runtime does not enforce the new revision contract or cooperate with its
  writer lock.

## Operator API changes

Configuration reads return an `ETag`. Native save and apply requests can pass
it in `If-Match` to reject stale writes; native requests without that header
remain unconditional. Typed source/table requests must send
`X-FSP-Configuration-Contract: 3.1.0` and the last ETag for writes. The header
identifies the typed contract, not the package version.

Catalog, Manager, and Config Builder operator errors now use
`application/problem+json` and expose a stable code and correlation ID. Client
parsers must accept the new content type and body. In particular, request
validation details and server-error details can be replaced to avoid exposing
submitted values. Agent control, S3, health, and HTML protocols are unchanged.
See the [Config Builder guide](CONFIG_BUILDER_GUIDE.md) for routes and limits.

## S3/GCS beta

The catalog and primary publication store remain on Azure. The beta adds
opt-in S3/GCS data-plane replicas and regional readers; it does not make
S3/GCS a Manager control store. Use non-production data and configure cloud
identity, network access, and a suitable Manager upload spool. The
[beta gate and limitations](FEDERATED_ARTIFACTS.md#cross-cloud-beta-release-gate)
do not replace live-provider, identity, Fabric-read, residency, or failover
acceptance for production support.

The default Python image does not include Enterprise or the S3/GCS extras.
Build and verify the [cloud-enabled image](../deploy/helm/fabric-shortcut-proxy/README.md#prerequisites)
before configuring the beta workloads.

## Rollback

Stop configuration writes, roll back the chart and matching images together,
and verify readiness and the previous regional reads. Configuration saved by
3.2.1 is not automatically reverted; verify it against 3.1.0 or restore the
pre-upgrade backup before resuming writes. Do not assume that a 3.1.0 client
enforces `If-Match`.
