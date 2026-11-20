# Roadmap and open gaps

This index summarizes open work as of 2026-11-19. GitHub issues are the
authority for current scope, acceptance criteria, and status. Historical plans
in [archive](archive/README.md) are not current implementation guidance.

## Open work

| Issue | Status and remaining work |
| --- | --- |
| [#94: Source capability gaps](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/94) | Databricks, Redshift, and Teradata remain beta pending source-specific live evidence. Impala is supported for the tested CDP 7.1.7 / Impala 3.4 scope. See the [capability matrix and live gates](SOURCE_CAPABILITIES.md). |
| [#171: Production AWS and GCP federation](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/171) | Umbrella issue split into [#182 GCP/GKE/GCS federation](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/182) and [#183 AWS/EKS/S3 federation](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/183). Shared cross-cloud residency and failover acceptance remain under #171. |
| [#182: GCP/GKE/GCS federation](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/182) | Live GKE, GCS catalog/replica, Workload Identity, SigV4, negative-IAM, residency, reader-restart and billing evidence passed on 2026-11-20. The short-lived clusters, buckets, GSAs and registries were deleted. Console billing showed `£0.00` cost for 1-20 November 2026. A live Microsoft Fabric shortcut read remains blocked by inactive workspace capacity and `constraints/iam.disableServiceAccountKeyCreation`. |
| [#144: S3 and GCS artifact stores](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/144) | **GCS provider exit evidence complete; combined issue open:** GCS live shared-conformance and mount conformance passed on 2026-11-19, and GCS regional replication/serving integration passed. Remaining work is live S3/Blob provider conformance and provider-specific production deployment, IAM isolation, Fabric reads, residency, egress, and failover evidence. See the [cross-cloud beta release gate](FEDERATED_ARTIFACTS.md#cross-cloud-beta-release-gate). |
| [#180: GCS mounts and BigQuery source](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/180) | Implementation and Azure UAT passed live gates on 2026-11-19. GCS passed read/write, range, size, SHA-256, retry, conflict, prefix, and denied-404 checks. BigQuery passed discovery, schema, range, modulo, date, reconnect, empty, timeout, read-only, and repeated/nested cases. UAT resources are cleaned up. |

## Delivered scope and boundaries

[#154: Edge Manager and local journal](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/154)
closed on 2026-11-18 in commit `9a31542`. The Azure lab ran two private
`Standard_B2s` VMs in Sweden Central and observed 179 acknowledged edge journal
generations, 160 regional replay generations, and a stable edge replay cursor.
The edge process was stopped and restarted during a TCP 9443 NSG outage, then
acknowledged generations in sequence after reconnect. This lab evidence covers
regional-only publication and ordered replay. It uses synthetic SQLite data and
local stores, so it does not establish a real-site or production-readiness gate.

[#176: Operator API revisions and errors](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/176)
shipped in 3.2.0. Revision checks cover the Config Builder current, save, and
apply endpoints; native writes without `If-Match` remain unconditional. See the
[Config Builder guide](CONFIG_BUILDER_GUIDE.md#conditional-configuration-writes-320)
for the contract and its limits.

The Azure multi-region and simulated hybrid-site work in
[#112](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/112)
met its 3.1.0 acceptance criteria. AWS/GCP production deployment and the edge
Manager are outside that scope. See [federated publication](FEDERATED_ARTIFACTS.md)
and [global governance](GLOBAL_GOVERNANCE.md) for acceptance evidence and
limitations, including physical primary-store relocation and multi-store
materialization dispatch.

Normal table startup currently requires an integer split key. The lower-level
date-range and row-number planner paths do not change that startup restriction.

The historical distributed architecture assessment referenced in older issues
was a local planning document, not a tracked current roadmap. Use the issues
and maintained references above for current status.
