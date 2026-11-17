# Roadmap and open gaps

This index summarizes open work as of 2026-11-17. GitHub issues are the
authority for current scope, acceptance criteria, and status. Historical plans
in [archive](archive/README.md) are not current implementation guidance.

## Open work

| Issue | Status and remaining work |
| --- | --- |
| [#176: Operator API revisions and errors](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/176) | Unreleased source adds ETags and conditional configuration writes. Shared Problem Details across Catalog, Manager, and Config Builder, the full error mapping tests, and OpenAPI coverage remain open. Released 3.1.0 does not contain the new configuration contract. |
| [#94: Source capability gaps](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/94) | Databricks, Redshift, and Teradata remain beta pending source-specific live evidence. Impala is supported for the tested CDP 7.1.7 / Impala 3.4 scope. See the [capability matrix and live gates](SOURCE_CAPABILITIES.md). |
| [#171: Production AWS and GCP federation](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/171) | Deferred beyond 3.1.0. Production regional deployment, workload identities, live-provider conformance, cross-cloud residency, and failover evidence remain. |
| [#144: S3 and GCS artifact stores](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/144) | Child of #171. SDK-backed data-plane foundations and test-double conformance exist; live provider conformance and regional replication/serving integration remain. |
| [#154: Edge Manager and local journal](https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/issues/154) | Deferred until a real site supplies its disconnected-refresh schedule, local-store, outage-duration, and replay requirements. |

## Delivered scope and boundaries

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
