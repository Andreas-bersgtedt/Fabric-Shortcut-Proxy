# Upgrade to 3.1

Version 3.1.0 adds Azure multi-region federation, hybrid identity, global
governance, and the distributed performance path. Existing standalone and
single-region defaults remain compatible and disabled features stay opt-in.

## Compatibility contract

- Upgrade Lite and Enterprise together. Enterprise 3.1.0 requires exactly
  `fabric-shortcut-proxy==3.1.0`.
- Upgrade Manager, Python materializers, regional readers, catalog service, and
  Helm chart together. Do not mix 3.0.1 and 3.1.0 Python control-plane
  components.
- C++ serving Agent 1.0.0 remains supported. Its read-only S3 contract is
  unchanged.
- Existing 3.0.1 configuration files, encrypted credentials, backup archives,
  non-federated artifact layouts, and durable Manager HA state remain
  compatible.
- Databricks, Redshift, and Teradata remain beta. Apache Impala remains
  supported for the tested CDP 7.1.7 / Impala 3.4 runtime.

## New optional configuration

The following controls are disabled unless configured:

- signed policy bundles and persistent last-known-good cache;
- tenant declarations and tenant-owned placement resources;
- Agent release lines and rollout rings;
- egress pricing;
- federated catalog, replication, and regional readers;
- compressed-byte split targets and connection/pool concurrency limits.

Helm supplies safe defaults when upgrading releases whose stored values predate
these fields. Use a complete environment values file for production rather
than relying on `--reuse-values` alone.

## Federated tenant migration

Existing non-federated deployments need no data migration. If an existing
federated catalog is moved from an unsigned single-tenant policy to a tenant
policy:

1. Back up the catalog container.
2. Quiesce regional Managers.
3. Add the intended `tenant_id` to each existing dataset record with
   compare-and-swap.
4. Deploy the matching signed policy and public key.
5. Restart catalog, regional readers, and Managers.
6. Verify tenant ownership, catalog fence, replica pointers, and both Fabric
   shortcuts before resuming refreshes.

A tenant-enabled runtime rejects legacy catalog records without `tenant_id`
rather than assigning ownership implicitly.

## Helm and image rollout

Use immutable image digests. Validate the release chart before applying it:

```powershell
helm lint deploy/helm/fabric-shortcut-proxy
helm template fsp deploy/helm/fabric-shortcut-proxy `
  --namespace fsp-phase3 `
  -f <environment-values.yaml>
```

Roll one non-production region first, then the production canary region, then
remaining regions. Wait for Manager, materializer, catalog, and regional reader
readiness at each ring. Verify the policy revision, tenant, Agent version ring,
replica generation, egress metrics, and a signed Fabric object read.

## Rollback

3.1.0 does not rewrite existing non-federated artifacts or Manager HA state.
If the new governance controls have not been enabled, roll back the Helm
revision and matching Python image digest together.

After tenant fields or signed policy revisions are applied, do not roll only
one regional component back to 3.0.1. Restore the prior policy/catalog backup
and roll all Python control-plane components together, then verify ownership
fences and regional reads.
