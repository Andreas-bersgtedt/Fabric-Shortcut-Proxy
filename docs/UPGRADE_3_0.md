# Upgrade to 3.0

Version 3.0.1 retains the 3.0 compatibility baseline and adds the scoped Impala
support described below. Version 3.0.0 established the Lite and Enterprise feature set as a new
supported compatibility baseline. It does not introduce a storage-format or
configuration-schema migration from 2.10.0.

## Compatibility contract

- Upgrade the Lite and Enterprise Python packages together. Enterprise 3.0.1
  requires exactly `fabric-shortcut-proxy==3.0.1`.
- Upgrade the Manager, Python materializer, and Helm chart together. Do not mix
  3.0.0 and 3.0.1 Python control-plane components.
- Existing configuration files, encrypted credential stores, shared artifact
  generations, durable Manager state, and backup archives remain compatible.
- The independently versioned C++ serving Agent 1.0.0 is the supported serving
  binary for this baseline. Its read-only S3 contract is unchanged.
- Databricks, Redshift, and Teradata remain beta. Apache Impala is supported
  for the tested CDP 7.1.7 / Impala 3.4 runtime. Other Impala versions require
  separate certification evidence.

## Helm upgrade

Use immutable application image digests in the environment-specific values
file. Validate and render the chart before applying it:

```powershell
helm lint deploy/helm/fabric-shortcut-proxy
helm template fsp deploy/helm/fabric-shortcut-proxy `
  --namespace fabric-shortcut-proxy `
  -f deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.example.yaml
```

When SQL Server is a configured source, build the enterprise Python image with
`--build-arg FSP_INSTALL_MSSQL_ODBC=1` as shown in the enterprise deployment
guide. Before pushing it, verify that the image contains `libodbc.so.2`, imports
`pyodbc`, and reports matching Lite and Enterprise package versions.

Apply the upgrade through the environment's private-cluster administration
path. For the enterprise demo, use the peered Linux jump box. Wait for the
Manager, materializers, C++ Agents, and Nginx deployments to become ready before
running a materialization and signed object read.

## Rollback

Because 3.0.0 does not migrate persisted formats, Helm may roll workloads back
to the prior 2.10.0 revision. Use `helm history` and `helm rollback`, then verify
all readiness endpoints and a complete materialization/read path. Restore a
backup only when workload rollback alone does not restore the expected
configuration or durable control state.
