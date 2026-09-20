# FSP enterprise demo infrastructure

This directory contains the Azure infrastructure and Windows PowerShell
automation for an FSP enterprise demo. The reusable Bicep, scripts, examples,
and Helm chart are tracked. Environment identifiers and generated output stay
local through narrow `.gitignore` rules.

This is the executable source of truth for the enterprise demo. For design context, see the
[enterprise deployment guide](../../docs/Enterprise_Deployment_guide.md); for chart values, see
the [Helm chart reference](../../deploy/helm/fabric-shortcut-proxy/README.md).

## Deployment boundaries

There is no single all-phases PowerShell orchestrator that provisions Azure, starts external
dependencies, and installs workloads. The boundaries are deliberate:

| Phase | Owner | Command |
| --- | --- | --- |
| Preview and provision Azure | Bicep through Azure CLI | `az deployment sub what-if/create` |
| Start existing dependencies and AKS | `Start-FspDemo.ps1` | Idempotent start and readiness checks |
| Install or upgrade Kubernetes workloads | `Deploy-FspDemo.ps1` | Atomic remote Helm upgrades |

Do not hide Bicep create behind a startup script. Infrastructure changes need their own what-if
review, permissions, failure handling, and audit record.

## Resources

The subscription-scope Bicep deployment creates:

- Private AKS with separate system and application pools.
- Azure Container Registry, Key Vault, Azure Files, and private endpoints.
- NFS shares for Manager configuration and FSP artifacts.
- Log Analytics and AKS Container Insights.
- A user-assigned workload identity and federated Kubernetes credential.
- A reserved public IP for ingress-nginx.
- An Entra-only Azure SQL server and database.
- Private endpoints to parameterized existing SQL sources.
- Private DNS and optional bidirectional VNet peering.

The Bicep output `deploymentContract` exposes the Azure-created names and IDs
needed by Helm automation without exposing credentials.

## Security boundary

The template creates these scoped role assignments:

- AKS control-plane identity: Network Contributor on the system subnet.
- FSP workload identity: Key Vault Secrets User on the demo Key Vault.

The AKS identity also needs Network Contributor on the public IP, application
subnet, and its network security group before Kubernetes LoadBalancer services
can reconcile. Assign `AcrPull` to the AKS kubelet identity. Review storage data
roles for any workflow that accesses Azure Files outside the CSI driver path.

ACR anonymous pull is disabled. Key Vault purge protection stays enabled. The
SQL server uses Microsoft Entra-only authentication and has no SQL password.
The three public-network bootstrap parameters should be switched to `Disabled`
after image push and private-path checks.

## Local configuration

Create four ignored files from the committed examples:

```powershell
Copy-Item infra/fsp-demo/main.example.bicepparam infra/fsp-demo/main.local.bicepparam
Copy-Item infra/fsp-demo/deployment.example.json infra/fsp-demo/deployment.local.json
Copy-Item infra/fsp-demo/ingress-nginx-values.example.yaml infra/fsp-demo/ingress-nginx-values.local.yaml
Copy-Item deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.example.yaml `
  deploy/helm/fabric-shortcut-proxy/values-enterprise-demo.local.yaml
```

Set the Azure resource IDs, Entra application IDs, DNS hostname, image digests,
and network values for the target environment. None of these files should
contain passwords, tokens, private keys, database URLs, or client secrets.

The chart expects an existing Kubernetes Secret named `fsp-source` by default.
Create that Secret through the approved secret-delivery process before running
the Helm deployment. The chart does not template credential values.

## Validate and preview Bicep

Run from the repository root:

```powershell
az bicep build --file infra/fsp-demo/main.bicep
az deployment sub validate `
  --name fsp-demo-validate `
  --location swedencentral `
  --template-file infra/fsp-demo/main.bicep `
  --parameters infra/fsp-demo/main.local.bicepparam
az deployment sub what-if `
  --name fsp-demo-whatif `
  --location swedencentral `
  --template-file infra/fsp-demo/main.bicep `
  --parameters infra/fsp-demo/main.local.bicepparam
```

Review existing-resource references, private endpoints, DNS links, and peering
changes before creating the deployment.

## Deploy infrastructure

```powershell
az deployment sub create `
  --name fsp-demo `
  --location swedencentral `
  --template-file infra/fsp-demo/main.bicep `
  --parameters infra/fsp-demo/main.local.bicepparam
```

Capture the deployment outputs, complete the external role assignments, push
the immutable FSP images, and place their digests in the local Helm values.

## Start dependencies

`Start-FspDemo.ps1` reads `deployment.local.json`, starts the external SQL MI
and OPDG VM, waits for Azure SQL, starts AKS, and checks workload readiness.

```powershell
./infra/fsp-demo/Start-FspDemo.ps1 -StatusOnly
./infra/fsp-demo/Start-FspDemo.ps1 -SkipWorkloadValidation
```

Use `-SkipWorkloadValidation` before the first Helm installation because no FSP
pods exist yet.

## Install or upgrade Helm releases

`Deploy-FspDemo.ps1` validates DNS and role assignments, downloads Helm
`v3.18.6` when the installed version differs, lints and packages the FSP chart,
then runs atomic Helm upgrades through `az aks command invoke`. This works with
the private AKS API server without a workstation VPN route.

```powershell
./infra/fsp-demo/Deploy-FspDemo.ps1 -WhatIf
./infra/fsp-demo/Deploy-FspDemo.ps1
```

The script manages three releases:

- `cert-manager`, pinned to `v1.18.2`.
- `ingress-nginx`, pinned to chart `4.13.2`.
- `fsp`, packaged from `deploy/helm/fabric-shortcut-proxy` at chart `2.9.3`.

The first migration uses `--take-ownership` so Helm can adopt matching resources
from the retired enterprise-demo Kustomize overlay. Later runs use normal Helm
release state for upgrades and rollback. Follow the
[Helm migration guide](../../docs/HELM_MIGRATION_GUIDE.md) before the first adoption.

The Namespace, PVs, and PVCs carry Helm's keep policy. `helm uninstall fsp` does not remove
retained state and must not be used as infrastructure cleanup.

## Verify

```powershell
az aks command invoke `
  --resource-group <resource-group> `
  --name <aks-name> `
  --command "helm list -A && kubectl -n fabric-shortcut-proxy get pods,svc,ingress,certificate"
```

`Start-FspDemo.ps1` can perform the private workload readiness checks after the
first Helm deployment.

Inspect or roll back a workload release through private AKS Run Command:

```powershell
az aks command invoke --resource-group <resource-group> --name <aks-name> `
  --command "helm history fsp -n fabric-shortcut-proxy"
az aks command invoke --resource-group <resource-group> --name <aks-name> `
  --command "helm rollback fsp <revision> -n fabric-shortcut-proxy --wait --timeout 15m"
```
