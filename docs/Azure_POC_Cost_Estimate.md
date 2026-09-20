# Azure POC cost estimate

This note defines the cost-estimation method for the current parameterized FSP enterprise demo.
It does not publish a fixed monthly total because Azure retail prices differ by region and change
over time. Generate the estimate from the selected `main.local.bicepparam`, Azure Pricing
Calculator, and an Azure what-if result before approval.

The source SQL platforms and Microsoft Fabric capacity can be existing dependencies. Include
them only when the FSP project owns their cost.

## Current resource inventory

The tracked Bicep defaults create this shape unless local parameters override it:

| Area | Default shape | Cost driver |
| --- | --- | --- |
| AKS control plane | Standard tier, private cluster | AKS tier and regional control-plane meter |
| System node pool | 3 x `Standard_D2s_v5`, zones 1/2/3 | VM, OS disk, and node uptime |
| Application node pool | 3 x `Standard_D4ds_v5`, zones 1/2/3 | VM, temporary/storage profile, and node uptime |
| ACR | Premium | Registry unit, storage, and private endpoint |
| Key Vault | Standard | Secret operations and private endpoint |
| Azure Files | Premium ZRS `FileStorage` | Provisioned storage and file private endpoint |
| Manager config share | 100 GiB NFS quota | Included in provisioned Azure Files capacity model |
| Artifact share | 256 GiB NFS quota | Included in provisioned Azure Files capacity model |
| Log Analytics | 30-day retention | Ingested GB and retention beyond included allowance |
| Public IP | Standard, zone-redundant | Reserved IP and ingress traffic |
| Load balancers | AKS outbound plus public/private ingress | Rules, processed data, and IPs |
| Private endpoints | ACR, Key Vault, Azure Files, SQL targets | Endpoint hours and processed data |
| Private DNS | ACR, Key Vault, Files, SQL, SQL MI, FSP hostname | Zones and query volume |
| Azure SQL | Logical server plus S0 database | Database compute/storage and backup retention |
| Workload identity | User-assigned managed identity | No direct identity hourly charge; dependent service use applies |
| External dependencies | Existing SQL MI and OPDG VM | VM/database uptime when charged to the POC |

The retired estimate used a 4 TiB Azure NetApp Files pool and B-series AKS nodes. Those resources
are not the current Bicep baseline and must not be used to approve this deployment.

## Estimation procedure

1. Populate `infra/fsp-demo/main.local.bicepparam` with the intended region, node sizes/counts,
   zones, network ranges, SQL targets, and bootstrap public-access settings.
2. Run an Azure what-if:

```powershell
az deployment sub what-if `
  --name fsp-demo-cost-review `
  --location <region> `
  --template-file infra/fsp-demo/main.bicep `
  --parameters infra/fsp-demo/main.local.bicepparam
```

3. Export or list the predicted resources and price each billable resource in Azure Pricing
   Calculator for the selected region and currency.
4. Add expected Log Analytics ingestion, outbound data transfer, ACR storage, private endpoint
   traffic, and public/private LoadBalancer data processing.
5. Add OPDG VM, existing SQL MI, source databases, and Fabric capacity only when the project pays
   those shared costs.
6. Record the estimate date, retail/contract price source, reservation assumptions, and uptime
   schedule. Recalculate after any Bicep parameter change.

## Uptime scenarios

Estimate at least these operating patterns:

| Scenario | AKS and dependencies | Use |
| --- | --- | --- |
| Continuous | 730 hours/month | Shared demonstration or UAT environment |
| Business hours | Defined weekday schedule | Repeated workshops with predictable windows |
| On demand | Started only for test sessions | Lowest-cost POC with startup lead time |

`Start-FspDemo.ps1` starts the SQL MI, OPDG VM, and AKS in dependency order. Stopping AKS does not
remove disks, Azure Files, ACR, Key Vault, SQL, private endpoints, DNS zones, or reserved IPs;
those resources continue to incur their own meters.

## Main cost controls

- Stop AKS, SQL MI, and the OPDG VM when the test window closes and service policy permits it.
- Tune AKS node counts and SKUs from measured CPU, memory, and materialization duration rather
  than copying the default HA-oriented shape into every POC.
- Set Log Analytics collection and retention to the evidence actually needed for UAT.
- Keep ACR Premium only while private endpoint support is required.
- Size Premium Azure Files for measured artifact and Manager-state growth. Share quota and billed
  provisioned capacity are separate concepts; confirm the current regional meter.
- Remove unused private endpoints and DNS links only through reviewed Bicep changes.
- Disable ACR, Key Vault, and storage public access after bootstrap; do not trade security for a
  small networking saving without an architecture review.

## Cost review gate

Approve deployment only when the estimate includes:

- Region and pricing date.
- AKS control-plane tier, all node pools, uptime, and scaling assumptions.
- Premium Azure Files provisioned capacity and redundancy.
- ACR, Key Vault, Log Analytics, SQL S0, private endpoints, DNS, IPs, and load balancers.
- Expected data transfer and operation volume.
- Ownership of SQL MI, OPDG VM, source databases, and Fabric capacity.
- A shutdown/deallocation plan and the resources that remain billable while compute is stopped.

The [enterprise runbook](../infra/fsp-demo/README.md) is the source of truth for the deployed
resource model. Reconcile this estimate with Bicep whenever that model changes.
