# Use Cases & Scenarios: Private-Infrastructure Connectivity

How to surface data into Microsoft Fabric through the Fabric Shortcut Proxy **without
exposing the data endpoint to the public internet**: across on-premises, Azure vNet, and other-cloud (AWS/GCP)
private infrastructure. Scenarios are split by consumption style (**Fabric shortcuts**
vs **Fabric Spark**) because each reaches a private endpoint through a different Fabric
mechanism.

> Companion docs: [README.md](../README.md) (setup),
> [TechnicalArchitecture.md](TechnicalArchitecture.md) (per-process flows),
> [SECURITY.md](SECURITY.md) (auth/TLS/audit/credential mediation),
> [CONFIGURATION.md](CONFIGURATION.md) (settings).
>
> **Setup guide:** step-by-step wiring for the connectivity patterns below (OPDG shortcut,
> Spark MPE/PLS, storage-proxy mounts) is in [CONNECTIVITY_SETUP.md](CONNECTIVITY_SETUP.md).

Grounded in the Fabric connectivity mechanisms documented at:
- On-premises data gateway shortcuts, <https://learn.microsoft.com/fabric/onelake/create-on-premises-shortcut>
- Managed private endpoints (Spark), <https://learn.microsoft.com/fabric/security/connect-to-on-premise-sources-using-managed-private-endpoints>
- Managed virtual networks, <https://learn.microsoft.com/fabric/security/security-managed-vnets-fabric-overview>
- Private links for Fabric, <https://learn.microsoft.com/fabric/security/security-private-links-overview>

---

## The two connectivity primitives

Everything below rests on two Fabric features that keep source and data-plane traffic on a private route, one per consumption style. Fabric service connectivity is still required.

**Shortcut path → On-premises data gateway (OPDG).** OneLake S3-compatible shortcuts can
target a gateway installed inside your private network. The gateway bridges OneLake to
endpoints like `http://10.0.1.4:9000` or `https://mys3api.contoso.com`, explicitly
supported for on-prem, firewall/VPC, and network-restricted S3-compatible storage.
Shortcut caching (1–28 day retention) reduces repeat egress.

**Spark path → Managed Private Endpoint (MPE) + Private Link Service (PLS).** A Fabric
workspace on a Managed VNet creates an outbound MPE to an Azure Private Link Service that
fronts the proxy (internal Standard LB → PLS). Approved once, Spark notebooks / Spark job
definitions read over the Microsoft backbone. For on-prem or other-cloud, the PLS sits on a
forwarding VM (DNAT) reachable over ExpressRoute/VPN.

```mermaid
flowchart LR
  subgraph Fabric
    SC[OneLake shortcut]
    SP[Spark notebook / SJD]
  end
  OPDG[On-prem data gateway]
  MPE[Managed Private Endpoint] --> PLS[Private Link Service + internal LB]
  PX[Fabric Shortcut Proxy<br/>S3 + SigV4, TLS]
  SRC[(Private source:<br/>RDBMS / NAS / object store)]

  SC -->|S3-compatible + SigV4| OPDG --> PX
  SP -->|s3a / boto3 + SigV4| PLS --> PX
  PX -->|SQL pushdown OR byte passthrough| SRC
```

The proxy is well suited to sit behind either primitive: a single S3-compatible endpoint
with SigV4 + per-key ACLs, TLS, upstream **credential mediation** (Fabric never sees the
source secrets), and two data paths, DB→Iceberg/Delta virtualization *and* file/object
passthrough (`local` NFS/SMB, `s3`/MinIO, `azure` blob).

---

## Use-case matrix

| # | Consumption | Environment | Proxy data path | Private link mechanism |
|---|---|---|---|---|
| 1 | **Shortcut** | On-prem | DB→Iceberg/Delta (SQL Server/Oracle) | OPDG in the same LAN |
| 2 | **Shortcut** | On-prem | Passthrough (NAS: NFS/SMB) | OPDG in the same LAN |
| 3 | **Shortcut** | Other cloud (AWS/GCP) | Passthrough (MinIO/S3) or DB (RDS) | OPDG inside the VPC |
| 4 | **Spark** | Azure vNet | DB (Azure SQL MI / Postgres, no public IP) | MPE → PLS (internal LB) |
| 5 | **Spark** | Azure vNet | Passthrough (private ADLS/MinIO) | MPE → PLS |
| 6 | **Spark** | On-prem (regulated) | DB→Delta (Oracle) | MPE → PLS on forwarding VM (DNAT) over ExpressRoute |
| 7 | **Shortcut + Spark** | Multi-cloud mesh | Fleet: many mounts, one endpoint | OPDG per site + MPE for Azure-resident |
| 8 | **Shortcut** | Cross-tenant/regulated | DB or passthrough | OneLake Private Link (same-tenant) + OPDG |

---

## Flagship scenarios

### 1: On-prem OLTP as a live Lakehouse table, zero copy [Shortcut]

The proxy runs beside an on-prem SQL Server/Oracle, exposing a table as Iceberg/Delta. An
OPDG on the same LAN bridges OneLake to `http://proxy:9000`; a Fabric S3-compatible
shortcut (bound to a scoped SigV4 key) makes it a Lakehouse table feeding a Direct Lake
model. Only the queried rows traverse the gateway; nothing lands in a public bucket, and
shortcut caching absorbs repeat reads. This is the proxy's core DB-virtualization value
with private connectivity.

### 3: Consolidate AWS-resident data into Fabric without public egress [Shortcut]

The proxy runs on an EC2/EKS host in the AWS VPC, with an `s3` mount to a VPC-internal
MinIO bucket (or DB-virtualization over RDS). The proxy holds the AWS credentials; Fabric
only ever sees SigV4. An OPDG installed on a Windows host in the same VPC is selected in
the shortcut's **Data gateway** dropdown, so OneLake reaches the VPC-private endpoint
directly. No bucket is exposed to the internet, and no AWS keys are handed to Fabric.

### 4: vNet-isolated Spark analytics over a private Azure DB [Spark]

The proxy runs in an Azure VNet (VM/AKS/Container Apps) behind an internal Standard LB +
Private Link Service, virtualizing an Azure SQL MI / Postgres Flexible Server that has *no*
public endpoint. The Fabric workspace (Managed VNet) creates an MPE to the PLS; after
approval, a Spark notebook does `spark.read.format("delta")` via `s3a`/boto3 against the
proxy's private FQDN. Heavy distributed joins/transforms run in Spark, the source DB stays
private, and every object read is audit-logged by the proxy.

### 6: Air-gapped / regulated Spark ETL against on-prem Oracle [Spark]

Where policy forbids any public path, front the on-prem proxy with the "Direct Connect"
pattern: a forwarding VM (iptables DNAT/MASQUERADE) + PLS on the Azure side of an
ExpressRoute/VPN, and a Fabric MPE to that PLS. Spark reads Oracle-as-Delta entirely over
private links, with credential mediation keeping the Oracle secret out of Fabric.

### 7: Multi-cloud data mesh on one endpoint [Shortcut + Spark]

Run the Manager/Agent fleet; each mount is a different private backend (on-prem NAS, AWS
MinIO, Azure ADLS, on-prem Oracle). One SigV4 endpoint, with per-key ACLs scoping each
domain team to its buckets/prefixes (read-only). Domains that just browse files use
shortcuts via OPDG; domains doing transforms use Spark via MPE, all against the same
governed front door.

---

## Why the proxy fits "private infrastructure without a public data endpoint"

- **Credential mediation**: upstream S3/Azure/DB secrets are held encrypted (DPAPI/Fernet)
  and resolved by id; Fabric only presents SigV4. Cross-cloud keys never leave the private
  environment.
- **Per-key ACL + forced mount auth**: a secured mount is never anonymous, even with
  `REQUIRE_SIGV4=0`.
- **TLS at the proxy or fronting LB**, plus an audit log per mounted-object access.
- **Two paths from one endpoint**: live SQL pushdown *and* file passthrough, so the same
  private deployment serves warehouse tables and legacy file estates.

---

## Key asymmetry: shortcuts vs Spark

| | Reaches private endpoint via | Works for |
|---|---|---|
| **Shortcuts** | On-premises data gateway (OPDG) | On-prem, AWS/GCP VPCs, firewall/network-restricted |
| **Spark** | Managed Private Endpoint → Private Link Service | In-VNet (trivial); on-prem/other-cloud via a forwarding VM + ExpressRoute/VPN |

Shortcuts reach private endpoints cross-cloud through OPDG; Spark reaches them
through an Azure-fronted PLS, which is trivial in-VNet and needs a forwarding VM for
on-prem or other-cloud sources.
