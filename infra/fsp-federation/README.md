# Phase 4 federated Azure infrastructure

This subscription-scope Bicep deployment adds the resources required by the
Phase 4 catalog without recreating either AKS cluster or VNet:

- a private `StorageV2` account using `Standard_RAGZRS`;
- separate `catalog`, `published`, and `fsp-hybrid-staging` Blob containers;
- a private North Europe `Standard_ZRS` replica account and `replica` container;
- container-scoped managed-identity role assignments for both regional
  Managers, read-only regional serving identities, and the hybrid-site identity;
- workload-identity federation for a dedicated `fsp-regional-reader` service
  account in each AKS cluster;
- a zone-redundant North Europe public IP with Network Contributor scoped to
  that IP for the existing AKS control-plane identity;
- Blob private endpoints in Sweden Central and North Europe;
- links from both existing AKS VNets and the hybrid VM VNet to the existing
  `privatelink.blob.core.windows.net` zone in `fabricproxy`; and
- reciprocal VNet peerings from the existing hybrid VM VNet to both endpoint
  VNets and between the regional VNets, so every advertised private endpoint
  address is routable from either Manager.

All VNets and identities are referenced as existing resources. The template
does not create or update the VM, NIC, VNet, subnet, AKS cluster, or private DNS
zone. It adds peering resources, a DNS-zone link, Blob data access for the
hybrid identity, and the catalog resources.

The regional Managers receive Blob Contributor only on the `catalog`,
`published`, and `replica` containers. Each regional serving identity receives
Blob Reader on `catalog` and its local `published` or `replica` container.

The North Europe edge uses the Azure-managed hostname
`fsp-phase4-neu-anbergst.northeurope.cloudapp.azure.com`. The existing Sweden
public IP is retained and carries
`fsp-phase4-swc-anbergst.swedencentral.cloudapp.azure.com`; preserve that DNS
label when updating `pip-fspdemo-nginx`.

Validate and preview from the repository root:

```powershell
az bicep build --file infra/fsp-federation/main.bicep
az deployment sub validate `
  --name fsp-phase4-validate `
  --location swedencentral `
  --template-file infra/fsp-federation/main.bicep `
  --parameters infra/fsp-federation/main.local.bicepparam
az deployment sub what-if `
  --name fsp-phase4-whatif `
  --location swedencentral `
  --template-file infra/fsp-federation/main.bicep `
  --parameters infra/fsp-federation/main.local.bicepparam
```

Review the what-if before deployment. It may add the storage account,
containers, private endpoints, private DNS VNet links, VNet peerings, and
container-scoped role assignments. It must not replace either referenced AKS
cluster, VNet, private DNS zone, or managed identity.

Do not redeploy the whole `infra/fsp-linux-test-vm.bicep` creation template
over the existing VM without a separate what-if. The template includes VM
security settings whose compatibility with the existing VM has not been
verified. Use a reviewed identity-only update if that preview changes
unrelated VM or network settings.

Before running the hybrid installer, deploy and review the additive template.
Its parameters default to the inventory values and can be overridden for other
environments. The installer checks the reciprocal peerings, the VM's private
DNS-zone link, VM identity attachment, and absence of a VM public IP before it
offers to start the VM.

The installer defaults to the published `main` branch and rejects a ref that
is not published. To install the current checkout instead, use
`-PackageCurrentCheckout`; this packages the core and enterprise projects,
uploads the SHA-256-addressed archive to the private `fsp-hybrid-staging`
container using the caller's Entra login, then downloads it on the VM using
the attached managed identity. The caller must be able to reach that private
Blob endpoint and upload to the container. A prepared archive can be supplied
with `-SourceArchivePath`.

The installer fails before installation unless private Blob DNS resolves to
private IPv4 addresses, Blob HTTPS and Manager `/readyz` respond, the VM can
obtain a token for its assigned identity, and the identity can read the
staging container. It installs both Python projects, checks that SQLite uses
the `aiosqlite` driver and can execute a query, then waits for successful
Manager registration. `systemctl is-active` by itself is not success evidence.

The separate `infra/Configure-FspHybridIdentity.ps1` step still assigns the
`FSP.Agent` application role. The installer does not change VM identity,
network security rules, or application-role assignments.

`infra/fsp-hybrid-site/Install-FspHybridSite.ps1` installs without a fleet token
or client secret, but private networking and the prerequisites above are not
automatically provided by that script.
