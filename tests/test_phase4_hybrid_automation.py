from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FEDERATION = ROOT / "infra" / "fsp-federation"
HYBRID = ROOT / "infra" / "fsp-hybrid-site"


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_federation_networking_references_existing_vm_and_adds_peerings():
    template = read("infra/fsp-federation/main.bicep")
    peering_module = read("infra/modules/hybrid-vnet-peering.bicep")
    local_params = read("infra/fsp-federation/main.local.bicepparam")

    assert "param hybridVmVnetName string" in template
    assert "param hybridVmResourceGroupName string" in template
    assert "resourceId(" in template
    assert "hybridVmVnetId" in template
    assert "param hybridVmVnetName = 'fsp-linux-test-vnet'" in local_params
    assert "../modules/hybrid-vnet-peering.bicep" in template
    assert template.count("remoteVirtualNetworkResourceId: hybridVmVnetId") == 2
    assert template.count("remoteVirtualNetworkResourceId: primaryVnetId") == 2
    assert template.count("remoteVirtualNetworkResourceId: secondaryVnetId") == 2
    assert "peeringName: 'peer-swc-to-neu'" in template
    assert "peeringName: 'peer-neu-to-swc'" in template
    assert "Microsoft.Compute/virtualMachines" not in template
    assert "Microsoft.Network/virtualNetworks@" not in template
    assert "name: 'fsp-${peeringName}'" in peering_module


def test_federation_adds_regional_publication_storage_and_reader_identities():
    template = read("infra/fsp-federation/main.bicep")
    catalog = read("infra/fsp-federation/modules/catalog-storage.bicep")
    replica = read("infra/fsp-federation/modules/replica-storage.bicep")
    reader_identity = read("infra/fsp-federation/modules/regional-reader-identity.bicep")
    ingress_edge = read("infra/fsp-federation/modules/regional-ingress-edge.bicep")

    assert "name: 'published'" in catalog
    assert "name: 'replica'" in replica
    assert "name: 'Standard_ZRS'" in replica
    assert "publicNetworkAccess: 'Disabled'" in replica
    assert "allowSharedKeyAccess: false" in replica
    assert "id-fsp-phase4-reader-swc" in template
    assert "id-fsp-phase4-reader-neu" in template
    assert "federatedIdentityCredentials@2024-11-30" in reader_identity
    assert "regionalReaderServiceAccountName" in template
    assert "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1" in catalog
    assert "2a2b9908-6ea1-4ae2-8e65-a410df84e7d1" in replica
    assert "pip-fsp-phase4-neu" in template
    assert "Microsoft.Network/publicIPAddresses@2024-07-01" in ingress_edge
    assert "'1'" in ingress_edge and "'2'" in ingress_edge and "'3'" in ingress_edge


def test_catalog_storage_reuses_blob_dns_and_links_hybrid_vnet():
    storage = read("infra/fsp-federation/modules/catalog-storage.bicep")

    assert "Microsoft.Network/privateDnsZones@2024-06-01' existing" in storage
    assert "virtualNetworkResourceId: hybridVmVnetId" in storage
    assert "name: 'link-fsp-hybrid-vm'" in storage
    assert "resource blobPrivateDnsZone 'Microsoft.Network/privateDnsZones@" in storage
    assert "registrationEnabled: false" in storage


def test_hybrid_staging_access_is_scoped_to_existing_container():
    staging = read("infra/modules/hybrid-staging.bicep")

    assert "Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing" in staging
    assert "scope: stagingContainer" in staging
    assert "principalId: principalId" in staging
    assert "Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' =" not in staging


def test_windows_installer_fails_preflight_before_side_effects():
    installer = read("infra/fsp-hybrid-site/Install-FspHybridSite.ps1")

    preflight = installer.index("$blobDnsLinks = Invoke-AzJson")
    process_gate = installer.index("$PSCmdlet.ShouldProcess(")
    package_upload = installer.index("& az storage blob upload")
    vm_start = installer.index("& az vm start")

    assert "is not attached to VM" in installer
    assert "$configuration.publicIpAddress.id" in installer
    assert "The existing Blob private DNS zone is not linked" in installer
    assert "git.exe ls-remote --exit-code --refs" in installer
    assert "PackageCurrentCheckout" in installer
    assert "Get-FileHash -Path $archive -Algorithm SHA256" in installer
    assert preflight < process_gate < package_upload < vm_start
    assert "FSP_HYBRID_INSTALL_READY" in installer


def test_linux_installer_checks_private_readiness_and_async_sqlite():
    installer = read("infra/fsp-hybrid-site/install.sh")

    assert "for prerequisite in" in installer
    assert "python3 -m pip --version" in installer
    assert "Python 3.11 or newer is required." in installer
    assert "Blob DNS did not resolve" in installer
    assert "non-private IPv4 address" in installer
    assert 'manager_ready_url="${manager_base}/readyz"' in installer
    assert "ManagedIdentityCredential(client_id=os.environ" in installer
    assert 'db_url="sqlite+aiosqlite:////var/lib/fsp/hybrid-site.db"' in installer
    assert "await connection.execute(text(\"SELECT 1\"))" in installer
    assert "agent_registered_ok" in installer
    assert "FSP_HYBRID_INSTALL_READY agent_id=" in installer


def test_federation_documentation_describes_additive_install_path():
    documentation = (FEDERATION / "README.md").read_text(encoding="utf-8")
    installer = (HYBRID / "Install-FspHybridSite.ps1").read_text(encoding="utf-8")

    assert "existing hybrid VM VNet" in documentation
    assert "PackageCurrentCheckout" in documentation
    assert "FSP.Agent" in documentation
    assert "$PSCmdlet.ShouldProcess(" in installer
