targetScope = 'subscription'

@description('Subscription containing the existing FSP resources.')
param targetSubscriptionId string

@description('Sweden Central resource group containing the primary FSP deployment.')
param primaryResourceGroupName string = 'FSP_Demo_xdev_swedencentral'

@description('North Europe resource group containing the secondary FSP deployment.')
param secondaryResourceGroupName string = 'FSP_Demo_xdev_northeurope'

@description('Resource group containing the existing hybrid-site VM and VNet.')
param hybridVmResourceGroupName string = 'rg-fsp-linux-test-sc'

@description('Existing hybrid-site VM VNet name.')
param hybridVmVnetName string = 'fsp-linux-test-vnet'

@description('Existing hybrid-site user-assigned identity name.')
param hybridSiteIdentityName string = 'id-fsp-hybrid-site'

@description('Resource group containing the existing Blob private DNS zone.')
param blobPrivateDnsZoneResourceGroupName string = 'fabricproxy'

@description('Existing Blob private DNS zone name.')
param blobPrivateDnsZoneName string = 'privatelink.blob.core.windows.net'

@description('Primary Azure region.')
param primaryLocation string = 'swedencentral'

@description('Primary FSP VNet name.')
param primaryVnetName string = 'vnet-fspdemo-xdev-swc'

@description('Secondary FSP VNet name.')
param secondaryVnetName string = 'vnet-fspdemo-xdev-neu'

@description('Private endpoint subnet present in both existing VNets.')
param privateEndpointSubnetName string = 'snet-private-endpoints'

@description('Primary AKS workload identity name.')
param primaryManagerIdentityName string = 'id-fspdemo-workload'

@description('Secondary AKS workload identity name.')
param secondaryManagerIdentityName string = 'id-fspdemo-xdev-neu'

@description('Primary AKS cluster name.')
param primaryAksName string = 'aks-fspdemo-xdev-swc'

@description('Secondary AKS cluster name.')
param secondaryAksName string = 'aks-fspdemo-xdev-neu'

@description('Namespace used by the federated regional serving workloads.')
param federationNamespace string = 'fsp-phase3'

@description('Service account used by the read-only regional serving workloads.')
param regionalReaderServiceAccountName string = 'fsp-regional-reader'

@description('Tags applied to new Phase 4 resources.')
param tags object = {
  Application: 'Fabric-Shortcut-Proxy'
  Environment: 'Demo'
  ManagedBy: 'Bicep'
  Phase: '4'
}

var storageAccountName = 'stfspcat${take(uniqueString(targetSubscriptionId, 'phase4-catalog'), 13)}'
var replicaStorageAccountName = 'stfsprep${take(uniqueString(targetSubscriptionId, 'phase4-replica'), 13)}'
var primaryVnetId = resourceId(
  targetSubscriptionId,
  primaryResourceGroupName,
  'Microsoft.Network/virtualNetworks',
  primaryVnetName
)
var secondaryVnetId = resourceId(
  targetSubscriptionId,
  secondaryResourceGroupName,
  'Microsoft.Network/virtualNetworks',
  secondaryVnetName
)
var hybridVmVnetId = resourceId(
  targetSubscriptionId,
  hybridVmResourceGroupName,
  'Microsoft.Network/virtualNetworks',
  hybridVmVnetName
)

resource primaryManagerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  name: primaryManagerIdentityName
}

resource secondaryManagerIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  name: secondaryManagerIdentityName
}

resource hybridSiteIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' existing = {
  scope: resourceGroup(targetSubscriptionId, hybridVmResourceGroupName)
  name: hybridSiteIdentityName
}

module primaryReaderIdentity './modules/regional-reader-identity.bicep' = {
  name: 'fsp-phase4-reader-identity-primary'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  params: {
    location: primaryLocation
    identityName: 'id-fsp-phase4-reader-swc'
    aksName: primaryAksName
    namespace: federationNamespace
    serviceAccountName: regionalReaderServiceAccountName
    tags: tags
  }
}

module secondaryReaderIdentity './modules/regional-reader-identity.bicep' = {
  name: 'fsp-phase4-reader-identity-secondary'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    location: 'northeurope'
    identityName: 'id-fsp-phase4-reader-neu'
    aksName: secondaryAksName
    namespace: federationNamespace
    serviceAccountName: regionalReaderServiceAccountName
    tags: tags
  }
}

module secondaryIngressEdge './modules/regional-ingress-edge.bicep' = {
  name: 'fsp-phase4-regional-ingress-secondary'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    location: 'northeurope'
    publicIpName: 'pip-fsp-phase4-neu'
    aksName: secondaryAksName
    tags: tags
  }
}

module catalogStorage './modules/catalog-storage.bicep' = {
  name: 'fsp-phase4-catalog-storage'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  params: {
    targetSubscriptionId: targetSubscriptionId
    location: primaryLocation
    storageAccountName: storageAccountName
    primaryManagerPrincipalId: primaryManagerIdentity.properties.principalId
    secondaryManagerPrincipalId: secondaryManagerIdentity.properties.principalId
    primaryReaderPrincipalId: primaryReaderIdentity.outputs.principalId
    secondaryReaderPrincipalId: secondaryReaderIdentity.outputs.principalId
    primaryVnetId: primaryVnetId
    secondaryVnetId: secondaryVnetId
    hybridVmVnetId: hybridVmVnetId
    blobPrivateDnsZoneResourceGroupName: blobPrivateDnsZoneResourceGroupName
    blobPrivateDnsZoneName: blobPrivateDnsZoneName
    tags: tags
  }
}

module replicaStorage './modules/replica-storage.bicep' = {
  name: 'fsp-phase4-replica-storage'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    location: 'northeurope'
    storageAccountName: replicaStorageAccountName
    primaryManagerPrincipalId: primaryManagerIdentity.properties.principalId
    secondaryManagerPrincipalId: secondaryManagerIdentity.properties.principalId
    secondaryReaderPrincipalId: secondaryReaderIdentity.outputs.principalId
    tags: tags
  }
}

module hybridStagingAccess '../modules/hybrid-staging.bicep' = {
  name: 'fsp-phase4-hybrid-staging-access'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  dependsOn: [
    catalogStorage
  ]
  params: {
    storageAccountName: storageAccountName
    containerName: 'fsp-hybrid-staging'
    principalId: hybridSiteIdentity.properties.principalId
  }
}

module hybridVmToPrimary '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-hybrid-vm-to-swc'
  scope: resourceGroup(targetSubscriptionId, hybridVmResourceGroupName)
  params: {
    localVnetName: hybridVmVnetName
    remoteVirtualNetworkResourceId: primaryVnetId
    peeringName: 'peer-fsp-hybrid-to-swc'
  }
}

module primaryToHybridVm '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-swc-to-hybrid-vm'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  params: {
    localVnetName: primaryVnetName
    remoteVirtualNetworkResourceId: hybridVmVnetId
    peeringName: 'peer-swc-to-fsp-hybrid'
  }
}

module hybridVmToSecondary '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-hybrid-vm-to-neu'
  scope: resourceGroup(targetSubscriptionId, hybridVmResourceGroupName)
  params: {
    localVnetName: hybridVmVnetName
    remoteVirtualNetworkResourceId: secondaryVnetId
    peeringName: 'peer-fsp-hybrid-to-neu'
  }
}

module secondaryToHybridVm '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-neu-to-hybrid-vm'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    localVnetName: secondaryVnetName
    remoteVirtualNetworkResourceId: hybridVmVnetId
    peeringName: 'peer-neu-to-fsp-hybrid'
  }
}

module primaryToSecondary '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-swc-to-neu'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  params: {
    localVnetName: primaryVnetName
    remoteVirtualNetworkResourceId: secondaryVnetId
    peeringName: 'peer-swc-to-neu'
  }
}

module secondaryToPrimary '../modules/hybrid-vnet-peering.bicep' = {
  name: 'fsp-phase4-neu-to-swc'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    localVnetName: secondaryVnetName
    remoteVirtualNetworkResourceId: primaryVnetId
    peeringName: 'peer-neu-to-swc'
  }
}

module primaryPrivateEndpoint './modules/catalog-private-endpoint.bicep' = {
  name: 'fsp-phase4-catalog-pe-primary'
  scope: resourceGroup(targetSubscriptionId, primaryResourceGroupName)
  params: {
    location: primaryLocation
    name: 'pe-${storageAccountName}-blob-swc'
    storageAccountId: catalogStorage.outputs.storageAccountId
    privateDnsZoneId: catalogStorage.outputs.privateDnsZoneId
    vnetName: primaryVnetName
    subnetName: privateEndpointSubnetName
    tags: tags
  }
}

module secondaryPrivateEndpoint './modules/catalog-private-endpoint.bicep' = {
  name: 'fsp-phase4-catalog-pe-secondary'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    location: 'northeurope'
    name: 'pe-${storageAccountName}-blob-neu'
    storageAccountId: catalogStorage.outputs.storageAccountId
    privateDnsZoneId: catalogStorage.outputs.privateDnsZoneId
    vnetName: secondaryVnetName
    subnetName: privateEndpointSubnetName
    tags: tags
  }
}

module replicaPrivateEndpoint './modules/catalog-private-endpoint.bicep' = {
  name: 'fsp-phase4-replica-pe-secondary'
  scope: resourceGroup(targetSubscriptionId, secondaryResourceGroupName)
  params: {
    location: 'northeurope'
    name: 'pe-${replicaStorageAccountName}-blob-neu'
    storageAccountId: replicaStorage.outputs.storageAccountId
    privateDnsZoneId: catalogStorage.outputs.privateDnsZoneId
    vnetName: secondaryVnetName
    subnetName: privateEndpointSubnetName
    tags: tags
  }
}

output storageAccountName string = catalogStorage.outputs.storageAccountName
output storageAccountId string = catalogStorage.outputs.storageAccountId
output catalogContainerId string = catalogStorage.outputs.catalogContainerId
output publishedContainerId string = catalogStorage.outputs.publishedContainerId
output hybridStagingContainerId string = catalogStorage.outputs.hybridStagingContainerId
output replicaStorageAccountName string = replicaStorage.outputs.storageAccountName
output replicaContainerId string = replicaStorage.outputs.replicaContainerId
output primaryReaderClientId string = primaryReaderIdentity.outputs.clientId
output secondaryReaderClientId string = secondaryReaderIdentity.outputs.clientId
output secondaryIngressPublicIpName string = secondaryIngressEdge.outputs.publicIpName
output secondaryIngressPublicIpAddress string = secondaryIngressEdge.outputs.ipAddress
output hybridVmVnetId string = hybridVmVnetId
output primaryPrivateEndpointId string = primaryPrivateEndpoint.outputs.privateEndpointId
output secondaryPrivateEndpointId string = secondaryPrivateEndpoint.outputs.privateEndpointId
output replicaPrivateEndpointId string = replicaPrivateEndpoint.outputs.privateEndpointId
