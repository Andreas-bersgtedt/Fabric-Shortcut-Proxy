targetScope = 'resourceGroup'

@description('Subscription containing the storage account and referenced network resources.')
param targetSubscriptionId string

param location string
param storageAccountName string
param primaryManagerPrincipalId string
param secondaryManagerPrincipalId string
param primaryVnetId string
param secondaryVnetId string
param hybridVmVnetId string
param blobPrivateDnsZoneResourceGroupName string
param blobPrivateDnsZoneName string
param tags object

var blobDataContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_RAGZRS'
  }
  kind: 'StorageV2'
  properties: {
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Disabled'
    supportsHttpsTrafficOnly: true
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: 'Deny'
      ipRules: []
      virtualNetworkRules: []
    }
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
  properties: {
    deleteRetentionPolicy: {
      enabled: true
      days: 14
    }
    containerDeleteRetentionPolicy: {
      enabled: true
      days: 14
    }
  }
}

resource catalogContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'catalog'
  properties: {
    publicAccess: 'None'
  }
}

resource hybridStagingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'fsp-hybrid-staging'
  properties: {
    publicAccess: 'None'
  }
}

resource primaryCatalogWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(catalogContainer.id, primaryManagerPrincipalId, blobDataContributorRoleId)
  scope: catalogContainer
  properties: {
    roleDefinitionId: blobDataContributorRoleId
    principalId: primaryManagerPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource secondaryCatalogWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(catalogContainer.id, secondaryManagerPrincipalId, blobDataContributorRoleId)
  scope: catalogContainer
  properties: {
    roleDefinitionId: blobDataContributorRoleId
    principalId: secondaryManagerPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource blobPrivateDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' existing = {
  scope: resourceGroup(targetSubscriptionId, blobPrivateDnsZoneResourceGroupName)
  name: blobPrivateDnsZoneName
}

module primaryDnsLink 'br/public:avm/res/network/private-dns-zone/virtual-network-link:0.1.0' = {
  name: 'fsp-primary-blob-dns-link'
  scope: resourceGroup(targetSubscriptionId, blobPrivateDnsZoneResourceGroupName)
  params: {
    privateDnsZoneName: blobPrivateDnsZoneName
    virtualNetworkResourceId: primaryVnetId
    name: 'link-fsp-primary'
    registrationEnabled: false
    enableTelemetry: false
  }
}

module secondaryDnsLink 'br/public:avm/res/network/private-dns-zone/virtual-network-link:0.1.0' = {
  name: 'fsp-secondary-blob-dns-link'
  scope: resourceGroup(targetSubscriptionId, blobPrivateDnsZoneResourceGroupName)
  params: {
    privateDnsZoneName: blobPrivateDnsZoneName
    virtualNetworkResourceId: secondaryVnetId
    name: 'link-fsp-secondary'
    registrationEnabled: false
    enableTelemetry: false
  }
}

module hybridVmDnsLink 'br/public:avm/res/network/private-dns-zone/virtual-network-link:0.1.0' = {
  name: 'fsp-hybrid-vm-blob-dns-link'
  scope: resourceGroup(targetSubscriptionId, blobPrivateDnsZoneResourceGroupName)
  params: {
    privateDnsZoneName: blobPrivateDnsZoneName
    virtualNetworkResourceId: hybridVmVnetId
    name: 'link-fsp-hybrid-vm'
    registrationEnabled: false
    enableTelemetry: false
  }
}

output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output catalogContainerId string = catalogContainer.id
output hybridStagingContainerId string = hybridStagingContainer.id
output privateDnsZoneId string = blobPrivateDnsZone.id
