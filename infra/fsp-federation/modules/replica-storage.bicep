targetScope = 'resourceGroup'

param location string
param storageAccountName string
param primaryManagerPrincipalId string
param secondaryManagerPrincipalId string
param secondaryReaderPrincipalId string
param tags object

var blobDataContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var blobDataReaderRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
)

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_ZRS'
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

resource replicaContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: 'replica'
  properties: {
    publicAccess: 'None'
  }
}

resource primaryReplicaWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(replicaContainer.id, primaryManagerPrincipalId, blobDataContributorRoleId)
  scope: replicaContainer
  properties: {
    roleDefinitionId: blobDataContributorRoleId
    principalId: primaryManagerPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource secondaryReplicaWriter 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(replicaContainer.id, secondaryManagerPrincipalId, blobDataContributorRoleId)
  scope: replicaContainer
  properties: {
    roleDefinitionId: blobDataContributorRoleId
    principalId: secondaryManagerPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource secondaryReplicaReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(replicaContainer.id, secondaryReaderPrincipalId, blobDataReaderRoleId)
  scope: replicaContainer
  properties: {
    roleDefinitionId: blobDataReaderRoleId
    principalId: secondaryReaderPrincipalId
    principalType: 'ServicePrincipal'
  }
}

output storageAccountName string = storageAccount.name
output storageAccountId string = storageAccount.id
output replicaContainerId string = replicaContainer.id
