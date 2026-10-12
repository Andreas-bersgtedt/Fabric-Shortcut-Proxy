targetScope = 'resourceGroup'

param location string
param identityName string
param aksName string
param namespace string
param serviceAccountName string
param tags object

resource aks 'Microsoft.ContainerService/managedClusters@2025-07-01' existing = {
  name: aksName
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: identityName
  location: location
  tags: tags
}

resource federation 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2024-11-30' = {
  parent: identity
  name: 'fsp-regional-reader'
  properties: {
    audiences: [
      'api://AzureADTokenExchange'
    ]
    issuer: aks.properties.oidcIssuerProfile.issuerURL
    subject: 'system:serviceaccount:${namespace}:${serviceAccountName}'
  }
}

output clientId string = identity.properties.clientId
output principalId string = identity.properties.principalId
