targetScope = 'resourceGroup'

param location string
param publicIpName string
param aksName string
param tags object

var networkContributorRoleId = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '4d97b98b-1d4f-4787-a291-c67834d212e7'
)

resource aks 'Microsoft.ContainerService/managedClusters@2025-07-01' existing = {
  name: aksName
}

resource publicIp 'Microsoft.Network/publicIPAddresses@2024-07-01' = {
  name: publicIpName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Regional'
  }
  zones: [
    '1'
    '2'
    '3'
  ]
  properties: {
    publicIPAllocationMethod: 'Static'
    publicIPAddressVersion: 'IPv4'
    idleTimeoutInMinutes: 4
    deleteOption: 'Detach'
  }
}

resource ingressNetworkContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(publicIp.id, aks.id, networkContributorRoleId)
  scope: publicIp
  properties: {
    roleDefinitionId: networkContributorRoleId
    principalId: aks.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output publicIpName string = publicIp.name
output publicIpId string = publicIp.id
output ipAddress string = publicIp.properties.ipAddress
