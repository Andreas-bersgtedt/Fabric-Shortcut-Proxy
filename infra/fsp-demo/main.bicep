targetScope = 'subscription'

type ResourceNames = {
  primaryResourceGroup: string
  nodeResourceGroup: string
  existingOpdgResourceGroup: string
  existingOpdgVnet: string
  vnet: string
  systemSubnet: string
  appSubnet: string
  privateEndpointSubnet: string
  aks: string
  acrPrefix: string
  keyVaultPrefix: string
  storageAccountPrefix: string
  sqlServerPrefix: string
  workloadIdentity: string
  logAnalytics: string
  publicIp: string
  sqlDatabase: string
}

type NetworkConfig = {
  addressSpace: string
  systemSubnetPrefix: string
  appSubnetPrefix: string
  privateEndpointSubnetPrefix: string
  podCidr: string
  serviceCidr: string
  dnsServiceIp: string
  privateHostname: string
  privateLoadBalancerIp: string
  sourceIngressCidr: string
}

type AksConfig = {
  systemNodeCount: int
  appNodeCount: int
  systemVmSize: string
  appVmSize: string
  zones: string[]
  maxPods: int
}

type PrivateEndpointTarget = {
  name: string
  resourceId: string
  groupId: string
}

type SqlAdministrator = {
  login: string
  principalType: string
  objectId: string
}

@description('Azure region for the FSP enterprise demo.')
param location string = 'swedencentral'

@description('Subscription that owns the FSP demo deployment and existing source resources.')
param targetSubscriptionId string

@description('Microsoft Entra tenant that owns the deployment.')
param tenantId string

@description('Names for created resources and the existing OPDG network.')
param resourceNames ResourceNames

@description('Address ranges and private FSP endpoint settings.')
param networkConfig NetworkConfig

@description('AKS node pool sizing and availability zones.')
param aksConfig AksConfig

@description('Existing Azure SQL servers to connect through private endpoints.')
param existingSqlPrivateEndpointTargets PrivateEndpointTarget[] = []

@description('Existing SQL Managed Instances to connect when deploySqlMiPrivateEndpoint is true.')
param existingSqlMiPrivateEndpointTargets PrivateEndpointTarget[] = []

@description('Microsoft Entra-only administrator for the demo Azure SQL server.')
param sqlAdministrator SqlAdministrator

@description('Tags applied to demo resources.')
param tags object = {
  Application: 'Fabric-Shortcut-Proxy'
  Environment: 'Demo'
  ManagedBy: 'Bicep'
}

@description('Allow public ACR data-plane access during bootstrap image pushes.')
param acrBootstrapPublicNetworkAccess string = 'Enabled'

@description('Allow public Key Vault data-plane access during bootstrap.')
param keyVaultBootstrapPublicNetworkAccess string = 'Enabled'

@description('Allow public storage data-plane access during bootstrap.')
param storageBootstrapPublicNetworkAccess string = 'Enabled'

@description('Create the SQL Managed Instance private endpoint after the stopped instance has started.')
param deploySqlMiPrivateEndpoint bool = false

resource primaryResourceGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: resourceNames.primaryResourceGroup
  location: location
  tags: tags
}

module platform './modules/platform.bicep' = {
  name: 'fsp-demo-platform'
  scope: primaryResourceGroup
  params: {
    location: location
    tenantId: tenantId
    nodeResourceGroupName: resourceNames.nodeResourceGroup
    existingOpdgVnetId: resourceId(targetSubscriptionId, resourceNames.existingOpdgResourceGroup, 'Microsoft.Network/virtualNetworks', resourceNames.existingOpdgVnet)
    resourceNames: resourceNames
    networkConfig: networkConfig
    aksConfig: aksConfig
    existingSqlPrivateEndpointTargets: existingSqlPrivateEndpointTargets
    existingSqlMiPrivateEndpointTargets: existingSqlMiPrivateEndpointTargets
    sqlAdministrator: sqlAdministrator
    acrBootstrapPublicNetworkAccess: acrBootstrapPublicNetworkAccess
    keyVaultBootstrapPublicNetworkAccess: keyVaultBootstrapPublicNetworkAccess
    storageBootstrapPublicNetworkAccess: storageBootstrapPublicNetworkAccess
    deploySqlMiPrivateEndpoint: deploySqlMiPrivateEndpoint
    tags: tags
  }
}

module reversePeering './modules/external-vnet-peering.bicep' = {
  name: 'fsp-demo-opdg-reverse-peering'
  scope: resourceGroup(targetSubscriptionId, resourceNames.existingOpdgResourceGroup)
  params: {
    existingVnetName: resourceNames.existingOpdgVnet
    remoteVnetId: platform.outputs.vnetId
    peeringName: 'peer-opdg-to-fspdemo'
  }
}

output aksKubeletObjectId string = platform.outputs.aksKubeletObjectId
output aksClusterPrincipalId string = platform.outputs.aksClusterPrincipalId
output workloadIdentityPrincipalId string = platform.outputs.workloadIdentityPrincipalId
output acrId string = platform.outputs.acrId
output keyVaultId string = platform.outputs.keyVaultId
output vnetId string = platform.outputs.vnetId
output subnetIds object = platform.outputs.subnetIds
output nginxPublicIpAddress string = platform.outputs.nginxPublicIpAddress
output nginxPublicIpId string = platform.outputs.nginxPublicIpId
output storageAccountName string = platform.outputs.storageAccountName
output sqlFqdn string = platform.outputs.sqlFqdn
output privateDnsZoneIds object = platform.outputs.privateDnsZoneIds
output privateEndpointIds object = platform.outputs.privateEndpointIds
output deploymentContract object = platform.outputs.deploymentContract
