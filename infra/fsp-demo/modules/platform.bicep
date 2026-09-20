targetScope = 'resourceGroup'

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

param location string
param tenantId string
param nodeResourceGroupName string
param existingOpdgVnetId string
param resourceNames ResourceNames
param networkConfig NetworkConfig
param aksConfig AksConfig
param existingSqlPrivateEndpointTargets PrivateEndpointTarget[]
param existingSqlMiPrivateEndpointTargets PrivateEndpointTarget[]
param sqlAdministrator SqlAdministrator
@allowed([
  'Enabled'
  'Disabled'
])
param acrBootstrapPublicNetworkAccess string
@allowed([
  'Enabled'
  'Disabled'
])
param keyVaultBootstrapPublicNetworkAccess string
@allowed([
  'Enabled'
  'Disabled'
])
param storageBootstrapPublicNetworkAccess string
param deploySqlMiPrivateEndpoint bool
param tags object

var suffix = take(uniqueString(subscription().subscriptionId, resourceGroup().id), 8)
var vnetName = resourceNames.vnet
var systemSubnetName = resourceNames.systemSubnet
var appSubnetName = resourceNames.appSubnet
var privateEndpointSubnetName = resourceNames.privateEndpointSubnet
var systemSubnetId = resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, systemSubnetName)
var appSubnetId = resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, appSubnetName)
var privateEndpointSubnetId = resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, privateEndpointSubnetName)
var aksName = resourceNames.aks
var acrName = '${resourceNames.acrPrefix}${suffix}'
var keyVaultName = '${resourceNames.keyVaultPrefix}${suffix}'
var storageAccountName = '${resourceNames.storageAccountPrefix}${suffix}'
var sqlServerName = '${resourceNames.sqlServerPrefix}${suffix}'
var workloadIdentityName = resourceNames.workloadIdentity
var networkContributorRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4d97b98b-1d4f-4787-a291-c67834d212e7')
var keyVaultSecretsUserRoleDefinitionId = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '4633458b-17de-408a-b874-0445c86b69e6')
var fspPrivateHostname = networkConfig.privateHostname
var fspPrivateLoadBalancerIp = networkConfig.privateLoadBalancerIp
var privateDnsZoneNames = {
  acr: 'privatelink${environment().suffixes.acrLoginServer}'
  keyVault: 'privatelink${replace(environment().suffixes.keyvaultDns, '.vault.', '.vaultcore.')}'
  file: 'privatelink.file.${environment().suffixes.storage}'
  sql: 'privatelink${environment().suffixes.sqlServerHostname}'
  sqlMi: 'privatelink.077a136e031e${environment().suffixes.sqlServerHostname}'
}
var sqlPrivateEndpointTargets = concat([
  {
    name: 'pe-sql-fspdemo'
    id: sqlServer.id
    groupId: 'sqlServer'
  }
], map(existingSqlPrivateEndpointTargets, target => {
  name: target.name
  id: target.resourceId
  groupId: target.groupId
}), deploySqlMiPrivateEndpoint ? map(existingSqlMiPrivateEndpointTargets, target => {
  name: target.name
  id: target.resourceId
  groupId: target.groupId
}) : [])

resource vnet 'Microsoft.Network/virtualNetworks@2024-07-01' = {
  name: vnetName
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        networkConfig.addressSpace
      ]
    }
    subnets: [
      {
        name: systemSubnetName
        properties: {
          addressPrefix: networkConfig.systemSubnetPrefix
        }
      }
      {
        name: appSubnetName
        properties: {
          addressPrefix: networkConfig.appSubnetPrefix
        }
      }
      {
        name: privateEndpointSubnetName
        properties: {
          addressPrefix: networkConfig.privateEndpointSubnetPrefix
          privateEndpointNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

resource localPeering 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: vnet
  name: 'peer-fspdemo-to-opdg'
  properties: {
    allowVirtualNetworkAccess: true
    allowForwardedTraffic: true
    allowGatewayTransit: false
    useRemoteGateways: false
    remoteVirtualNetwork: {
      id: existingOpdgVnetId
    }
  }
}

resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: resourceNames.logAnalytics
  location: location
  tags: tags
  properties: {
    retentionInDays: 30
    features: {
      enableLogAccessUsingOnlyResourcePermissions: true
    }
  }
}

resource aks 'Microsoft.ContainerService/managedClusters@2025-05-01' = {
  name: aksName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  sku: {
    name: 'Base'
    tier: 'Standard'
  }
  properties: {
    dnsPrefix: aksName
    nodeResourceGroup: nodeResourceGroupName
    enableRBAC: true
    disableLocalAccounts: false
    aadProfile: {
      managed: true
      enableAzureRBAC: true
      tenantID: tenantId
    }
    apiServerAccessProfile: {
      enablePrivateCluster: true
      enablePrivateClusterPublicFQDN: false
      privateDNSZone: 'system'
    }
    oidcIssuerProfile: {
      enabled: true
    }
    securityProfile: {
      workloadIdentity: {
        enabled: true
      }
    }
    networkProfile: {
      networkPlugin: 'azure'
      networkPluginMode: 'overlay'
      networkPolicy: 'azure'
      loadBalancerSku: 'standard'
      outboundType: 'loadBalancer'
      podCidr: networkConfig.podCidr
      serviceCidr: networkConfig.serviceCidr
      dnsServiceIP: networkConfig.dnsServiceIp
    }
    agentPoolProfiles: [
      {
        name: 'system'
        mode: 'System'
        count: aksConfig.systemNodeCount
        vmSize: aksConfig.systemVmSize
        osType: 'Linux'
        osSKU: 'AzureLinux'
        type: 'VirtualMachineScaleSets'
        availabilityZones: aksConfig.zones
        vnetSubnetID: systemSubnetId
        maxPods: aksConfig.maxPods
        nodeLabels: {
          'node.kubernetes.io/exclude-from-external-load-balancers': 'true'
        }
      }
      {
        name: 'app'
        mode: 'User'
        count: aksConfig.appNodeCount
        vmSize: aksConfig.appVmSize
        osType: 'Linux'
        osSKU: 'AzureLinux'
        type: 'VirtualMachineScaleSets'
        availabilityZones: aksConfig.zones
        vnetSubnetID: appSubnetId
        maxPods: aksConfig.maxPods
      }
    ]
    addonProfiles: {
      omsagent: {
        enabled: true
        config: {
          logAnalyticsWorkspaceResourceID: logAnalytics.id
          useAADAuth: 'true'
        }
      }
    }
    autoUpgradeProfile: {
      upgradeChannel: 'patch'
      nodeOSUpgradeChannel: 'NodeImage'
    }
  }
}

resource systemSubnet 'Microsoft.Network/virtualNetworks/subnets@2024-07-01' existing = {
  parent: vnet
  name: systemSubnetName
}

resource aksSystemSubnetNetworkContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(systemSubnet.id, aks.id, networkContributorRoleDefinitionId)
  scope: systemSubnet
  properties: {
    principalId: aks.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: networkContributorRoleDefinitionId
  }
}

resource workloadIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: workloadIdentityName
  location: location
  tags: tags
}

resource workloadFederatedCredential 'Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials@2024-11-30' = {
  parent: workloadIdentity
  name: 'fsp-workload'
  properties: {
    audiences: [
      'api://AzureADTokenExchange'
    ]
    issuer: aks.properties.oidcIssuerProfile.issuerURL
    subject: 'system:serviceaccount:fabric-shortcut-proxy:fsp-workload'
  }
}

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: acrName
  location: location
  tags: tags
  sku: {
    name: 'Premium'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: acrBootstrapPublicNetworkAccess
    networkRuleBypassOptions: 'AzureServices'
    policies: {
      retentionPolicy: {
        days: 7
        status: 'enabled'
      }
      trustPolicy: {
        type: 'Notary'
        status: 'disabled'
      }
    }
  }
}

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    accessPolicies: []
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
    publicNetworkAccess: keyVaultBootstrapPublicNetworkAccess
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: keyVaultBootstrapPublicNetworkAccess == 'Enabled' ? 'Allow' : 'Deny'
    }
  }
}

resource workloadIdentityKeyVaultSecretsUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, workloadIdentity.id, keyVaultSecretsUserRoleDefinitionId)
  scope: keyVault
  properties: {
    principalId: workloadIdentity.properties.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: keyVaultSecretsUserRoleDefinitionId
  }
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Premium_ZRS'
  }
  kind: 'FileStorage'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowCrossTenantReplication: false
    allowSharedKeyAccess: false
    publicNetworkAccess: storageBootstrapPublicNetworkAccess
    networkAcls: {
      bypass: 'AzureServices'
      defaultAction: storageBootstrapPublicNetworkAccess == 'Enabled' ? 'Allow' : 'Deny'
    }
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource managerConfigShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = {
  parent: fileService
  name: 'manager-config'
  properties: {
    enabledProtocols: 'NFS'
    rootSquash: 'NoRootSquash'
    shareQuota: 100
  }
}

resource artifactsShare 'Microsoft.Storage/storageAccounts/fileServices/shares@2023-05-01' = {
  parent: fileService
  name: 'artifacts'
  properties: {
    enabledProtocols: 'NFS'
    rootSquash: 'NoRootSquash'
    shareQuota: 256
  }
}

resource nginxPublicIp 'Microsoft.Network/publicIPAddresses@2024-07-01' = {
  name: resourceNames.publicIp
  location: location
  zones: [
    '1'
    '2'
    '3'
  ]
  tags: tags
  sku: {
    name: 'Standard'
    tier: 'Regional'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    publicIPAddressVersion: 'IPv4'
  }
}

resource appSubnetNsg 'Microsoft.Network/networkSecurityGroups@2024-07-01' existing = {
  name: '${vnetName}-${appSubnetName}-nsg-${location}'
}

resource publicIngressRule 'Microsoft.Network/networkSecurityGroups/securityRules@2024-07-01' = {
  parent: appSubnetNsg
  name: 'AllowFspPublicIngress'
  properties: {
    priority: 500
    access: 'Allow'
    direction: 'Inbound'
    protocol: 'Tcp'
    sourcePortRange: '*'
    destinationPortRanges: [
      '80'
      '443'
    ]
    sourceAddressPrefix: 'Internet'
    destinationAddressPrefix: nginxPublicIp.properties.ipAddress
  }
  dependsOn: [
    aks
  ]
}

resource opdgPrivateIngressRule 'Microsoft.Network/networkSecurityGroups/securityRules@2024-07-01' = {
  parent: appSubnetNsg
  name: 'AllowOpdgFspPrivateHttps'
  properties: {
    priority: 510
    access: 'Allow'
    direction: 'Inbound'
    protocol: 'Tcp'
    sourcePortRange: '*'
    destinationPortRange: '443'
    sourceAddressPrefix: networkConfig.sourceIngressCidr
    destinationAddressPrefix: fspPrivateLoadBalancerIp
  }
  dependsOn: [
    aks
  ]
}

resource sqlServer 'Microsoft.Sql/servers@2023-08-01' = {
  name: sqlServerName
  location: location
  tags: tags
  properties: {
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Disabled'
    restrictOutboundNetworkAccess: 'Disabled'
    administrators: {
      administratorType: 'ActiveDirectory'
      azureADOnlyAuthentication: true
      login: sqlAdministrator.login
      principalType: sqlAdministrator.principalType
      sid: sqlAdministrator.objectId
      tenantId: tenantId
    }
  }
}

resource sqlDatabase 'Microsoft.Sql/servers/databases@2023-08-01' = {
  parent: sqlServer
  name: resourceNames.sqlDatabase
  location: location
  tags: tags
  sku: {
    name: 'S0'
    tier: 'Standard'
  }
  properties: {
    zoneRedundant: false
    readScale: 'Disabled'
  }
}

resource sqlShortTermRetention 'Microsoft.Sql/servers/databases/backupShortTermRetentionPolicies@2023-08-01' = {
  parent: sqlDatabase
  name: 'default'
  properties: {
    retentionDays: 7
    diffBackupIntervalInHours: 24
  }
}

resource acrDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneNames.acr
  location: 'global'
  tags: tags
}

resource keyVaultDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneNames.keyVault
  location: 'global'
  tags: tags
}

resource fileDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneNames.file
  location: 'global'
  tags: tags
}

resource sqlDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: privateDnsZoneNames.sql
  location: 'global'
  tags: tags
}

resource sqlMiDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = if (deploySqlMiPrivateEndpoint) {
  name: privateDnsZoneNames.sqlMi
  location: 'global'
  tags: tags
}

resource fspPrivateDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: fspPrivateHostname
  location: 'global'
  tags: tags
}

resource fspPrivateDnsRecord 'Microsoft.Network/privateDnsZones/A@2024-06-01' = {
  parent: fspPrivateDnsZone
  name: '@'
  properties: {
    ttl: 60
    aRecords: [
      {
        ipv4Address: fspPrivateLoadBalancerIp
      }
    ]
  }
}

resource acrDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: acrDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource acrDnsOpdgVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: acrDnsZone
  name: 'link-opdg-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: existingOpdgVnetId
    }
  }
}

resource keyVaultDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: keyVaultDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource fileDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: fileDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource fileDnsOpdgVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: fileDnsZone
  name: 'link-opdg-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: existingOpdgVnetId
    }
  }
}

resource sqlDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: sqlDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource sqlMiDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = if (deploySqlMiPrivateEndpoint) {
  parent: sqlMiDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource sqlMiDnsOpdgVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = if (deploySqlMiPrivateEndpoint) {
  parent: sqlMiDnsZone
  name: 'link-opdg-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: existingOpdgVnetId
    }
  }
}

resource fspPrivateDnsVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: fspPrivateDnsZone
  name: 'link-fspdemo-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

resource fspPrivateDnsOpdgVnetLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: fspPrivateDnsZone
  name: 'link-opdg-vnet'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: existingOpdgVnetId
    }
  }
}

resource acrPrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pe-${acrName}'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'acr'
        properties: {
          privateLinkServiceId: acr.id
          groupIds: [
            'registry'
          ]
        }
      }
    ]
  }
}

resource acrPrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: acrPrivateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'acr'
        properties: {
          privateDnsZoneId: acrDnsZone.id
        }
      }
    ]
  }
}

resource keyVaultPrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pe-${keyVaultName}'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'vault'
        properties: {
          privateLinkServiceId: keyVault.id
          groupIds: [
            'vault'
          ]
        }
      }
    ]
  }
}

resource keyVaultPrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: keyVaultPrivateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'keyVault'
        properties: {
          privateDnsZoneId: keyVaultDnsZone.id
        }
      }
    ]
  }
}

resource filePrivateEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pe-${storageAccountName}-file'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: 'file'
        properties: {
          privateLinkServiceId: storage.id
          groupIds: [
            'file'
          ]
        }
      }
    ]
  }
}

resource filePrivateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: filePrivateEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'file'
        properties: {
          privateDnsZoneId: fileDnsZone.id
        }
      }
    ]
  }
}

resource sqlPrivateEndpoints 'Microsoft.Network/privateEndpoints@2024-07-01' = [for target in sqlPrivateEndpointTargets: {
  name: target.name
  location: location
  tags: tags
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: target.groupId
        properties: {
          privateLinkServiceId: target.id
          groupIds: [
            target.groupId
          ]
        }
      }
    ]
  }
}]

resource sqlPrivateDnsZoneGroups 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = [for (target, index) in sqlPrivateEndpointTargets: {
  parent: sqlPrivateEndpoints[index]
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: target.groupId == 'managedInstance' ? 'sqlMi' : 'sql'
        properties: {
          privateDnsZoneId: resourceId('Microsoft.Network/privateDnsZones', target.groupId == 'managedInstance' ? privateDnsZoneNames.sqlMi : privateDnsZoneNames.sql)
        }
      }
    ]
  }
  dependsOn: [
    sqlDnsZone
    sqlMiDnsZone
  ]
}]

output aksKubeletObjectId string = aks.properties.identityProfile.kubeletidentity.objectId
output aksClusterPrincipalId string = aks.identity.principalId
output workloadIdentityPrincipalId string = workloadIdentity.properties.principalId
output acrId string = acr.id
output keyVaultId string = keyVault.id
output vnetId string = vnet.id
output subnetIds object = {
  system: systemSubnetId
  app: appSubnetId
  privateEndpoints: privateEndpointSubnetId
}
output nginxPublicIpAddress string = nginxPublicIp.properties.ipAddress
output nginxPublicIpId string = nginxPublicIp.id
output storageAccountName string = storage.name
output sqlFqdn string = sqlServer.properties.fullyQualifiedDomainName
output privateDnsZoneIds object = {
  acr: acrDnsZone.id
  keyVault: keyVaultDnsZone.id
  file: fileDnsZone.id
  sql: sqlDnsZone.id
}
output privateEndpointIds object = {
  acr: acrPrivateEndpoint.id
  keyVault: keyVaultPrivateEndpoint.id
  file: filePrivateEndpoint.id
  sql: map(sqlPrivateEndpoints, endpoint => endpoint.id)
}
output deploymentContract object = {
  resourceGroup: resourceGroup().name
  aksName: aks.name
  acrLoginServer: acr.properties.loginServer
  keyVaultUri: keyVault.properties.vaultUri
  workloadIdentityClientId: workloadIdentity.properties.clientId
  storageAccountName: storage.name
  artifactsShareName: artifactsShare.name
  managerConfigShareName: managerConfigShare.name
  appSubnetName: appSubnetName
  privateLoadBalancerIp: fspPrivateLoadBalancerIp
  publicIpAddress: nginxPublicIp.properties.ipAddress
  hostname: fspPrivateHostname
}
