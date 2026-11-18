@description('Azure region for the lab')
param location string

param tags object

var regionalSubnetPrefix = '10.154.1.0/24'
var edgeSubnetPrefix = '10.154.2.0/24'

resource regionalNsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: 'nsg-regional-fsp-edge-lab-dev-349e'
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'AllowEdgeReplay9443'
        properties: {
          priority: 100
          access: 'Allow'
          direction: 'Inbound'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '9443'
          sourceAddressPrefix: edgeSubnetPrefix
          destinationAddressPrefix: regionalSubnetPrefix
        }
      }
    ]
  }
}

resource edgeNsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: 'nsg-edge-fsp-edge-lab-dev-349e'
  location: location
  tags: tags
  properties: {
    securityRules: [
      {
        name: 'AllowRegionalReplay9443'
        properties: {
          priority: 100
          access: 'Allow'
          direction: 'Outbound'
          protocol: 'Tcp'
          sourcePortRange: '*'
          destinationPortRange: '9443'
          sourceAddressPrefix: edgeSubnetPrefix
          destinationAddressPrefix: regionalSubnetPrefix
        }
      }
    ]
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: 'vnet-fsp-edge-lab-dev-349e'
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.154.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'regional'
        properties: {
          addressPrefix: regionalSubnetPrefix
          networkSecurityGroup: {
            id: regionalNsg.id
          }
        }
      }
      {
        name: 'edge'
        properties: {
          addressPrefix: edgeSubnetPrefix
          networkSecurityGroup: {
            id: edgeNsg.id
          }
        }
      }
    ]
  }
}

resource vaultPrivateDnsZone 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.vaultcore.azure.net'
  location: 'global'
  tags: tags
}

resource vaultPrivateDnsLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  name: 'fsp-edge-lab-vault-link'
  parent: vaultPrivateDnsZone
  location: 'global'
  tags: tags
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}

output regionalSubnetId string = vnet.properties.subnets[0].id
output edgeSubnetId string = vnet.properties.subnets[1].id
output regionalNsgId string = regionalNsg.id
output edgeNsgId string = edgeNsg.id
output vaultPrivateDnsZoneId string = vaultPrivateDnsZone.id
