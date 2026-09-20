targetScope = 'resourceGroup'

param existingVnetName string
param remoteVnetId string
param peeringName string

resource existingVnet 'Microsoft.Network/virtualNetworks@2024-07-01' existing = {
  name: existingVnetName
}

resource reversePeering 'Microsoft.Network/virtualNetworks/virtualNetworkPeerings@2024-07-01' = {
  parent: existingVnet
  name: peeringName
  properties: {
    allowVirtualNetworkAccess: true
    allowForwardedTraffic: true
    allowGatewayTransit: false
    useRemoteGateways: false
    remoteVirtualNetwork: {
      id: remoteVnetId
    }
  }
}

output peeringId string = reversePeering.id
