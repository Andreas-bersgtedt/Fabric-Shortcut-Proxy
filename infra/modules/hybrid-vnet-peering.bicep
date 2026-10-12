targetScope = 'resourceGroup'

@description('Name of the existing local virtual network.')
param localVnetName string

@description('Resource ID of the existing remote virtual network.')
param remoteVirtualNetworkResourceId string

@description('Stable name for this side of the VNet peering.')
param peeringName string

resource localVnet 'Microsoft.Network/virtualNetworks@2024-07-01' existing = {
  name: localVnetName
}

module peering 'br/public:avm/res/network/virtual-network/virtual-network-peering:0.2.0' = {
  name: 'fsp-hybrid-vnet-peering'
  params: {
    localVnetName: localVnet.name
    remoteVirtualNetworkResourceId: remoteVirtualNetworkResourceId
    name: peeringName
    allowVirtualNetworkAccess: true
    allowForwardedTraffic: false
    allowGatewayTransit: false
    useRemoteGateways: false
    doNotVerifyRemoteGateways: true
    enableTelemetry: false
  }
}
