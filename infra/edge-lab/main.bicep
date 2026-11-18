targetScope = 'subscription'

@minLength(1)
param environmentName string

@minLength(1)
param location string

param sessionId string
param deployedBy string
param createdAt string
param deployerObjectId string
param vmAdminUsername string = 'azureuser'

param vmSshPublicKey string

var tags = {
  'app-onboard-skill': 'true'
  'app-onboard-session-id': sessionId
  'created-at': createdAt
  environment: environmentName
  'deployed-by': deployedBy
}

resource rg 'Microsoft.Resources/resourceGroups@2023-07-01' = {
  name: 'rg-fsp-edge-lab-dev-349e'
  location: location
  tags: tags
}

module network './modules/network.bicep' = {
  name: 'edgeLabNetwork'
  scope: rg
  params: {
    location: location
    tags: tags
  }
}

module acr './modules/acr.bicep' = {
  name: 'edgeLabAcr'
  scope: rg
  params: {
    location: location
    tags: tags
  }
}

module edgeKeyVault './modules/keyvault.bicep' = {
  name: 'edgeLabEdgeKeyVault'
  scope: rg
  params: {
    location: location
    tags: tags
    keyVaultName: 'kv-edge-fspedge349e'
    privateEndpointSubnetId: network.outputs.edgeSubnetId
    privateDnsZoneId: network.outputs.vaultPrivateDnsZoneId
  }
}

module regionalKeyVault './modules/keyvault.bicep' = {
  name: 'edgeLabRegionalKeyVault'
  scope: rg
  params: {
    location: location
    tags: tags
    keyVaultName: 'kv-region-fspedge349e'
    privateEndpointSubnetId: network.outputs.regionalSubnetId
    privateDnsZoneId: network.outputs.vaultPrivateDnsZoneId
  }
}

module regionalVm './modules/vm.bicep' = {
  name: 'edgeLabRegionalVm'
  scope: rg
  params: {
    location: location
    tags: tags
    vmName: 'vm-regional-fsp-edge-lab-dev-349e'
    dataDiskName: 'disk-regional-fsp-edge-lab-dev-349e'
    subnetId: network.outputs.regionalSubnetId
    nsgId: network.outputs.regionalNsgId
    vmSshPublicKey: vmSshPublicKey
    vmAdminUsername: vmAdminUsername
  }
}

module edgeVm './modules/vm.bicep' = {
  name: 'edgeLabEdgeVm'
  scope: rg
  params: {
    location: location
    tags: tags
    vmName: 'vm-edge-fsp-edge-lab-dev-349e'
    dataDiskName: 'disk-edge-fsp-edge-lab-dev-349e'
    subnetId: network.outputs.edgeSubnetId
    nsgId: network.outputs.edgeNsgId
    vmSshPublicKey: vmSshPublicKey
    vmAdminUsername: vmAdminUsername
  }
}

module roleAssignments './modules/role-assignments.bicep' = {
  name: 'edgeLabRoleAssignments'
  scope: rg
  params: {
    acrName: acr.outputs.acrName
    edgeKeyVaultName: edgeKeyVault.outputs.keyVaultName
    regionalKeyVaultName: regionalKeyVault.outputs.keyVaultName
    edgePrincipalId: edgeVm.outputs.vmPrincipalId
    regionalPrincipalId: regionalVm.outputs.vmPrincipalId
    deployerObjectId: deployerObjectId
  }
}

output resourceGroupName string = rg.name
output acrName string = acr.outputs.acrName
output acrLoginServer string = acr.outputs.acrLoginServer
output edgeKeyVaultName string = edgeKeyVault.outputs.keyVaultName
output regionalKeyVaultName string = regionalKeyVault.outputs.keyVaultName
output regionalVmName string = regionalVm.outputs.vmName
output edgeVmName string = edgeVm.outputs.vmName
output regionalPrivateIp string = regionalVm.outputs.privateIp
output edgePrivateIp string = edgeVm.outputs.privateIp
