param acrName string
param edgeKeyVaultName string
param regionalKeyVaultName string
param edgePrincipalId string
param regionalPrincipalId string
param deployerObjectId string

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' existing = {
  name: acrName
}

resource edgeKeyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: edgeKeyVaultName
}

resource regionalKeyVault 'Microsoft.KeyVault/vaults@2023-07-01' existing = {
  name: regionalKeyVaultName
}

var acrPullRoleId = '7f951dda-4ed3-4680-a7ca-43fe172d538d'
var keyVaultSecretsOfficerRoleId = 'b86a8fe4-44ce-4948-aee5-eccb2c155cd7'

resource edgeAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, edgePrincipalId, acrPullRoleId)
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
    principalId: edgePrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource regionalAcrPull 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(acr.id, regionalPrincipalId, acrPullRoleId)
  scope: acr
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', acrPullRoleId)
    principalId: regionalPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource edgeKeyVaultUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(edgeKeyVault.id, edgePrincipalId, keyVaultSecretsOfficerRoleId)
  scope: edgeKeyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
    principalId: edgePrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource regionalKeyVaultUser 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(regionalKeyVault.id, regionalPrincipalId, keyVaultSecretsOfficerRoleId)
  scope: regionalKeyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
    principalId: regionalPrincipalId
    principalType: 'ServicePrincipal'
  }
}

resource deployerEdgeKeyVaultOfficer 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(edgeKeyVault.id, deployerObjectId, keyVaultSecretsOfficerRoleId)
  scope: edgeKeyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
    principalId: deployerObjectId
    principalType: 'User'
  }
}
resource deployerRegionalKeyVaultOfficer 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(regionalKeyVault.id, deployerObjectId, keyVaultSecretsOfficerRoleId)
  scope: regionalKeyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsOfficerRoleId)
    principalId: deployerObjectId
    principalType: 'User'
  }
}
