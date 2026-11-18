param location string
param tags object

resource acr 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: 'crfspedgelab349e'
  location: location
  tags: tags
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: false
    publicNetworkAccess: 'Enabled'
  }
}

output acrName string = acr.name
output acrId string = acr.id
output acrLoginServer string = acr.properties.loginServer
