targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Name of the disposable test virtual machine.')
param vmName string = 'fsp-linux-test'

@description('Administrator account used only by Azure Run Command and break-glass SSH.')
param adminUsername string = 'azureuser'

@secure()
@description('SSH public key for the administrator account. Do not commit private keys.')
param adminSshPublicKey string

@description('VM SKU for Linux compilation and test execution.')
param vmSize string = 'Standard_B2s'

@description('User-assigned identity used by the simulated on-premises Agent.')
param hybridIdentityName string = 'id-fsp-hybrid-site'

@description('Subscription containing the existing FSP artifact storage account.')
param artifactSubscriptionId string = subscription().subscriptionId

@description('Resource group containing the existing FSP artifact storage account.')
param artifactStorageResourceGroup string = 'FSP_Demo_xdev_swedencentral'

@description('Existing FSP artifact storage account.')
param artifactStorageAccountName string = 'stfspcat${take(uniqueString(subscription().subscriptionId, 'phase4-catalog'), 13)}'

@description('Dedicated staging container writable by only the simulated on-premises pool.')
param artifactStagingContainerName string = 'fsp-hybrid-staging'

var networkName = '${vmName}-vnet'
var subnetName = 'workload'
var networkSecurityGroupName = '${vmName}-nsg'
var networkInterfaceName = '${vmName}-nic'

resource networkSecurityGroup 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: networkSecurityGroupName
  location: location
  properties: {
    securityRules: []
  }
}

resource virtualNetwork 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: networkName
  location: location
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.42.0.0/16'
      ]
    }
    subnets: [
      {
        name: subnetName
        properties: {
          addressPrefix: '10.42.1.0/24'
          networkSecurityGroup: {
            id: networkSecurityGroup.id
          }
        }
      }
    ]
  }
}

resource networkInterface 'Microsoft.Network/networkInterfaces@2024-05-01' = {
  name: networkInterfaceName
  location: location
  properties: {
    ipConfigurations: [
      {
        name: 'primary'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          subnet: {
            id: virtualNetwork.properties.subnets[0].id
          }
        }
      }
    ]
  }
}

resource hybridIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2024-11-30' = {
  name: hybridIdentityName
  location: location
}

resource virtualMachine 'Microsoft.Compute/virtualMachines@2024-07-01' = {
  name: vmName
  location: location
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${hybridIdentity.id}': {}
    }
  }
  properties: {
    hardwareProfile: {
      vmSize: vmSize
    }
    securityProfile: {
      encryptionAtHost: true
      securityType: 'TrustedLaunch'
      uefiSettings: {
        secureBootEnabled: true
        vTpmEnabled: true
      }
    }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: 'ubuntu-24_04-lts'
        sku: 'server'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: 'StandardSSD_LRS'
        }
      }
    }
    osProfile: {
      computerName: vmName
      adminUsername: adminUsername
      linuxConfiguration: {
        disablePasswordAuthentication: true
        patchSettings: {
          assessmentMode: 'AutomaticByPlatform'
          patchMode: 'AutomaticByPlatform'
        }
        ssh: {
          publicKeys: [
            {
              path: '/home/${adminUsername}/.ssh/authorized_keys'
              keyData: adminSshPublicKey
            }
          ]
        }
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: networkInterface.id
          properties: {
            primary: true
          }
        }
      ]
    }
    diagnosticsProfile: {
      bootDiagnostics: {
        enabled: true
      }
    }
  }
}

module hybridStaging './modules/hybrid-staging.bicep' = {
  name: 'fsp-hybrid-staging'
  scope: resourceGroup(artifactSubscriptionId, artifactStorageResourceGroup)
  params: {
    storageAccountName: artifactStorageAccountName
    containerName: artifactStagingContainerName
    principalId: hybridIdentity.properties.principalId
  }
}

output privateIpAddress string = networkInterface.properties.ipConfigurations[0].properties.privateIPAddress
output hybridIdentityClientId string = hybridIdentity.properties.clientId
output hybridIdentityPrincipalId string = hybridIdentity.properties.principalId
output artifactStagingContainerId string = hybridStaging.outputs.containerId
