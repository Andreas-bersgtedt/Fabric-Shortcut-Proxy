param location string
param tags object
param vmName string
param dataDiskName string
param subnetId string
param nsgId string

param vmAdminUsername string = 'azureuser'
param vmSshPublicKey string

resource dataDisk 'Microsoft.Compute/disks@2023-10-02' = {
  name: dataDiskName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  properties: {
    creationData: {
      createOption: 'Empty'
    }
    diskSizeGB: 32
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2024-05-01' = {
  name: '${vmName}-nic'
  location: location
  tags: tags
  properties: {
    networkSecurityGroup: {
      id: nsgId
    }
    ipConfigurations: [
      {
        name: 'ipconfig1'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          subnet: {
            id: subnetId
          }
        }
      }
    ]
  }
}

resource vm 'Microsoft.Compute/virtualMachines@2024-11-01' = {
  name: vmName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    hardwareProfile: {
      vmSize: 'Standard_B2s'
    }
    osProfile: {
      computerName: vmName
      adminUsername: vmAdminUsername
      linuxConfiguration: {
        disablePasswordAuthentication: true
        provisionVMAgent: true
        ssh: {
          publicKeys: [
            {
              path: '/home/${vmAdminUsername}/.ssh/authorized_keys'
              keyData: vmSshPublicKey
            }
          ]
        }
      }
    }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: '0001-com-ubuntu-server-jammy'
        sku: '22_04-lts-gen2'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        diskSizeGB: 32
        deleteOption: 'Detach'
        managedDisk: {
          storageAccountType: 'Standard_LRS'
        }
      }
      dataDisks: [
        {
          lun: 0
          name: dataDisk.name
          createOption: 'Attach'
          caching: 'ReadWrite'
          deleteOption: 'Detach'
          managedDisk: {
            id: dataDisk.id
            storageAccountType: 'Standard_LRS'
          }
        }
      ]
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: nic.id
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

output vmName string = vm.name
output vmId string = vm.id
output vmPrincipalId string = vm.identity.principalId
output privateIp string = nic.properties.ipConfigurations[0].properties.privateIPAddress
