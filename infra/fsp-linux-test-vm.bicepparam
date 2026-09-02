using './fsp-linux-test-vm.bicep'

param location = 'swedencentral'
param vmName = 'fsp-linux-test'
param adminUsername = 'azureuser'
param adminSshPublicKey = ''
param vmSize = 'Standard_B2s'
