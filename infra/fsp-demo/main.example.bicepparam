using './main.bicep'

param location = 'swedencentral'
param targetSubscriptionId = '00000000-0000-0000-0000-000000000001'
param tenantId = '00000000-0000-0000-0000-000000000002'
param resourceNames = {
	primaryResourceGroup: 'FSP_Demo_example_swedencentral'
	nodeResourceGroup: 'FSP_Demo_example_swedencentral_aks_nodes'
	existingOpdgResourceGroup: 'existing-network-rg'
	existingOpdgVnet: 'existing-vnet'
	vnet: 'vnet-fspdemo-example-swc'
	systemSubnet: 'snet-aks-system'
	appSubnet: 'snet-aks-app'
	privateEndpointSubnet: 'snet-private-endpoints'
	aks: 'aks-fspdemo-example-swc'
	acrPrefix: 'acrfspdemoexample'
	keyVaultPrefix: 'kv-fspdemo-'
	storageAccountPrefix: 'stfspdemo'
	sqlServerPrefix: 'sql-fspdemo-example-'
	workloadIdentity: 'id-fspdemo-workload'
	logAnalytics: 'log-fspdemo-example-swc'
	publicIp: 'pip-fspdemo-nginx'
	sqlDatabase: 'fspdemo'
}
param networkConfig = {
	addressSpace: '10.240.0.0/16'
	systemSubnetPrefix: '10.240.0.0/22'
	appSubnetPrefix: '10.240.4.0/22'
	privateEndpointSubnetPrefix: '10.240.8.0/24'
	podCidr: '10.244.0.0/16'
	serviceCidr: '10.2.0.0/16'
	dnsServiceIp: '10.2.0.10'
	privateHostname: 'fsp.example.com'
	privateLoadBalancerIp: '10.240.4.100'
	sourceIngressCidr: '10.0.0.0/24'
}
param aksConfig = {
	systemNodeCount: 3
	appNodeCount: 3
	systemVmSize: 'Standard_D2s_v5'
	appVmSize: 'Standard_D4ds_v5'
	zones: [
		'1'
		'2'
		'3'
	]
	maxPods: 110
}
param existingSqlPrivateEndpointTargets = [
	{
		name: 'pe-sql-existing'
		resourceId: '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/data/providers/Microsoft.Sql/servers/example'
		groupId: 'sqlServer'
	}
]
param existingSqlMiPrivateEndpointTargets = [
	{
		name: 'pe-sqlmi-existing'
		resourceId: '/subscriptions/00000000-0000-0000-0000-000000000001/resourceGroups/data/providers/Microsoft.Sql/managedInstances/example'
		groupId: 'managedInstance'
	}
]
param sqlAdministrator = {
	login: 'fsp-demo-entra-admin'
	principalType: 'User'
	objectId: '00000000-0000-0000-0000-000000000003'
}
param tags = {
	Application: 'Fabric-Shortcut-Proxy'
	Environment: 'Demo'
	Tenant: 'example'
	ManagedBy: 'Bicep'
}
param acrBootstrapPublicNetworkAccess = 'Enabled'
param keyVaultBootstrapPublicNetworkAccess = 'Enabled'
param storageBootstrapPublicNetworkAccess = 'Enabled'
param deploySqlMiPrivateEndpoint = false
