<#
.SYNOPSIS
    Starts the FSP enterprise demo in dependency order and verifies readiness.

.DESCRIPTION
    Starts independent data-plane dependencies first (SQL Managed Instance and
    OPDG VM), waits for them and the Azure SQL database, then starts AKS and
    validates the Kubernetes workloads through the private cluster command API.

    The script is idempotent: resources already running are not restarted. It
    stores no credentials and deletes its temporary AKS admin kubeconfig.

.PARAMETER StatusOnly
    Report current state without starting resources.

.PARAMETER SkipWorkloadValidation
    Skip Kubernetes pod, Manager, and LoadBalancer checks after AKS starts.

.EXAMPLE
    .\Start-FspDemo.ps1

.EXAMPLE
    .\Start-FspDemo.ps1 -StatusOnly
#>
[CmdletBinding()]
param(
    [string]$ConfigurationFile = (Join-Path $PSScriptRoot "deployment.local.json"),
    [string]$SubscriptionId = "",
    [string]$TenantId = "",
    [string]$DemoResourceGroup = "",
    [string]$AksName = "",
    [string]$SqlServerName = "",
    [string]$SqlDatabaseName = "",
    [string]$SqlManagedInstanceResourceGroup = "",
    [string]$SqlManagedInstanceName = "",
    [string]$OpdgResourceGroup = "",
    [string]$OpdgVmName = "",
    [string]$Namespace = "",
    [string]$PublicIpName = "",
    [string]$VnetName = "",
    [string]$AppSubnetName = "",
    [int]$DependencyTimeoutMinutes = 120,
    [int]$AksTimeoutMinutes = 45,
    [int]$WorkloadTimeoutMinutes = 30,
    [switch]$StatusOnly,
    [switch]$SkipWorkloadValidation
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $ConfigurationFile -PathType Leaf)) {
    throw "Configuration file not found: $ConfigurationFile. Copy deployment.example.json to deployment.local.json."
}
$deploymentConfiguration = Get-Content $ConfigurationFile -Raw | ConvertFrom-Json

function Resolve-DeploymentValue {
    param(
        [string]$ExplicitValue,
        [Parameter(Mandatory)][string]$ConfigurationName
    )

    if (-not [string]::IsNullOrWhiteSpace($ExplicitValue)) {
        return $ExplicitValue
    }
    if ($ConfigurationName -notin $deploymentConfiguration.PSObject.Properties.Name) {
        throw "Deployment configuration value '$ConfigurationName' is required."
    }
    $configuredValue = [string]$deploymentConfiguration.$ConfigurationName
    if ([string]::IsNullOrWhiteSpace($configuredValue)) {
        throw "Deployment configuration value '$ConfigurationName' cannot be empty."
    }
    return $configuredValue
}

$SubscriptionId = Resolve-DeploymentValue $SubscriptionId "subscriptionId"
$TenantId = Resolve-DeploymentValue $TenantId "tenantId"
$DemoResourceGroup = Resolve-DeploymentValue $DemoResourceGroup "resourceGroup"
$AksName = Resolve-DeploymentValue $AksName "aksName"
$SqlServerName = Resolve-DeploymentValue $SqlServerName "sqlServerName"
$SqlDatabaseName = Resolve-DeploymentValue $SqlDatabaseName "sqlDatabaseName"
$SqlManagedInstanceResourceGroup = Resolve-DeploymentValue $SqlManagedInstanceResourceGroup "sqlManagedInstanceResourceGroup"
$SqlManagedInstanceName = Resolve-DeploymentValue $SqlManagedInstanceName "sqlManagedInstanceName"
$OpdgResourceGroup = Resolve-DeploymentValue $OpdgResourceGroup "opdgResourceGroup"
$OpdgVmName = Resolve-DeploymentValue $OpdgVmName "opdgVmName"
$Namespace = Resolve-DeploymentValue $Namespace "namespace"
$PublicIpName = Resolve-DeploymentValue $PublicIpName "publicIpName"
$VnetName = Resolve-DeploymentValue $VnetName "vnetName"
$AppSubnetName = Resolve-DeploymentValue $AppSubnetName "appSubnetName"

function Invoke-AzText {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $output = & az @Arguments --only-show-errors 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI failed: az $($Arguments -join ' ')`n$($output | Out-String)"
    }
    return ($output | Out-String).Trim()
}

function Invoke-AzJson {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $text = Invoke-AzText ($Arguments + @("--output", "json"))
    if (-not $text) {
        return $null
    }
    return $text | ConvertFrom-Json
}

function Wait-ForResource {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][scriptblock]$GetState,
        [Parameter(Mandatory)][scriptblock]$IsReady,
        [Parameter(Mandatory)][int]$TimeoutMinutes
    )

    $deadline = (Get-Date).AddMinutes($TimeoutMinutes)
    do {
        $state = & $GetState
        Write-Host ("[{0:HH:mm:ss}] {1}: {2}" -f (Get-Date), $Name, ($state | ConvertTo-Json -Compress))
        if (& $IsReady $state) {
            return $state
        }
        if ((Get-Date) -ge $deadline) {
            throw "Timed out after $TimeoutMinutes minutes waiting for $Name."
        }
        Start-Sleep -Seconds 20
    } while ($true)
}

function Get-SqlMiState {
    Invoke-AzJson @(
        "sql", "mi", "show",
        "--resource-group", $SqlManagedInstanceResourceGroup,
        "--name", $SqlManagedInstanceName,
        "--query", "{state:state,provisioningState:provisioningState}"
    )
}

function Get-OpdgState {
    Invoke-AzJson @(
        "vm", "get-instance-view",
        "--resource-group", $OpdgResourceGroup,
        "--name", $OpdgVmName,
        "--query", "{powerState:instanceView.statuses[?starts_with(code, 'PowerState/')].code|[0],provisioningState:provisioningState}"
    )
}

function Get-AksState {
    Invoke-AzJson @(
        "aks", "show",
        "--resource-group", $DemoResourceGroup,
        "--name", $AksName,
        "--query", "{powerState:powerState.code,provisioningState:provisioningState}"
    )
}

function Get-SqlDatabaseState {
    Invoke-AzJson @(
        "sql", "db", "show",
        "--resource-group", $DemoResourceGroup,
        "--server", $SqlServerName,
        "--name", $SqlDatabaseName,
        "--query", "{status:status,provisioningState:provisioningState}"
    )
}

function Test-IngressRoleAssignments {
    $cluster = Invoke-AzJson @("aks", "show", "--resource-group", $DemoResourceGroup, "--name", $AksName)
    $publicIpId = Invoke-AzText @(
        "network", "public-ip", "show",
        "--resource-group", $DemoResourceGroup,
        "--name", $PublicIpName,
        "--query", "id", "--output", "tsv"
    )
    $subnetId = Invoke-AzText @(
        "network", "vnet", "subnet", "show",
        "--resource-group", $DemoResourceGroup,
        "--vnet-name", $VnetName,
        "--name", $AppSubnetName,
        "--query", "id", "--output", "tsv"
    )
    $subnetNsgId = Invoke-AzText @(
        "network", "vnet", "subnet", "show",
        "--resource-group", $DemoResourceGroup,
        "--vnet-name", $VnetName,
        "--name", $AppSubnetName,
        "--query", "networkSecurityGroup.id", "--output", "tsv"
    )

    $allowedRoles = @("Network Contributor", "Contributor", "Owner")
    foreach ($scope in @($publicIpId, $subnetId, $subnetNsgId) | Where-Object { $_ }) {
        $assignments = @(Invoke-AzJson @(
            "role", "assignment", "list",
            "--assignee-object-id", $cluster.identity.principalId,
            "--scope", $scope,
            "--include-inherited",
            "--query", "[].roleDefinitionName"
        ))
        if (-not ($assignments | Where-Object { $_ -in $allowedRoles })) {
            Write-Warning "AKS identity $($cluster.identity.principalId) lacks Network Contributor on $scope. The corresponding LoadBalancer may remain pending or unreachable."
        }
    }
}

function Invoke-AksCommand {
    param(
        [Parameter(Mandatory)][string]$Command,
        [Parameter(Mandatory)][string]$KubeconfigDirectory
    )

    Push-Location $KubeconfigDirectory
    try {
        Invoke-AzText @(
            "aks", "command", "invoke",
            "--resource-group", $DemoResourceGroup,
            "--name", $AksName,
            "--command", $Command,
            "--file", "admin-kubeconfig",
            "--query", "logs",
            "--output", "tsv"
        )
    }
    finally {
        Pop-Location
    }
}

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required."
}

$account = Invoke-AzJson @("account", "show", "--query", "{tenantId:tenantId,id:id,user:user.name}")
if ($account.tenantId -ne $TenantId) {
    throw "Azure CLI is signed into tenant $($account.tenantId); expected $TenantId."
}
Invoke-AzText @("account", "set", "--subscription", $SubscriptionId) | Out-Null
$account = Invoke-AzJson @("account", "show", "--query", "{tenantId:tenantId,id:id,user:user.name}")
if ($account.id -ne $SubscriptionId) {
    throw "Failed to select subscription $SubscriptionId."
}

Write-Host "FSP demo startup"
Write-Host "Subscription: $SubscriptionId"
Write-Host "Signed in as: $($account.user)"

$sqlMiState = Get-SqlMiState
$opdgState = Get-OpdgState
$aksState = Get-AksState
$sqlDatabaseState = Get-SqlDatabaseState
Write-Host "SQL MI: $($sqlMiState | ConvertTo-Json -Compress)"
Write-Host "OPDG VM: $($opdgState | ConvertTo-Json -Compress)"
Write-Host "AKS: $($aksState | ConvertTo-Json -Compress)"
Write-Host "Azure SQL database: $($sqlDatabaseState | ConvertTo-Json -Compress)"
Test-IngressRoleAssignments

if ($StatusOnly) {
    Write-Host "Status-only check complete; no resources were changed."
    return
}

# Tier 1: independent source/network dependencies can start concurrently.
if ($sqlMiState.state -ne "Ready") {
    Write-Host "Starting SQL Managed Instance $SqlManagedInstanceName..."
    Invoke-AzText @(
        "sql", "mi", "start",
        "--resource-group", $SqlManagedInstanceResourceGroup,
        "--mi", $SqlManagedInstanceName,
        "--no-wait"
    ) | Out-Null
}
else {
    Write-Host "SQL Managed Instance is already ready."
}

if ($opdgState.powerState -ne "PowerState/running") {
    Write-Host "Starting OPDG VM $OpdgVmName..."
    Invoke-AzText @(
        "vm", "start",
        "--resource-group", $OpdgResourceGroup,
        "--name", $OpdgVmName,
        "--no-wait"
    ) | Out-Null
}
else {
    Write-Host "OPDG VM is already running."
}

Wait-ForResource -Name "SQL Managed Instance" -TimeoutMinutes $DependencyTimeoutMinutes `
    -GetState { Get-SqlMiState } `
    -IsReady { param($state) $state.state -eq "Ready" -and $state.provisioningState -eq "Succeeded" } | Out-Null

Wait-ForResource -Name "OPDG VM" -TimeoutMinutes $DependencyTimeoutMinutes `
    -GetState { Get-OpdgState } `
    -IsReady { param($state) $state.powerState -eq "PowerState/running" } | Out-Null

Wait-ForResource -Name "Azure SQL database" -TimeoutMinutes 10 `
    -GetState { Get-SqlDatabaseState } `
    -IsReady { param($state) $state.status -eq "Online" } | Out-Null

# Tier 2: start compute only after its external data dependencies are available.
$aksState = Get-AksState
if ($aksState.powerState -ne "Running") {
    Write-Host "Starting AKS cluster $AksName..."
    Invoke-AzText @(
        "aks", "start",
        "--resource-group", $DemoResourceGroup,
        "--name", $AksName,
        "--no-wait"
    ) | Out-Null
}
else {
    Write-Host "AKS is already running."
}

Wait-ForResource -Name "AKS" -TimeoutMinutes $AksTimeoutMinutes `
    -GetState { Get-AksState } `
    -IsReady { param($state) $state.powerState -eq "Running" -and $state.provisioningState -eq "Succeeded" } | Out-Null

if ($SkipWorkloadValidation) {
    Write-Host "Azure resources are running; Kubernetes workload validation was skipped."
    return
}

# Tier 3: validate the private cluster without persisting cluster credentials.
$temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("fsp-demo-start-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
try {
    $kubeconfig = Join-Path $temporaryDirectory "admin-kubeconfig"
    Invoke-AzText @(
        "aks", "get-credentials",
        "--resource-group", $DemoResourceGroup,
        "--name", $AksName,
        "--admin",
        "--file", $kubeconfig,
        "--overwrite-existing"
    ) | Out-Null

    $waitSeconds = $WorkloadTimeoutMinutes * 60
    Write-Host "Waiting for all demo pods to become ready..."
    Invoke-AksCommand -KubeconfigDirectory $temporaryDirectory -Command `
        "kubectl --kubeconfig admin-kubeconfig -n $Namespace wait --for=condition=Ready pod --all --timeout=${waitSeconds}s" | Write-Host

    Write-Host "Checking Manager registration and source readiness..."
    Invoke-AksCommand -KubeconfigDirectory $temporaryDirectory -Command `
        "kubectl --kubeconfig admin-kubeconfig -n $Namespace exec deploy/fsp-nginx -- wget -qO- http://fsp-manager:9200/healthz" | Write-Host
    Invoke-AksCommand -KubeconfigDirectory $temporaryDirectory -Command `
        "kubectl --kubeconfig admin-kubeconfig -n $Namespace exec deploy/fsp-nginx -- wget --no-check-certificate -qO- https://fsp-nginx-private/readyz" | Write-Host

    Write-Host "Current workload and endpoint state:"
    Invoke-AksCommand -KubeconfigDirectory $temporaryDirectory -Command `
        "kubectl --kubeconfig admin-kubeconfig -n $Namespace get pods -o wide" | Write-Host
    Invoke-AksCommand -KubeconfigDirectory $temporaryDirectory -Command `
        "kubectl --kubeconfig admin-kubeconfig -n $Namespace get svc fsp-nginx-public fsp-nginx-private -o wide" | Write-Host
}
finally {
    Remove-Item -Recurse -Force $temporaryDirectory -ErrorAction SilentlyContinue
}

Write-Host "FSP demo startup and readiness validation completed."