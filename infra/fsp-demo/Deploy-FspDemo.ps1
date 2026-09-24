<#
.SYNOPSIS
    Installs or upgrades the FSP enterprise demo as Helm releases on private AKS.

.DESCRIPTION
    Validates and packages the FSP chart locally, validates Azure, DNS, and AKS
    network-role prerequisites, then invokes Helm inside the AKS Run Command pod.
    Local values files are attached to the transient command and are not stored
    in the repository or Azure deployment history.

.PARAMETER ConfigurationFile
    Path to the ignored deployment.local.json file.

.PARAMETER WhatIf
    Runs local chart validation and Azure preflight checks without changing AKS.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$ConfigurationFile = (Join-Path $PSScriptRoot "deployment.local.json"),
    [string]$HelmVersion = "v3.18.6",
    [string]$CertManagerVersion = "v1.18.2",
    [string]$IngressNginxVersion = "4.13.2"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$chartPath = Join-Path $repositoryRoot "deploy\helm\fabric-shortcut-proxy"

function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$FilePath,
        [Parameter(Mandatory)][string[]]$Arguments
    )

    $output = & $FilePath @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed: $($Arguments -join ' ')`n$($output | Out-String)"
    }
    return ($output | Out-String).Trim()
}

function Invoke-AzJson {
    param([Parameter(Mandatory)][string[]]$Arguments)

    $text = Invoke-Native "az" ($Arguments + @("--only-show-errors", "--output", "json"))
    if (-not $text) {
        return $null
    }
    return $text | ConvertFrom-Json
}

function Get-RequiredConfigurationValue {
    param(
        [Parameter(Mandatory)][pscustomobject]$Configuration,
        [Parameter(Mandatory)][string]$Name
    )

    if ($Name -notin $Configuration.PSObject.Properties.Name) {
        throw "Configuration value '$Name' is required."
    }
    $value = [string]$Configuration.$Name
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Configuration value '$Name' cannot be empty."
    }
    return $value
}

function Resolve-RepositoryPath {
    param([Parameter(Mandatory)][string]$Path)

    $candidate = if ([IO.Path]::IsPathRooted($Path)) {
        $Path
    }
    else {
        Join-Path $repositoryRoot $Path
    }
    return (Resolve-Path $candidate).Path
}

function Assert-KubernetesName {
    param(
        [Parameter(Mandatory)][string]$Value,
        [Parameter(Mandatory)][string]$ConfigurationName
    )

    if ($Value.Length -gt 63 -or $Value -notmatch '^[a-z0-9]([-a-z0-9]*[a-z0-9])?$') {
        throw "Configuration value '$ConfigurationName' must be a Kubernetes DNS label."
    }
}

function Install-TemporaryHelm {
    param([Parameter(Mandatory)][string]$DestinationDirectory)

    if ($env:OS -ne "Windows_NT") {
        throw "Automatic Helm bootstrap supports Windows only. Install $HelmVersion and rerun."
    }

    $archiveName = "helm-$HelmVersion-windows-amd64.zip"
    $archivePath = Join-Path $DestinationDirectory $archiveName
    $downloadUri = "https://get.helm.sh/$archiveName"
    $checksumPath = "$archivePath.sha256sum"
    $previousProgressPreference = $ProgressPreference
    try {
        $ProgressPreference = "SilentlyContinue"
        Invoke-WebRequest -Uri $downloadUri -OutFile $archivePath
        Invoke-WebRequest -Uri "$downloadUri.sha256sum" -OutFile $checksumPath
    }
    finally {
        $ProgressPreference = $previousProgressPreference
    }

    $expectedHash = ((Get-Content $checksumPath -Raw).Trim() -split "\s+")[0]
    $actualHash = (Get-FileHash -Path $archivePath -Algorithm SHA256).Hash
    if ($actualHash -ne $expectedHash) {
        throw "Helm archive checksum verification failed."
    }

    Expand-Archive -Path $archivePath -DestinationPath $DestinationDirectory -Force
    return Join-Path $DestinationDirectory "windows-amd64\helm.exe"
}

function Get-PinnedHelm {
    param([Parameter(Mandatory)][string]$TemporaryDirectory)

    $command = Get-Command helm -ErrorAction SilentlyContinue
    if ($command) {
        $installedVersion = (& $command.Source version --short 2>$null | Out-String).Trim()
        if ($installedVersion.StartsWith($HelmVersion, [StringComparison]::OrdinalIgnoreCase)) {
            return $command.Source
        }
    }
    Write-Host "Downloading pinned Helm $HelmVersion for this deployment..."
    return Install-TemporaryHelm -DestinationDirectory $TemporaryDirectory
}

function Test-NetworkRole {
    param(
        [Parameter(Mandatory)][string]$PrincipalId,
        [Parameter(Mandatory)][string]$Scope
    )

    $roles = @(Invoke-AzJson @(
        "role", "assignment", "list",
        "--assignee-object-id", $PrincipalId,
        "--scope", $Scope,
        "--include-inherited",
        "--query", "[].roleDefinitionName"
    ))
    return [bool]($roles | Where-Object { $_ -in @("Network Contributor", "Contributor", "Owner") })
}

function Invoke-AksRunCommand {
    param(
        [Parameter(Mandatory)][string]$Command,
        [string[]]$Files = @()
    )

    $arguments = @(
        "aks", "command", "invoke",
        "--resource-group", $resourceGroup,
        "--name", $aksName,
        "--command", $Command
    )
    foreach ($file in $Files) {
        $arguments += @("--file", $file)
    }
    $result = Invoke-AzJson $arguments
    if ($result.exitCode -ne 0) {
        throw "AKS Run Command failed:`n$($result.logs)"
    }
    if ($result.logs) {
        Write-Host $result.logs
    }
}

if (-not (Test-Path $ConfigurationFile -PathType Leaf)) {
    throw "Configuration file not found: $ConfigurationFile. Copy deployment.example.json to deployment.local.json."
}
if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required."
}

$configuration = Get-Content $ConfigurationFile -Raw | ConvertFrom-Json
$subscriptionId = Get-RequiredConfigurationValue $configuration "subscriptionId"
$tenantId = Get-RequiredConfigurationValue $configuration "tenantId"
$resourceGroup = Get-RequiredConfigurationValue $configuration "resourceGroup"
$aksName = Get-RequiredConfigurationValue $configuration "aksName"
$hostname = Get-RequiredConfigurationValue $configuration "hostname"
$publicIpName = Get-RequiredConfigurationValue $configuration "publicIpName"
$vnetName = Get-RequiredConfigurationValue $configuration "vnetName"
$appSubnetName = Get-RequiredConfigurationValue $configuration "appSubnetName"
$namespace = Get-RequiredConfigurationValue $configuration "namespace"
$releaseName = Get-RequiredConfigurationValue $configuration "helmReleaseName"
$sourceSecretName = Get-RequiredConfigurationValue $configuration "sourceSecretName"
$valuesPath = Resolve-RepositoryPath (Get-RequiredConfigurationValue $configuration "helmValuesFile")
$ingressValuesPath = Resolve-RepositoryPath (Get-RequiredConfigurationValue $configuration "ingressValuesFile")

Assert-KubernetesName $namespace "namespace"
Assert-KubernetesName $releaseName "helmReleaseName"
Assert-KubernetesName $sourceSecretName "sourceSecretName"
if ([Uri]::CheckHostName($hostname) -ne [UriHostNameType]::Dns) {
    throw "Configuration value 'hostname' must be a DNS hostname."
}

$account = Invoke-AzJson @("account", "show", "--query", "{tenantId:tenantId,id:id,user:user.name}")
if ($account.tenantId -ne $tenantId) {
    throw "Azure CLI is signed into tenant $($account.tenantId); expected $tenantId."
}
Invoke-Native "az" @("account", "set", "--subscription", $subscriptionId) | Out-Null

$cluster = Invoke-AzJson @("aks", "show", "--resource-group", $resourceGroup, "--name", $aksName)
if ($cluster.powerState.code -ne "Running" -or $cluster.provisioningState -ne "Succeeded") {
    throw "AKS must be Running/Succeeded. Run Start-FspDemo.ps1 first."
}

$publicIp = Invoke-AzJson @("network", "public-ip", "show", "--resource-group", $resourceGroup, "--name", $publicIpName)
$resolvedAddresses = @([System.Net.Dns]::GetHostAddresses($hostname) | ForEach-Object IPAddressToString)
if ($publicIp.ipAddress -notin $resolvedAddresses) {
    throw "$hostname does not resolve to reserved public IP $($publicIp.ipAddress)."
}

$subnet = Invoke-AzJson @(
    "network", "vnet", "subnet", "show",
    "--resource-group", $resourceGroup,
    "--vnet-name", $vnetName,
    "--name", $appSubnetName,
    "--query", "{id:id,networkSecurityGroupId:networkSecurityGroup.id}"
)
$missingScopes = @()
foreach ($scope in @($publicIp.id, $subnet.id, $subnet.networkSecurityGroupId) | Where-Object { $_ }) {
    if (-not (Test-NetworkRole -PrincipalId $cluster.identity.principalId -Scope $scope)) {
        $missingScopes += $scope
    }
}
if ($missingScopes.Count -gt 0) {
    throw "AKS identity $($cluster.identity.principalId) needs Network Contributor on:`n$($missingScopes -join "`n")"
}

$temporaryDirectory = Join-Path ([IO.Path]::GetTempPath()) ("fsp-helm-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $temporaryDirectory | Out-Null
try {
    $helmPath = Get-PinnedHelm -TemporaryDirectory $temporaryDirectory
    Invoke-Native $helmPath @("lint", $chartPath, "-f", $valuesPath) | Write-Host
    Invoke-Native $helmPath @("template", $releaseName, $chartPath, "-f", $valuesPath, "--namespace", $namespace) | Out-Null
    Invoke-Native $helmPath @("package", $chartPath, "--destination", $temporaryDirectory) | Write-Host

    $chartArchive = Get-ChildItem $temporaryDirectory -Filter "fabric-shortcut-proxy-*.tgz"
    if (@($chartArchive).Count -ne 1) {
        throw "Expected exactly one packaged chart archive in $temporaryDirectory, found $(@($chartArchive).Count)."
    }
    $attachedValues = Join-Path $temporaryDirectory "fsp-values.yaml"
    $attachedIngressValues = Join-Path $temporaryDirectory "ingress-values.yaml"
    Copy-Item $valuesPath $attachedValues
    Copy-Item $ingressValuesPath $attachedIngressValues

    if (-not $PSCmdlet.ShouldProcess($aksName, "Install or upgrade cert-manager, ingress-nginx, and FSP Helm releases")) {
        Write-Host "Preflight and local Helm validation completed; no cluster changes were made."
        return
    }

    Push-Location $temporaryDirectory
    try {
        $certManagerChartVersion = $CertManagerVersion.TrimStart("v")
        Invoke-AksRunCommand -Command "helm version --short && helm upgrade --help | grep -q -- --take-ownership"
        Invoke-AksRunCommand -Command "helm repo add jetstack https://charts.jetstack.io --force-update && helm repo update && helm upgrade --install cert-manager jetstack/cert-manager --namespace cert-manager --create-namespace --version $certManagerChartVersion --set crds.enabled=true --atomic --timeout 10m --take-ownership"
        Invoke-AksRunCommand -Command "helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx --force-update && helm repo update && helm upgrade --install ingress-nginx ingress-nginx/ingress-nginx --namespace ingress-nginx --create-namespace --version $IngressNginxVersion -f ingress-values.yaml --atomic --timeout 10m --take-ownership" -Files @("ingress-values.yaml")
        $redirectUri = "https://$hostname/_config/"
        Invoke-AksRunCommand -Command "kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f - && kubectl -n $namespace get secret $sourceSecretName >/dev/null && helm upgrade --install $releaseName $($chartArchive.Name) --namespace $namespace -f fsp-values.yaml --set-string sourceSecret.name=$sourceSecretName --set-string tls.hostname=$hostname --set-string manager.entra.redirectUri=$redirectUri --set-string manager.entra.postLogoutRedirectUri=$redirectUri --atomic --timeout 15m --take-ownership" -Files @($chartArchive.Name, "fsp-values.yaml")

        $reconcileTimestamp = [DateTime]::UtcNow.ToString("yyyyMMddHHmmss")
        Invoke-AksRunCommand -Command "kubectl annotate service -n ingress-nginx ingress-nginx-controller fsp.microsoft.com/reconcile-at=$reconcileTimestamp --overwrite && kubectl annotate service -n $namespace fsp-nginx-private fsp.microsoft.com/reconcile-at=$reconcileTimestamp --overwrite"
        Invoke-AksRunCommand -Command "kubectl -n $namespace wait --for=condition=Ready certificate/fsp-nginx-tls --timeout=600s && helm status $releaseName -n $namespace"
    }
    finally {
        Pop-Location
    }
}
finally {
    Remove-Item -Recurse -Force $temporaryDirectory -ErrorAction SilentlyContinue
}

Write-Host "FSP Helm release is ready at https://$hostname/."
