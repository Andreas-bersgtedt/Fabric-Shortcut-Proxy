<#
.SYNOPSIS
Securely injects an Azure service-principal client secret into the AKS FSP Manager.

.DESCRIPTION
Prompts for the secret without echoing it, stores it only in the Kubernetes Secret,
injects that Secret into the Manager Deployment, and waits for the Manager rollout.
The client secret is never written to a config JSON file or printed to the console.

.EXAMPLE
\.\Set-FspAksAzureClientSecret.ps1 -ResourceGroup <resource-group> -ClusterName <aks-cluster>

.EXAMPLE
\.\Set-FspAksAzureClientSecret.ps1 -ResourceGroup my-resource-group -ClusterName my-aks-cluster
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$ResourceGroup,
    [Parameter(Mandatory)]
    [string]$ClusterName,
    [string]$Namespace = "fabric-shortcut-proxy",
    [string]$SecretName = "fsp-azure-identity",
    [string]$DeploymentName = "fsp-manager",
    [switch]$SkipRestart
)

$ErrorActionPreference = "Stop"

function Invoke-Kubectl {
    param([Parameter(Mandatory)][string[]]$Arguments)

    & kubectl @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl $($Arguments -join ' ') failed with exit code $LASTEXITCODE."
    }
}

if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
    throw "Azure CLI (az) is required."
}
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl is required. Run this from the AKS jump host or another private-cluster admin host."
}

Write-Host "Retrieving AKS credentials for $ClusterName..."
& az aks get-credentials --resource-group $ResourceGroup --name $ClusterName --admin --overwrite-existing --output none
if ($LASTEXITCODE -ne 0) {
    throw "Unable to retrieve AKS credentials."
}

& kubectl cluster-info | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "kubectl cannot reach the private AKS API from this host. Run set-fsp-aks-azure-client-secret.sh on the AKS jump host instead."
}

$secureSecret = Read-Host "Azure client secret" -AsSecureString
$bstr = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureSecret)
try {
    $clientSecret = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($bstr)
    if ([string]::IsNullOrWhiteSpace($clientSecret)) {
        throw "Azure client secret cannot be empty."
    }

    $secretYaml = @{
        apiVersion = "v1"
        kind = "Secret"
        metadata = @{
            name = $SecretName
            namespace = $Namespace
        }
        type = "Opaque"
        stringData = @{
            AZURE_CLIENT_SECRET = $clientSecret
        }
    } | ConvertTo-Json -Depth 5 -Compress

    $secretYaml | & kubectl apply -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to create or update Kubernetes Secret $SecretName."
    }
}
finally {
    if ($bstr -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($bstr)
    }
    $clientSecret = $null
}

Invoke-Kubectl -Arguments @(
    "-n", $Namespace,
    "set", "env", "deployment/$DeploymentName",
    "--from=secret/$SecretName"
)

if (-not $SkipRestart) {
    Invoke-Kubectl -Arguments @(
        "-n", $Namespace,
        "rollout", "status", "deployment/$DeploymentName",
        "--timeout=180s"
    )
}

Write-Host "Azure client secret injected into $Namespace/$DeploymentName." -ForegroundColor Green