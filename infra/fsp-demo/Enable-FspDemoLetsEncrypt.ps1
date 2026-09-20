<#
.SYNOPSIS
    Compatibility entry point for the FSP enterprise demo Helm deployment.

.DESCRIPTION
    The enterprise demo is now managed as Helm releases. This wrapper preserves
    the previous command name and delegates to Deploy-FspDemo.ps1.
#>
[CmdletBinding()]
param(
    [string]$ConfigurationFile = (Join-Path $PSScriptRoot "deployment.local.json"),
    [string]$HelmVersion = "v3.18.6",
    [string]$CertManagerVersion = "v1.18.2",
    [string]$IngressNginxVersion = "4.13.2"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Write-Warning "Enable-FspDemoLetsEncrypt.ps1 now deploys the complete FSP Helm release. Use Deploy-FspDemo.ps1 for new automation."
& (Join-Path $PSScriptRoot "Deploy-FspDemo.ps1") `
    -ConfigurationFile $ConfigurationFile `
    -HelmVersion $HelmVersion `
    -CertManagerVersion $CertManagerVersion `
    -IngressNginxVersion $IngressNginxVersion
