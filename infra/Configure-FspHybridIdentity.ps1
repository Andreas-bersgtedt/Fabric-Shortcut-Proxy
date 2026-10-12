[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $AgentApiAppId,

    [string] $SubscriptionId = "68e360fa-2a87-4ea1-96e3-f4101bea3709",

    [string] $ResourceGroup = "rg-fsp-linux-test-sc",

    [string] $IdentityName = "id-fsp-hybrid-site",

    [string] $AgentId = "hybrid-site-01"
)

$ErrorActionPreference = "Stop"

function Invoke-AzJson {
    param([string[]] $Arguments)

    $output = & az @Arguments --only-show-errors -o json
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command failed: az $($Arguments -join ' ')"
    }
    return $output | ConvertFrom-Json
}

$identity = Invoke-AzJson @(
    "identity", "show",
    "--subscription", $SubscriptionId,
    "--resource-group", $ResourceGroup,
    "--name", $IdentityName
)
$api = Invoke-AzJson @(
    "ad", "sp", "show",
    "--id", $AgentApiAppId
)
$role = @($api.appRoles | Where-Object {
    $_.value -eq "FSP.Agent" -and $_.isEnabled -and "Application" -in $_.allowedMemberTypes
})
if ($role.Count -ne 1) {
    throw "The API service principal must expose exactly one enabled FSP.Agent application role."
}

$existing = Invoke-AzJson @(
    "rest", "--method", "GET",
    "--url", "https://graph.microsoft.com/v1.0/servicePrincipals/$($identity.principalId)/appRoleAssignments"
)
$assignment = @($existing.value | Where-Object {
    $_.resourceId -eq $api.id -and $_.appRoleId -eq $role[0].id
})
if (-not $assignment) {
    $body = @{
        principalId = $identity.principalId
        resourceId = $api.id
        appRoleId = $role[0].id
    } | ConvertTo-Json -Compress
    & az rest --method POST `
        --url "https://graph.microsoft.com/v1.0/servicePrincipals/$($identity.principalId)/appRoleAssignments" `
        --headers "Content-Type=application/json" `
        --body $body `
        --only-show-errors `
        --output none
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to assign the FSP.Agent application role."
    }
}

@{
    agent_id = $AgentId
    client_id = $identity.clientId
    principal_id = $identity.principalId
} | ConvertTo-Json
