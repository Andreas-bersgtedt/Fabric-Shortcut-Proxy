[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string] $ManagerUrl,

    [Parameter(Mandatory = $true)]
    [string] $AgentApiAppId,

    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[a-z0-9]{3,24}$')]
    [string] $StorageAccountName,

    [string] $SubscriptionId = "68e360fa-2a87-4ea1-96e3-f4101bea3709",

    [string] $ResourceGroup = "rg-fsp-linux-test-sc",

    [string] $VmName = "fsp-linux-test",

    [string] $VmVnetName = "fsp-linux-test-vnet",

    [string] $IdentityName = "id-fsp-hybrid-site",

    [string] $GitRef = "main",

    [string] $AgentId = "hybrid-site-01",

    [string] $AgentLocation = "azure-iaas:simulated-onprem",

    [string] $BlobPrivateDnsZoneResourceGroupName = "fabricproxy",

    [string] $BlobPrivateDnsZoneName = "privatelink.blob.core.windows.net",

    [string] $PrimaryVnetResourceGroupName = "FSP_Demo_xdev_swedencentral",

    [string] $PrimaryVnetName = "vnet-fspdemo-xdev-swc",

    [string] $SecondaryVnetResourceGroupName = "FSP_Demo_xdev_northeurope",

    [string] $SecondaryVnetName = "vnet-fspdemo-xdev-neu",

    [string] $SourceArchivePath,

    [switch] $PackageCurrentCheckout
)

$ErrorActionPreference = "Stop"
$repositoryUrl = "https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git"
$scriptPath = Join-Path $PSScriptRoot "install.sh"
$temporaryArchivePath = $null

function Invoke-AzJson {
    param([Parameter(Mandatory = $true)][string[]] $Arguments)

    $output = & az @Arguments --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI query failed: az $($Arguments -join ' ')"
    }
    if (-not $output) {
        return $null
    }
    try {
        return ($output | ConvertFrom-Json)
    }
    catch {
        throw "Azure CLI returned invalid JSON for: az $($Arguments -join ' ')"
    }
}

function Assert-ConnectedPeering {
    param(
        [string] $LocalResourceGroup,
        [string] $LocalVnetName,
        [string] $RemoteVnetId
    )

    $peerings = Invoke-AzJson @(
        "network", "vnet", "peering", "list",
        "--resource-group", $LocalResourceGroup,
        "--vnet-name", $LocalVnetName
    )
    $matches = @($peerings | Where-Object {
        $_.remoteVirtualNetwork.id -ieq $RemoteVnetId -and
        $_.peeringState -eq "Connected"
    })
    if ($matches.Count -ne 1) {
        throw "Required connected peering is missing: $LocalResourceGroup/$LocalVnetName -> $RemoteVnetId. Deploy the additive Phase 4 network template first."
    }
}

function Assert-PackageArchive {
    param([Parameter(Mandatory = $true)][string] $Path)

    $entries = @(& tar.exe -tzf $Path)
    if ($LASTEXITCODE -ne 0) {
        throw "Source archive is not a readable .tar.gz file: $Path"
    }
    $normalizedEntries = @($entries | ForEach-Object {
        $_.TrimStart([char[]]@('.', '/')).Replace('\', '/')
    })
    if ($normalizedEntries -notcontains "main.py" -or
        $normalizedEntries -notcontains "pyproject.toml" -or
        $normalizedEntries -notcontains "enterprise/pyproject.toml" -or
        -not @($normalizedEntries | Where-Object {
            $_.StartsWith("src/fabric_shortcut_proxy/", [StringComparison]::Ordinal)
        }).Count) {
        throw "The source archive must contain the root launcher, both project manifests, and src/fabric_shortcut_proxy/."
    }
}

try {
    if (-not (Get-Command az -ErrorAction SilentlyContinue)) {
        throw "Azure CLI (az) is required."
    }
    if (-not (Test-Path $scriptPath -PathType Leaf)) {
        throw "Missing bootstrap script: $scriptPath"
    }
    $managerUri = $null
    if (-not [Uri]::TryCreate($ManagerUrl, [UriKind]::Absolute, [ref]$managerUri) -or
        $managerUri.Scheme -ne "https" -or
        $managerUri.UserInfo -or
        $managerUri.Query -or
        $managerUri.Fragment -or
        $ManagerUrl -match '[\s`"$\\#]') {
        throw "ManagerUrl must be an HTTPS URL without credentials, query, fragment, whitespace, or shell metacharacters."
    }
    if ($AgentApiAppId -notmatch '^[A-Fa-f0-9-]{36}$') {
        throw "AgentApiAppId must be a GUID."
    }
    if ($AgentId -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$') {
        throw "AgentId contains unsupported characters."
    }
    if ($AgentLocation -notmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') {
        throw "AgentLocation contains unsupported characters."
    }
    if ($GitRef -notmatch '^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$' -or
        $GitRef.Contains("..") -or $GitRef.Contains("//")) {
        throw "GitRef must be a valid published branch or tag name."
    }
    if ($PackageCurrentCheckout -and $SourceArchivePath) {
        throw "Choose either PackageCurrentCheckout or SourceArchivePath, not both."
    }

    if ($PackageCurrentCheckout) {
        if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
            throw "tar.exe is required to package the current checkout."
        }
        $repositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
        $temporaryArchivePath = Join-Path ([IO.Path]::GetTempPath()) (
            "fsp-hybrid-site-{0}.tar.gz" -f [Guid]::NewGuid().ToString("N")
        )
        & tar.exe -czf $temporaryArchivePath `
            --exclude=__pycache__ --exclude=*.pyc `
            -C $repositoryRoot main.py pyproject.toml README.md src enterprise
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to package the current checkout."
        }
        $SourceArchivePath = $temporaryArchivePath
    }

    $packageBlobName = "-"
    $packageSha256 = "-"
    if ($SourceArchivePath) {
        $archive = (Resolve-Path $SourceArchivePath -ErrorAction Stop).Path
        if ([IO.Path]::GetExtension($archive) -notin @(".gz", ".tgz")) {
            throw "SourceArchivePath must name a gzip-compressed tar archive."
        }
        Assert-PackageArchive -Path $archive
        $packageSha256 = (Get-FileHash -Path $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        $packageBlobName = "packages/fsp-hybrid-site-$packageSha256.tar.gz"
    }
    else {
        if (-not (Get-Command git.exe -ErrorAction SilentlyContinue)) {
            throw "git.exe is required to verify GitRef when no source package is supplied."
        }
        $publishedRefs = & git.exe ls-remote --exit-code --refs $repositoryUrl `
            "refs/heads/$GitRef" "refs/tags/$GitRef"
        if ($LASTEXITCODE -ne 0 -or -not $publishedRefs) {
            throw "GitRef '$GitRef' is not published on the repository. Publish it or use -PackageCurrentCheckout / -SourceArchivePath."
        }
    }

    $subscription = Invoke-AzJson @("account", "show", "--subscription", $SubscriptionId)
    if ($subscription.id -ine $SubscriptionId) {
        throw "Azure CLI is not authenticated to the requested subscription."
    }

    $identity = Invoke-AzJson @(
        "identity", "show",
        "--subscription", $SubscriptionId,
        "--resource-group", $ResourceGroup,
        "--name", $IdentityName
    )
    if (-not $identity.clientId -or -not $identity.principalId -or -not $identity.id) {
        throw "Managed identity $IdentityName is missing a client ID, principal ID, or resource ID."
    }

    $vm = Invoke-AzJson @(
        "vm", "show",
        "--subscription", $SubscriptionId,
        "--resource-group", $ResourceGroup,
        "--name", $VmName
    )
    $assignedIdentityIds = @($vm.identity.userAssignedIdentities.PSObject.Properties.Name)
    if (-not ($assignedIdentityIds | Where-Object { $_ -ieq $identity.id })) {
        throw "Managed identity $IdentityName is not attached to VM $VmName. Attach it separately; the installer will not change VM identity."
    }
    if (-not $vm.networkProfile.networkInterfaces) {
        throw "VM $VmName has no network interfaces."
    }

    $vmVnet = Invoke-AzJson @(
        "network", "vnet", "show",
        "--subscription", $SubscriptionId,
        "--resource-group", $ResourceGroup,
        "--name", $VmVnetName
    )
    $primaryVnet = Invoke-AzJson @(
        "network", "vnet", "show",
        "--subscription", $SubscriptionId,
        "--resource-group", $PrimaryVnetResourceGroupName,
        "--name", $PrimaryVnetName
    )
    $secondaryVnet = Invoke-AzJson @(
        "network", "vnet", "show",
        "--subscription", $SubscriptionId,
        "--resource-group", $SecondaryVnetResourceGroupName,
        "--name", $SecondaryVnetName
    )

    $subnetIds = @()
    $publicIpIds = @()
    foreach ($networkInterfaceReference in $vm.networkProfile.networkInterfaces) {
        $networkInterface = Invoke-AzJson @(
            "network", "nic", "show",
            "--subscription", $SubscriptionId,
            "--ids", $networkInterfaceReference.id
        )
        foreach ($configuration in $networkInterface.ipConfigurations) {
            if ($configuration.subnet.id) {
                $subnetIds += $configuration.subnet.id
            }
            if ($configuration.publicIpAddress.id) {
                $publicIpIds += $configuration.publicIpAddress.id
            }
        }
    }
    if (-not @($subnetIds | Where-Object {
        $_.StartsWith("$($vmVnet.id)/subnets/", [StringComparison]::OrdinalIgnoreCase)
    }).Count) {
        throw "VM $VmName is not attached to the configured VNet $($vmVnet.id)."
    }
    if ($publicIpIds.Count -gt 0) {
        throw "VM $VmName now has a public IP. This installer expects the existing private-only VM."
    }

    Assert-ConnectedPeering -LocalResourceGroup $ResourceGroup `
        -LocalVnetName $VmVnetName -RemoteVnetId $primaryVnet.id
    Assert-ConnectedPeering -LocalResourceGroup $PrimaryVnetResourceGroupName `
        -LocalVnetName $PrimaryVnetName -RemoteVnetId $vmVnet.id
    Assert-ConnectedPeering -LocalResourceGroup $ResourceGroup `
        -LocalVnetName $VmVnetName -RemoteVnetId $secondaryVnet.id
    Assert-ConnectedPeering -LocalResourceGroup $SecondaryVnetResourceGroupName `
        -LocalVnetName $SecondaryVnetName -RemoteVnetId $vmVnet.id

    $blobDnsLinks = Invoke-AzJson @(
        "network", "private-dns", "link", "vnet", "list",
        "--subscription", $SubscriptionId,
        "--resource-group", $BlobPrivateDnsZoneResourceGroupName,
        "--zone-name", $BlobPrivateDnsZoneName
    )
    if (-not @($blobDnsLinks | Where-Object {
        $_.virtualNetwork.id -ieq $vmVnet.id
    }).Count) {
        throw "The existing Blob private DNS zone is not linked to $($vmVnet.id). Deploy the additive Phase 4 network template first."
    }

    if (-not $PSCmdlet.ShouldProcess(
        "$ResourceGroup/$VmName",
        "Upload the source package if selected, start the VM if needed, and install the no-secret FSP hybrid Agent"
    )) {
        return
    }

    if ($SourceArchivePath) {
        & az storage blob upload `
            --subscription $SubscriptionId `
            --auth-mode login `
            --account-name $StorageAccountName `
            --container-name "fsp-hybrid-staging" `
            --name $packageBlobName `
            --file (Resolve-Path $SourceArchivePath).Path `
            --overwrite true `
            --only-show-errors `
            --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to upload the SHA-256-addressed source package to the private staging container. The VM was not started."
        }
    }

    $instanceView = Invoke-AzJson @(
        "vm", "get-instance-view",
        "--subscription", $SubscriptionId,
        "--resource-group", $ResourceGroup,
        "--name", $VmName
    )
    $powerState = @($instanceView.statuses | Where-Object {
        $_.code -like "PowerState/*"
    } | Select-Object -First 1).code
    if ($powerState -ne "PowerState/running") {
        & az vm start `
            --subscription $SubscriptionId `
            --resource-group $ResourceGroup `
            --name $VmName `
            --only-show-errors
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to start VM $VmName."
        }
    }

    $runOutput = & az vm run-command invoke `
        --subscription $SubscriptionId `
        --resource-group $ResourceGroup `
        --name $VmName `
        --command-id RunShellScript `
        --scripts "@$scriptPath" `
        --parameters `
            $ManagerUrl `
            $AgentApiAppId `
            $identity.clientId `
            $StorageAccountName `
            $GitRef `
            $AgentId `
            $AgentLocation `
            $packageBlobName `
            $packageSha256 `
        --only-show-errors `
        --output json
    if ($LASTEXITCODE -ne 0) {
        throw "Hybrid Agent installation failed; inspect the Run Command output."
    }

    try {
        $runResult = $runOutput | ConvertFrom-Json
    }
    catch {
        throw "Run Command returned invalid JSON; installation success could not be verified."
    }
    $runMessages = @($runResult.value | ForEach-Object { $_.message }) -join "`n"
    if ($runMessages -notmatch "FSP_HYBRID_INSTALL_READY agent_id=$([Regex]::Escape($AgentId))\b") {
        throw "Run Command completed without the installer readiness marker. Inspect its output; installation is not considered successful."
    }
    Write-Output $runMessages
}
finally {
    if ($temporaryArchivePath -and (Test-Path $temporaryArchivePath -PathType Leaf)) {
        Remove-Item -LiteralPath $temporaryArchivePath -Force
    }
}
