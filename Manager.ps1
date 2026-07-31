<#
.SYNOPSIS
    Bootstrap and launch the Fabric Shortcut Proxy **Manager** (control plane).

.DESCRIPTION
    One-shot bootstrap for the Manager/Agent cluster (docs/SCALE_ARCHITECTURE_PLAN.md):
      1. Verifies a suitable Python interpreter is available.
      2. Creates a local virtual environment (.venv) if it does not exist.
      3. Installs / updates project dependencies from pyproject.toml.
      4. Launches the Manager (python manager.py), which starts the control-plane
         REST server AND spawns + supervises one local Agent (the S3 server).

    The Manager restarts the Agent automatically if it crashes. Point your Fabric
    S3 shortcut at the **Agent** port (-AgentPort), not the control port.

    Re-runnable: reuses the existing venv and skips dependency installation unless
    -Reinstall is passed.

.PARAMETER ControlPort
    Control-plane REST port (Manager). Overrides CONTROL_PORT (default 9200).

.PARAMETER ControlHost
    Control-plane bind address (Manager). Overrides CONTROL_HOST (default 127.0.0.1).

.PARAMETER AgentPort
    S3 data-plane port the supervised Agent serves on (this is what Fabric connects
    to). Overrides PORT (default 9000).

.PARAMETER AgentBindHost
    Interface the Agent binds to. Overrides HOST (default 0.0.0.0).

.PARAMETER DbUrl
    SQLAlchemy async DB URL. Overrides DB_URL
    (default sqlite+aiosqlite:///./poc_source.db).

.PARAMETER TableFormat
    Output format served by the Agent: 'iceberg' (default) or 'delta'. Overrides
    TABLE_FORMAT.

.PARAMETER HeartbeatMs
    Agent heartbeat interval in milliseconds. Overrides HEARTBEAT_MS (default 2000).

.PARAMETER AgentCount
    Number of Agents the Manager supervises (each on AgentPort + i). Overrides
    AGENT_COUNT (default 1). Cold materialization is sharded across them.

.PARAMETER Gateway
    Front the Agent fleet with the Manager's built-in round-robin S3 gateway (on
    the control port). Point the Fabric shortcut at the gateway. Sets ENABLE_GATEWAY=1.

.PARAMETER AdminUi
    Serve the /_manager operator console (fleet monitor + start/stop/restart/drain)
    on the control port. Sets ENABLE_ADMIN_UI=1.

.PARAMETER ConfigUi
    Serve the config builder UI/API at /_config on the control port.
    Sets ENABLE_CONFIG_BUILDER=1.

.PARAMETER AllowConfigDbCreds
    Deprecated / no-op: allowing an inline db_url in the local (gitignored)
    config.connection.json is now the Manager.ps1 default. Use
    -StrictDbCredentials to opt back into the strict startup gate.

.PARAMETER StrictDbCredentials
    Enforce the secure-by-default startup gate that REJECTS an inline DB
    credential in config.connection.json db_url. By default this local launcher
    ALLOWS it (config files are local, gitignored, per-deployment). Pass this to
    require the DB_URL env var (-DbUrl) or passwordless auth instead.

.PARAMETER AdminToken
    Token required for mutating /_manager actions (X-Admin-Token header or ?token=).
    Sets ADMIN_TOKEN. Reads stay open; blank = no auth.

.PARAMETER Ha
    Manager HA: run a leader lease over the shared artifact store. Only the primary
    supervises Agents + serves the gateway; standbys take over on primary loss.
    Sets MANAGER_HA=1. Start a second Manager with -Ha (same ARTIFACT_STORE_DIR,
    different -ControlPort) as a warm standby.

.PARAMETER RetentionGc
    Periodically prune orphaned Parquet splits from the shared store (Agent shard 0).
    Sets RETENTION_GC=1.

.PARAMETER Branch
    Git branch to check out and fast-forward before building (e.g.
    feature/scale-architecture). If this folder is already the repo it fetches +
    checks out + fast-forwards in place. If this folder is NOT a git repo yet, it
    bootstraps the codebase here from -RepoUrl (git init + fetch + checkout) so the
    code lands alongside this script - no sub-folder, no clone elsewhere. Never
    discards uncommitted work in an existing repo.

.PARAMETER Remote
    Git remote to fetch/pull from (default 'origin').

.PARAMETER RepoUrl
    Repository URL used only when bootstrapping an empty/non-git folder
    (default the Fabric-Shortcut-Proxy origin). Ignored when the folder is already a repo.

.PARAMETER NoPull
    Skip the git fetch/checkout/pull step entirely (build from the working tree
    as-is).

.PARAMETER Reinstall
    Force reinstall of dependencies even if the venv already exists.

.PARAMETER Recreate
    Delete and recreate the virtual environment from scratch.

.PARAMETER SkipInstall
    Skip dependency installation entirely (fastest start; assumes venv is ready).

.PARAMETER AutoStash
    When switching branches, automatically stash local tracked and untracked
    changes if needed. The script does NOT auto-pop the stash; it prints how to
    restore it after startup.

.PARAMETER ObjectPathLayout
    Virtual object path layout: 'legacy' (db/<table>) or 'canonical'
    (db/<server>/<database>/<schema>/<object>). Overrides OBJECT_PATH_LAYOUT.

.PARAMETER DisableLegacyAliases
    Disable serving legacy object aliases when canonical layout is enabled.
    Sets ENABLE_LEGACY_PATH_ALIASES=0.

.EXAMPLE
    .\Manager.ps1

.EXAMPLE
    .\Manager.ps1 -Branch feature/scale-architecture -SkipInstall

.EXAMPLE
    .\Manager.ps1 -ControlPort 9200 -AgentPort 9100 -TableFormat delta -SkipInstall

.EXAMPLE
    .\Manager.ps1 -DbUrl "mssql+aioodbc://@host/db?driver=ODBC+Driver+18+for+SQL+Server&trusted_connection=yes"
#>
[CmdletBinding()]
param(
    [int]$ControlPort,
    [string]$ControlHost,
    [int]$AgentPort,
    [string]$AgentBindHost,
    [string]$DbUrl,
    [ValidateSet("iceberg", "delta")]
    [string]$TableFormat,
    [int]$HeartbeatMs,
    [int]$AgentCount,
    [switch]$Gateway,
    [switch]$AdminUi,
    [switch]$ConfigUi,
    [switch]$AllowConfigDbCreds,
    [switch]$StrictDbCredentials,
    [string]$AdminToken,
    [switch]$Ha,
    [switch]$RetentionGc,
    [string]$Branch,
    [string]$Remote = "origin",
    [string]$RepoUrl = "https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git",
    [switch]$NoPull,
    [switch]$Reinstall,
    [switch]$Recreate,
    [switch]$SkipInstall,
    [switch]$AutoStash,
    [ValidateSet("legacy", "canonical")]
    [string]$ObjectPathLayout,
    [switch]$DisableLegacyAliases
)

$ErrorActionPreference = "Stop"

# Always operate from the script's own directory so relative paths resolve.
$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

$VenvDir = Join-Path $ProjectRoot ".venv"
$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$StampFile = Join-Path $VenvDir ".deps-installed"

function Write-Step {
    param([string]$Message)
    Write-Host "==> $Message" -ForegroundColor Cyan
}

# Run git safely: git writes progress to stderr, which under
# $ErrorActionPreference='Stop' becomes a terminating NativeCommandError even with
# stderr output. Force 'Continue' inside this scope and return exit code + output so the
# caller decides what to do.
function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$GitArgs)
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $out = & git @GitArgs 2>&1
        return [pscustomobject]@{ Output = (($out -join "`n").Trim()); ExitCode = $LASTEXITCODE }
    } finally {
        $ErrorActionPreference = $prev
    }
}

function Ensure-CleanOrAutoStash {
    param(
        [switch]$AllowAutoStash,
        [ref]$DidStash,
        [ref]$StashRef
    )

    $dirty = (Invoke-Git status --porcelain)
    $isDirty = ($dirty.ExitCode -eq 0 -and [string]::IsNullOrWhiteSpace($dirty.Output) -eq $false)
    if (-not $isDirty) { return }

    if (-not $AllowAutoStash) {
        throw "Local changes are present. Commit, stash, or rerun with -AutoStash.`nLocal changes:`n$($dirty.Output)"
    }

    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $stashMsg = "manager-autostash-$stamp"
    Write-Step "Local changes detected - auto-stashing"
    $r = Invoke-Git stash push -u -m $stashMsg
    if ($r.ExitCode -ne 0) {
        throw "git stash push failed:`n$($r.Output)"
    }
    $DidStash.Value = $true
    $StashRef.Value = "stash^{/$stashMsg}"
}

# ---------------------------------------------------------------------------
# 0. Sync the codebase (optional) - IN PLACE in this folder (never a sub-folder).
#    * Folder IS this repo's root -> fetch + checkout branch + fast-forward pull.
#    * Folder is NOT a git repo yet -> bootstrap it here from -RepoUrl (git init +
#      fetch + force checkout) so the code lands next to this script.
#    Unrelated untracked files (e.g. .venv) are preserved; tracked files
#    (including this script) are populated/updated from the repo. Use -NoPull to
#    build the folder exactly as-is.
# ---------------------------------------------------------------------------
if ($NoPull) {
    if ($Branch) { Write-Warning "-Branch given with -NoPull; ignoring (no sync performed)." }
} elseif (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Warning "git not found on PATH - skipping codebase sync."
} else {
    $autoStashCreated = $false
    $autoStashRef = ""

    $inside = Invoke-Git rev-parse --is-inside-work-tree
    $isRepo = ($inside.ExitCode -eq 0 -and $inside.Output -eq "true")

    if ($isRepo) {
        # Confirm the repo root IS this folder (never operate on a parent repo).
        $top = Invoke-Git rev-parse --show-toplevel
        $topFull = if ($top.ExitCode -eq 0 -and $top.Output) { (Resolve-Path $top.Output).Path } else { "" }
        if ($topFull -ne $ProjectRoot) {
            Write-Warning "Git root ($topFull) differs from the script folder ($ProjectRoot) - skipping sync to avoid touching a parent repo."
        } else {
            $headBefore = (Invoke-Git rev-parse HEAD).Output

            Write-Step "Fetching from '$Remote' (in place: $ProjectRoot)"
            $r = Invoke-Git fetch $Remote --prune
            if ($r.ExitCode -ne 0) { throw "git fetch '$Remote' failed:`n$($r.Output)" }

            if ($Branch) {
                $current = (Invoke-Git rev-parse --abbrev-ref HEAD).Output
                if ($current -ne $Branch) {
                    $branchExistsLocal = ((Invoke-Git show-ref --verify --quiet "refs/heads/$Branch").ExitCode -eq 0)
                    $branchExistsRemote = ((Invoke-Git show-ref --verify --quiet "refs/remotes/$Remote/$Branch").ExitCode -eq 0)
                    Ensure-CleanOrAutoStash -AllowAutoStash:$AutoStash -DidStash ([ref]$autoStashCreated) -StashRef ([ref]$autoStashRef)

                    Write-Step "Checking out branch '$Branch'"
                    if ($branchExistsLocal) {
                        $r = Invoke-Git checkout $Branch
                    } elseif ($branchExistsRemote) {
                        $r = Invoke-Git checkout -B $Branch --track "$Remote/$Branch"
                    } else {
                        throw "Branch '$Branch' was not found locally or on '$Remote'."
                    }

                    if ($r.ExitCode -ne 0) {
                        throw "git checkout '$Branch' failed:`n$($r.Output)"
                    }
                }
            }

            $curBranch = (Invoke-Git rev-parse --abbrev-ref HEAD).Output
            if ($curBranch -eq "HEAD") {
                Write-Warning "Detached HEAD - skipping pull. Pass -Branch to check out a branch."
            } else {
                # Even when no branch switch is needed, local edits can block
                # ff-only pulls. Apply the same safety policy here.
                Ensure-CleanOrAutoStash -AllowAutoStash:$AutoStash -DidStash ([ref]$autoStashCreated) -StashRef ([ref]$autoStashRef)
                Write-Step "Pulling '$curBranch' (fast-forward only)"
                $r = Invoke-Git pull --ff-only $Remote $curBranch
                if ($r.ExitCode -ne 0) {
                    throw "git pull --ff-only failed (branch diverged or local changes; resolve manually - the script won't force):`n$($r.Output)"
                }
            }

            $headAfter = (Invoke-Git rev-parse HEAD).Output
            if ($headBefore -and $headAfter -and $headBefore -ne $headAfter) {
                Write-Step "Repo updated: $($headBefore.Substring(0,7)) -> $($headAfter.Substring(0,7))"
                if (-not $SkipInstall -and (Test-Path $StampFile)) { Remove-Item -Force $StampFile }
            } else {
                Write-Step "Repo already up to date"
            }

            if ($autoStashCreated) {
                Write-Warning "AutoStash created and kept for safety. Restore later with: git stash pop $autoStashRef"
            }
        }
    } else {
        # Not a git repo yet: bootstrap the codebase INTO this folder (in place).
        $targetBranch = if ($Branch) { $Branch } else { "main" }
        Write-Step "No git repo here - bootstrapping '$RepoUrl' ($targetBranch) into $ProjectRoot"

        if (-not (Test-Path (Join-Path $ProjectRoot ".git"))) {
            $r = Invoke-Git init
            if ($r.ExitCode -ne 0) { throw "git init failed:`n$($r.Output)" }
        }
        $origin = Invoke-Git remote get-url origin
        if ($origin.ExitCode -eq 0) { [void](Invoke-Git remote set-url origin $RepoUrl) }
        else { [void](Invoke-Git remote add origin $RepoUrl) }

        Write-Step "Fetching '$targetBranch' from origin"
        $r = Invoke-Git fetch origin $targetBranch
        if ($r.ExitCode -ne 0) { throw "git fetch origin $targetBranch failed (check -RepoUrl / -Branch):`n$($r.Output)" }

        # Force-populate the working tree; overwrites the bootstrap copy of this
        # script with the repo's tracked version. Unrelated untracked files stay.
        Write-Step "Populating the working tree (force checkout '$targetBranch')"
        $r = Invoke-Git checkout -f -B $targetBranch FETCH_HEAD
        if ($r.ExitCode -ne 0) { throw "git checkout failed:`n$($r.Output)" }
        [void](Invoke-Git branch --set-upstream-to "origin/$targetBranch" $targetBranch)

        Write-Step "Codebase bootstrapped in place (branch '$targetBranch'). Re-run if this script was updated."
        if (-not $SkipInstall -and (Test-Path $StampFile)) { Remove-Item -Force $StampFile }
    }
}

# ---------------------------------------------------------------------------
# 1. Locate a base Python interpreter
# ---------------------------------------------------------------------------
function Resolve-BasePython {
    foreach ($candidate in @("py -3", "python", "python3")) {
        $parts = $candidate.Split(" ")
        $exe = $parts[0]
        if (Get-Command $exe -ErrorAction SilentlyContinue) {
            try {
                $ver = & $exe @($parts[1..($parts.Length - 1)]) --version 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-Step "Using base interpreter: $candidate ($ver)"
                    return $candidate
                }
            } catch { }
        }
    }
    throw "No Python interpreter found. Install Python 3.11+ and ensure 'py' or 'python' is on PATH."
}

# ---------------------------------------------------------------------------
# 2. (Re)create virtual environment
# ---------------------------------------------------------------------------
if ($Recreate -and (Test-Path $VenvDir)) {
    Write-Step "Removing existing virtual environment"
    Remove-Item -Recurse -Force $VenvDir
}

if (-not (Test-Path $VenvPython)) {
    $basePython = Resolve-BasePython
    Write-Step "Creating virtual environment at .venv"
    $parts = $basePython.Split(" ")
    & $parts[0] @($parts[1..($parts.Length - 1)]) -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "Failed to create virtual environment." }
}

if (-not (Test-Path $VenvPython)) {
    throw "Virtual environment python not found at $VenvPython"
}

# ---------------------------------------------------------------------------
# 3. Install / update dependencies
# ---------------------------------------------------------------------------
$needsInstall = $Reinstall -or (-not (Test-Path $StampFile))

if ($SkipInstall) {
    Write-Step "Skipping dependency installation (-SkipInstall)"
} elseif ($needsInstall) {
    Write-Step "Upgrading pip"
    & $VenvPython -m pip install --upgrade pip --quiet
    if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed." }

    Write-Step "Installing project dependencies from pyproject.toml"
    & $VenvPython -m pip install -e . --quiet
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

    New-Item -ItemType File -Path $StampFile -Force | Out-Null
    Write-Step "Dependencies installed"
} else {
    Write-Step "Dependencies already installed (use -Reinstall to refresh)"
}

# ---------------------------------------------------------------------------
# 4. Apply runtime configuration overrides
# ---------------------------------------------------------------------------
if ($PSBoundParameters.ContainsKey("ControlPort"))   { $env:CONTROL_PORT = "$ControlPort" }
if ($PSBoundParameters.ContainsKey("ControlHost"))   { $env:CONTROL_HOST = $ControlHost }
if ($PSBoundParameters.ContainsKey("AgentPort"))     { $env:PORT         = "$AgentPort" }
if ($PSBoundParameters.ContainsKey("AgentBindHost")) { $env:HOST         = $AgentBindHost }
if ($PSBoundParameters.ContainsKey("DbUrl"))         { $env:DB_URL       = $DbUrl }
if ($PSBoundParameters.ContainsKey("TableFormat"))   { $env:TABLE_FORMAT = $TableFormat }
if ($PSBoundParameters.ContainsKey("HeartbeatMs"))   { $env:HEARTBEAT_MS = "$HeartbeatMs" }
if ($PSBoundParameters.ContainsKey("AgentCount"))    { $env:AGENT_COUNT  = "$AgentCount" }
if ($Gateway)                                        { $env:ENABLE_GATEWAY = "1" }
if ($AdminUi)                                        { $env:ENABLE_ADMIN_UI = "1" }
if ($ConfigUi)                                       { $env:ENABLE_CONFIG_BUILDER = "1" }
# Local launcher: config.connection.json is a local, gitignored per-deployment file, so allow
# an inline db_url by default. -StrictDbCredentials re-enables the secure startup gate.
$env:ALLOW_CONFIG_DB_CREDENTIALS = "1"
if ($StrictDbCredentials)                            { $env:ALLOW_CONFIG_DB_CREDENTIALS = "0" }
if ($PSBoundParameters.ContainsKey("AdminToken"))    { $env:ADMIN_TOKEN  = $AdminToken }
if ($Ha)                                             { $env:MANAGER_HA = "1" }
if ($RetentionGc)                                    { $env:RETENTION_GC = "1" }
if ($PSBoundParameters.ContainsKey("ObjectPathLayout")) { $env:OBJECT_PATH_LAYOUT = $ObjectPathLayout }
if ($DisableLegacyAliases)                           { $env:ENABLE_LEGACY_PATH_ALIASES = "0" }

$effCtlHost   = if ($env:CONTROL_HOST) { $env:CONTROL_HOST } else { "127.0.0.1" }
$effCtlPort   = if ($env:CONTROL_PORT) { $env:CONTROL_PORT } else { "9200" }
$effAgentHost = if ($env:HOST) { $env:HOST } else { "0.0.0.0" }
$effAgentPort = if ($env:PORT) { $env:PORT } else { "9000" }
$effFormat    = if ($env:TABLE_FORMAT) { $env:TABLE_FORMAT } else { "iceberg" }
$effCount     = if ($env:AGENT_COUNT) { [int]$env:AGENT_COUNT } else { 1 }
$effGateway   = ($env:ENABLE_GATEWAY -eq "1")
$effAdminUi   = ($env:ENABLE_ADMIN_UI -eq "1")
$effConfigUi  = ($env:ENABLE_CONFIG_BUILDER -eq "1")
$effHa        = ($env:MANAGER_HA -eq "1")
$effLayout    = if ($env:OBJECT_PATH_LAYOUT) { $env:OBJECT_PATH_LAYOUT } else { "canonical" }
$effAliases   = if ($env:ENABLE_LEGACY_PATH_ALIASES) { ($env:ENABLE_LEGACY_PATH_ALIASES -eq "1") } else { $false }

# Address a client (Fabric) would use to reach an Agent / the gateway when bound 0.0.0.0.
$agentDialHost = if ($effAgentHost -in @("0.0.0.0", "::")) { "127.0.0.1" } else { $effAgentHost }
$ctlDialHost   = if ($effCtlHost   -in @("0.0.0.0", "::")) { "127.0.0.1" } else { $effCtlHost }
$agentPortLast = [int]$effAgentPort + $effCount - 1

# Mask any password in the DB URL for the banner.
$dbUrlRaw = if ($env:DB_URL) { $env:DB_URL } else { "sqlite+aiosqlite:///./poc_source.db" }
$dbUrlMasked = [regex]::Replace($dbUrlRaw, "(://[^:/@]+:)[^@/]*(@)", '$1***$2')

# ---------------------------------------------------------------------------
# 5. Launch the Manager
# ---------------------------------------------------------------------------
Write-Host ""
Write-Step "Starting Manager (control plane + $effCount supervised Agent(s))"
Write-Host "    Manager control : http://${ctlDialHost}:${effCtlPort}   (/healthz  /readyz  /agents)" -ForegroundColor Green
if ($effAdminUi) {
    Write-Host "    Operator console: http://${ctlDialHost}:${effCtlPort}/_manager   <- start/stop/restart/drain + monitor" -ForegroundColor Green
}
if ($effConfigUi) {
    Write-Host "    Config builder  : http://${ctlDialHost}:${effCtlPort}/_config    <- live config UI/API" -ForegroundColor Green
}
if ($effHa) {
    Write-Host "    Manager HA      : leader lease (primary supervises; run a 2nd -Ha Manager as standby)" -ForegroundColor DarkGray
}
if ($effGateway) {
    Write-Host "    S3 gateway (Fabric): http://${ctlDialHost}:${effCtlPort}   <- point the Fabric shortcut here (round-robins the fleet)" -ForegroundColor Green
    Write-Host "    Agents          : http://${agentDialHost}:${effAgentPort} .. :${agentPortLast}  ($effCount, sharded materialize)" -ForegroundColor DarkGray
} else {
    Write-Host "    Agent S3 (Fabric): http://${agentDialHost}:${effAgentPort}   <- point the Fabric shortcut here" -ForegroundColor Green
    if ($effCount -gt 1) {
        Write-Host "    Extra Agents    : http://${agentDialHost}:$([int]$effAgentPort + 1) .. :${agentPortLast}  (add -Gateway to load-balance them)" -ForegroundColor DarkGray
    }
}
Write-Host "    Table format    : $effFormat" -ForegroundColor DarkGray
Write-Host "    Object paths    : layout=$effLayout legacy_aliases=$effAliases" -ForegroundColor DarkGray
Write-Host "    Source DB       : $dbUrlMasked" -ForegroundColor DarkGray
Write-Host "    The Manager restarts any Agent automatically if it crashes." -ForegroundColor DarkGray
Write-Host "    Press Ctrl+C to stop (also stops the supervised Agents)." -ForegroundColor DarkGray
Write-Host ""
& $VenvPython manager.py
exit $LASTEXITCODE
