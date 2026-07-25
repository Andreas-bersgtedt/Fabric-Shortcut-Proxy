# Build the C++ serving Agent with the MSVC toolchain (Phase 6).
#   .\build.ps1            # compile agent.cpp -> agent.exe
# Requires Visual Studio 2022 (or Build Tools) with the C++ workload.
[CmdletBinding()]
param([switch]$Clean)

$ErrorActionPreference = "Stop"
$here = $PSScriptRoot

if ($Clean) {
    Get-ChildItem $here -Include *.obj,*.exe,build.txt -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Write-Host "cleaned." -ForegroundColor DarkGray
    return
}

# Locate vcvars64.bat (prefer vswhere, else well-known paths).
$vcvars = $null
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (Test-Path $vswhere) {
    $vsroot = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
    if ($vsroot) { $cand = Join-Path $vsroot "VC\Auxiliary\Build\vcvars64.bat"; if (Test-Path $cand) { $vcvars = $cand } }
}
if (-not $vcvars) {
    foreach ($p in @(
        "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat",
        "C:\Program Files\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat",
        "C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Auxiliary\Build\vcvars64.bat")) {
        if (Test-Path $p) { $vcvars = $p; break }
    }
}
if (-not $vcvars) { throw "vcvars64.bat not found - install Visual Studio 2022 with the 'Desktop development with C++' workload." }

Write-Host "==> Building agent.exe (MSVC)" -ForegroundColor Cyan
$cmd = "`"$vcvars`" >nul 2>&1 && cd /d `"$here`" && cl /nologo /EHsc /std:c++17 /O2 /W3 agent.cpp /Fe:agent.exe"
cmd /c $cmd
if ($LASTEXITCODE -ne 0) { throw "build failed (exit $LASTEXITCODE)" }

Write-Host "==> Built: $(Join-Path $here 'agent.exe')" -ForegroundColor Green
