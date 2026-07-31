# Build + run the Tier 1 conformance tests with the MSVC toolchain.
#   .\build_tier1.ps1          # compile tier1_tests.cpp -> tier1_tests.exe and run it
[CmdletBinding()]
param([switch]$Clean, [switch]$NoRun)

$ErrorActionPreference = "Stop"
$here = $PSScriptRoot

if ($Clean) {
    Get-ChildItem $here -Include tier1_tests.obj,tier1_tests.exe -File -ErrorAction SilentlyContinue | Remove-Item -Force
    Write-Host "cleaned." -ForegroundColor DarkGray
    return
}

$vcvars = $null
$vswhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
if (Test-Path $vswhere) {
    $vsroot = & $vswhere -latest -products * -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath 2>$null
    if ($vsroot) { $cand = Join-Path $vsroot "VC\Auxiliary\Build\vcvars64.bat"; if (Test-Path $cand) { $vcvars = $cand } }
}
if (-not $vcvars) {
    foreach ($p in @(
        "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat",
        "C:\Program Files\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat")) {
        if (Test-Path $p) { $vcvars = $p; break }
    }
}
if (-not $vcvars) { throw "vcvars64.bat not found - install Visual Studio 2022 with the C++ workload." }

Write-Host "==> Building tier1_tests.exe (MSVC)" -ForegroundColor Cyan
$cmd = "`"$vcvars`" >nul 2>&1 && cd /d `"$here`" && cl /nologo /EHsc /std:c++17 /O2 /W3 /D_CRT_SECURE_NO_WARNINGS tier1_tests.cpp /Fe:tier1_tests.exe"
cmd /c $cmd
if ($LASTEXITCODE -ne 0) { throw "build failed (exit $LASTEXITCODE)" }
Write-Host "==> Built: $(Join-Path $here 'tier1_tests.exe')" -ForegroundColor Green

if (-not $NoRun) {
    & (Join-Path $here 'tier1_tests.exe')
    if ($LASTEXITCODE -ne 0) { throw "tier1 tests FAILED (exit $LASTEXITCODE)" }
}
