# Build the native Arrow/Parquet + Avro Tier 1 modules via the VS-bundled vcpkg.
#
#   .\build_native.ps1            # install deps (heavy) + configure + build
#   .\build_native.ps1 -ConfigureOnly
#
# WARNING: the first run builds Apache Arrow C++ (parquet) and Avro C++ (Boost)
# from source through vcpkg. That download+build needs roughly 20-40 GB of free
# scratch space and can take tens of minutes. Do not run it on a disk with less
# than about 30 GB free. Prefer a machine or CI runner with headroom, or set
# VCPKG_DEFAULT_BINARY_CACHE to reuse prebuilt binaries.
[CmdletBinding()]
param([switch]$ConfigureOnly, [string]$BuildType = "Release")

$ErrorActionPreference = "Stop"
$here = $PSScriptRoot

$vs = "C:\Program Files\Microsoft Visual Studio\2022\Community"
$cmake = Join-Path $vs "Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
$ninja = Join-Path $vs "Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja\ninja.exe"
foreach ($t in @($cmake, $ninja)) {
    if (-not (Test-Path $t)) { throw "Required tool not found: $t (install the VS 2022 C++ + CMake components)." }
}

# vcpkg needs git history to resolve manifest baselines; the VS-bundled copy has
# none. Prefer a standalone clone at ~\vcpkg (created by git clone + bootstrap),
# then $env:VCPKG_ROOT only if it is a git checkout. This avoids silently using
# the VS-bundled vcpkg (which cannot resolve the pinned builtin-baseline).
function Test-VcpkgGit([string]$root) {
    return $root -and (Test-Path (Join-Path $root 'vcpkg.exe')) -and (Test-Path (Join-Path $root '.git'))
}
$vcpkgRoot = if (Test-VcpkgGit "$env:USERPROFILE\vcpkg") { "$env:USERPROFILE\vcpkg" }
             elseif (Test-VcpkgGit $env:VCPKG_ROOT) { $env:VCPKG_ROOT }
             else { throw "No standalone git-based vcpkg found. Run: git clone https://github.com/microsoft/vcpkg `"$env:USERPROFILE\vcpkg`"; & `"$env:USERPROFILE\vcpkg\bootstrap-vcpkg.bat`" -disableMetrics" }
$toolchain = Join-Path $vcpkgRoot "scripts\buildsystems\vcpkg.cmake"
if (-not (Test-Path $toolchain)) { throw "vcpkg toolchain file not found at $toolchain (bootstrap vcpkg first)." }
Write-Host "Using vcpkg at $vcpkgRoot" -ForegroundColor DarkGray

# Import the MSVC x64 environment (cl.exe on PATH) so the Ninja generator and
# vcpkg can find the compiler. This shell is not a VS Developer shell by default.
if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue)) {
    $vcvars = Join-Path $vs "VC\Auxiliary\Build\vcvars64.bat"
    if (-not (Test-Path $vcvars)) { throw "vcvars64.bat not found at $vcvars (install the VS 2022 C++ build tools)." }
    Write-Host "==> Importing MSVC environment from vcvars64.bat" -ForegroundColor DarkGray
    cmd /c "`"$vcvars`" >nul 2>&1 && set" | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') { Set-Item -Path "Env:$($matches[1])" -Value $matches[2] }
    }
    if (-not (Get-Command cl.exe -ErrorAction SilentlyContinue)) { throw "cl.exe still not on PATH after importing vcvars64.bat." }
}

$freeGb = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
Write-Host "Free disk on C: $freeGb GB" -ForegroundColor DarkGray
if ($freeGb -lt 30) {
    Write-Warning "Less than 30 GB free. The Arrow/Avro source build may exhaust the disk and fail."
}

$buildDir = Join-Path $here "build"
Write-Host "==> Configuring (vcpkg manifest install runs here)" -ForegroundColor Cyan
# CMake writes progress/warnings to stderr; merge into stdout so a benign
# warning does not trip $ErrorActionPreference=Stop into a NativeCommandError.
$eap = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $cmake -S $here -B $buildDir -G "Ninja" `
    "-DCMAKE_MAKE_PROGRAM=$ninja" `
    "-DCMAKE_C_COMPILER=cl" `
    "-DCMAKE_CXX_COMPILER=cl" `
    "-DCMAKE_TOOLCHAIN_FILE=$toolchain" `
    "-DCMAKE_BUILD_TYPE=$BuildType" 2>&1 | ForEach-Object { "$_" }
$cfg = $LASTEXITCODE
$ErrorActionPreference = $eap
if ($cfg -ne 0) { throw "cmake configure failed (exit $cfg)" }

if ($ConfigureOnly) { return }

Write-Host "==> Building" -ForegroundColor Cyan
$ErrorActionPreference = "Continue"
& $cmake --build $buildDir 2>&1 | ForEach-Object { "$_" }
$bld = $LASTEXITCODE
$ErrorActionPreference = $eap
if ($bld -ne 0) { throw "cmake build failed (exit $bld)" }
Write-Host "==> Built native targets in $buildDir" -ForegroundColor Green
