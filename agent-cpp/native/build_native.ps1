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
$vcpkg = Join-Path $vs "VC\vcpkg\vcpkg.exe"
$cmake = Join-Path $vs "Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
$ninja = Join-Path $vs "Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja\ninja.exe"
foreach ($t in @($vcpkg, $cmake, $ninja)) {
    if (-not (Test-Path $t)) { throw "Required tool not found: $t (install the VS 2022 C++ + CMake components)." }
}

# vcpkg needs a bootstrapped VCPKG_ROOT; the VS copy lives beside vcpkg.exe.
$vcpkgRoot = Split-Path $vcpkg
$toolchain = Join-Path $vcpkgRoot "scripts\buildsystems\vcpkg.cmake"
if (-not (Test-Path $toolchain)) { throw "vcpkg toolchain file not found at $toolchain (bootstrap vcpkg first)." }

$freeGb = [math]::Round((Get-PSDrive C).Free / 1GB, 1)
Write-Host "Free disk on C: $freeGb GB" -ForegroundColor DarkGray
if ($freeGb -lt 30) {
    Write-Warning "Less than 30 GB free. The Arrow/Avro source build may exhaust the disk and fail."
}

$buildDir = Join-Path $here "build"
Write-Host "==> Configuring (vcpkg manifest install runs here)" -ForegroundColor Cyan
& $cmake -S $here -B $buildDir -G "Ninja" `
    "-DCMAKE_MAKE_PROGRAM=$ninja" `
    "-DCMAKE_TOOLCHAIN_FILE=$toolchain" `
    "-DCMAKE_BUILD_TYPE=$BuildType"
if ($LASTEXITCODE -ne 0) { throw "cmake configure failed (exit $LASTEXITCODE)" }

if ($ConfigureOnly) { return }

Write-Host "==> Building" -ForegroundColor Cyan
& $cmake --build $buildDir
if ($LASTEXITCODE -ne 0) { throw "cmake build failed (exit $LASTEXITCODE)" }
Write-Host "==> Built native targets in $buildDir" -ForegroundColor Green
