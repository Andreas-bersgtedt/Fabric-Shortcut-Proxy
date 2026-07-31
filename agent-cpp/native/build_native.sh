#!/usr/bin/env bash
# Build the native Arrow/Parquet + Avro Tier 1 modules on Linux via a standalone
# vcpkg (with git history, so the pinned builtin-baseline in vcpkg.json resolves).
#
#   ./build_native.sh                 # install deps (heavy) + configure + build
#   ./build_native.sh --configure-only
#
# WARNING: the first run builds Apache Arrow C++ (parquet) and Avro C++ (Boost)
# from source through vcpkg. That download+build needs roughly 20-40 GB of free
# scratch space and can take tens of minutes. Prefer a CI runner with headroom,
# or set VCPKG_DEFAULT_BINARY_CACHE to reuse prebuilt binaries. Requires g++ (or
# clang), cmake, ninja, git, curl, zip, unzip, tar, pkg-config on PATH.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
configure_only=0
build_type="Release"
for arg in "$@"; do
    case "$arg" in
        --configure-only) configure_only=1 ;;
        --debug) build_type="Debug" ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

for tool in cmake ninja git g++; do
    command -v "$tool" >/dev/null 2>&1 || { echo "required tool not found: $tool" >&2; exit 1; }
done

# vcpkg needs git history to resolve the manifest baseline. Prefer $VCPKG_ROOT if
# it is a git checkout, else a standalone clone at ~/vcpkg (create + bootstrap).
vcpkg_git() { [ -n "${1:-}" ] && [ -x "$1/vcpkg" ] && [ -d "$1/.git" ]; }
if vcpkg_git "${VCPKG_ROOT:-}"; then
    vcpkg_root="$VCPKG_ROOT"
elif vcpkg_git "$HOME/vcpkg"; then
    vcpkg_root="$HOME/vcpkg"
else
    echo "==> Cloning + bootstrapping vcpkg at $HOME/vcpkg"
    git clone --depth 1 https://github.com/microsoft/vcpkg "$HOME/vcpkg"
    "$HOME/vcpkg/bootstrap-vcpkg.sh" -disableMetrics
    vcpkg_root="$HOME/vcpkg"
fi
toolchain="$vcpkg_root/scripts/buildsystems/vcpkg.cmake"
[ -f "$toolchain" ] || { echo "vcpkg toolchain not found at $toolchain" >&2; exit 1; }
echo "Using vcpkg at $vcpkg_root"

avail_gb="$(df -BG --output=avail "$here" | tail -1 | tr -dc '0-9')"
echo "Free disk near source: ${avail_gb} GB"
[ "${avail_gb:-0}" -ge 30 ] || echo "WARNING: <30 GB free; the Arrow/Avro source build may exhaust the disk."

build_dir="$here/build"
echo "==> Configuring (vcpkg manifest install runs here)"
cmake -S "$here" -B "$build_dir" -G Ninja \
    "-DCMAKE_TOOLCHAIN_FILE=$toolchain" \
    "-DCMAKE_BUILD_TYPE=$build_type"

if [ "$configure_only" -eq 1 ]; then
    exit 0
fi

echo "==> Building"
cmake --build "$build_dir"
echo "==> Built native targets in $build_dir"
