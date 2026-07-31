#!/usr/bin/env bash
# Build + run the Tier 1 conformance tests with g++ (Linux/macOS).
set -euo pipefail
cd "$(dirname "$0")"

if [[ "${1:-}" == "--clean" ]]; then
  rm -f tier1_tests
  exit 0
fi

CXX="${CXX:-g++}"
CXXFLAGS="${CXXFLAGS:--O2 -std=c++17 -Wall -Wextra}"

$CXX $CXXFLAGS tier1_tests.cpp -o tier1_tests
echo "Built $(pwd)/tier1_tests"

if [[ "${1:-}" != "--no-run" ]]; then
  ./tier1_tests
fi
