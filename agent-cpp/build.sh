#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ "${1:-}" == "--clean" ]]; then
  rm -f agent
  exit 0
fi

CXX="${CXX:-g++}"
CXXFLAGS="${CXXFLAGS:--O2 -std=c++17 -Wall -Wextra -pthread}"

$CXX $CXXFLAGS agent.cpp -o agent

echo "Built $(pwd)/agent"
