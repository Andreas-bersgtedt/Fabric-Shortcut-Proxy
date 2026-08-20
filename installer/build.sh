#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

command -v make >/dev/null 2>&1 || {
    printf '%s\n' 'Error: make is required to build the C++ installer.' >&2
    exit 127
}

CXX_COMMAND=${CXX:-c++}
command -v "$CXX_COMMAND" >/dev/null 2>&1 || {
    printf 'Error: C++ compiler not found: %s\n' "$CXX_COMMAND" >&2
    exit 127
}

exec make -B -C "$SCRIPT_DIR" "$@"
