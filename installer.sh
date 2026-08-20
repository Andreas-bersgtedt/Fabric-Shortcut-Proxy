#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CPP_INSTALLER="$ROOT_DIR/installer/fsp-installer"

printf '%s\n' 'Building the SSH-safe C++ installer...'
"$ROOT_DIR/installer/build.sh"

[ -x "$CPP_INSTALLER" ] || {
    printf '%s\n' 'Error: C++ installer build did not produce installer/fsp-installer.' >&2
    exit 1
}

exec "$CPP_INSTALLER" "$@"
