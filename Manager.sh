#!/usr/bin/env bash
# Bootstrap and launch the Fabric Shortcut Proxy Manager on Linux/macOS.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="$SCRIPT_DIR/.venv"
VENV_PYTHON="$VENV_DIR/bin/python"
STAMP_FILE="$VENV_DIR/.deps-installed"

ARG_CONTROL_PORT=""
ARG_CONTROL_HOST=""
ARG_AGENT_PORT=""
ARG_AGENT_BIND_HOST=""
ARG_DB_URL=""
ARG_TABLE_FORMAT=""
ARG_HEARTBEAT_MS=""
ARG_AGENT_COUNT=""
ARG_GATEWAY=0
ARG_ADMIN_UI=0
ARG_ADMIN_TOKEN_SET=0
ARG_ADMIN_TOKEN=""
ARG_HA=0
ARG_RETENTION_GC=0
ARG_BRANCH=""
ARG_REMOTE="origin"
ARG_REPO_URL="https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git"
ARG_NO_PULL=0
ARG_REINSTALL=0
ARG_RECREATE=0
ARG_SKIP_INSTALL=0

step() { printf '==> %s\n' "$1"; }
warn() { printf 'WARN: %s\n' "$1" >&2; }
die() { printf 'ERROR: %s\n' "$1" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: bash Manager.sh [options]

Core options:
  --control-port N       CONTROL_PORT override (default 9200)
  --control-host HOST    CONTROL_HOST override (default 127.0.0.1)
  --agent-port N         Agent S3 PORT override (default 9000)
  --agent-bind-host HOST Agent HOST override (default 0.0.0.0)
  --db-url URL           DB_URL override
  --table-format FMT     TABLE_FORMAT override: iceberg|delta
  --heartbeat-ms N       HEARTBEAT_MS override (default 2000)
  --agent-count N        AGENT_COUNT override (default 1)

Feature flags:
  --gateway              Set ENABLE_GATEWAY=1
  --admin-ui             Set ENABLE_ADMIN_UI=1
  --admin-token TOKEN    Set ADMIN_TOKEN=TOKEN
  --ha                   Set MANAGER_HA=1
  --retention-gc         Set RETENTION_GC=1

Git sync options:
  --branch NAME          Checkout branch before launch
  --remote NAME          Git remote for fetch/pull (default origin)
  --repo-url URL         Repo URL for bootstrap in non-git folder
  --no-pull              Skip git sync step entirely

Bootstrap options:
  --reinstall            Force dependency reinstall
  --recreate             Recreate .venv from scratch
  --skip-install         Skip dependency install

Misc:
  -h, --help             Show this help

Examples:
  bash Manager.sh
  bash Manager.sh --agent-port 9100 --gateway --admin-ui
  bash Manager.sh --db-url "postgresql+asyncpg://user:pass@host/db" --skip-install
EOF
}

require_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "Missing value for $flag"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --control-port) require_value "$1" "${2:-}"; ARG_CONTROL_PORT="$2"; shift 2 ;;
    --control-host) require_value "$1" "${2:-}"; ARG_CONTROL_HOST="$2"; shift 2 ;;
    --agent-port) require_value "$1" "${2:-}"; ARG_AGENT_PORT="$2"; shift 2 ;;
    --agent-bind-host) require_value "$1" "${2:-}"; ARG_AGENT_BIND_HOST="$2"; shift 2 ;;
    --db-url) require_value "$1" "${2:-}"; ARG_DB_URL="$2"; shift 2 ;;
    --table-format) require_value "$1" "${2:-}"; ARG_TABLE_FORMAT="$2"; shift 2 ;;
    --heartbeat-ms) require_value "$1" "${2:-}"; ARG_HEARTBEAT_MS="$2"; shift 2 ;;
    --agent-count) require_value "$1" "${2:-}"; ARG_AGENT_COUNT="$2"; shift 2 ;;
    --gateway) ARG_GATEWAY=1; shift ;;
    --admin-ui) ARG_ADMIN_UI=1; shift ;;
    --admin-token) require_value "$1" "${2:-}"; ARG_ADMIN_TOKEN_SET=1; ARG_ADMIN_TOKEN="$2"; shift 2 ;;
    --ha) ARG_HA=1; shift ;;
    --retention-gc) ARG_RETENTION_GC=1; shift ;;
    --branch) require_value "$1" "${2:-}"; ARG_BRANCH="$2"; shift 2 ;;
    --remote) require_value "$1" "${2:-}"; ARG_REMOTE="$2"; shift 2 ;;
    --repo-url) require_value "$1" "${2:-}"; ARG_REPO_URL="$2"; shift 2 ;;
    --no-pull) ARG_NO_PULL=1; shift ;;
    --reinstall) ARG_REINSTALL=1; shift ;;
    --recreate) ARG_RECREATE=1; shift ;;
    --skip-install) ARG_SKIP_INSTALL=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1 (run with --help)" ;;
  esac
done

if [[ -n "$ARG_TABLE_FORMAT" && "$ARG_TABLE_FORMAT" != "iceberg" && "$ARG_TABLE_FORMAT" != "delta" ]]; then
  die "--table-format must be 'iceberg' or 'delta'"
fi

if [[ $ARG_NO_PULL -eq 1 ]]; then
  [[ -z "$ARG_BRANCH" ]] || warn "--branch given with --no-pull; ignoring (no sync performed)."
elif ! command -v git >/dev/null 2>&1; then
  warn "git not found on PATH - skipping codebase sync."
else
  if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    top_full="$(git rev-parse --show-toplevel 2>/dev/null || true)"
    [[ -z "$top_full" ]] || top_full="$(cd "$top_full" && pwd)"
    if [[ "$top_full" != "$SCRIPT_DIR" ]]; then
      warn "Git root ($top_full) differs from script folder ($SCRIPT_DIR) - skipping sync to avoid touching a parent repo."
    else
      head_before="$(git rev-parse HEAD 2>/dev/null || true)"
      step "Fetching from '$ARG_REMOTE' (in place: $SCRIPT_DIR)"
      git fetch "$ARG_REMOTE" --prune

      if [[ -n "$ARG_BRANCH" ]]; then
        current="$(git rev-parse --abbrev-ref HEAD)"
        if [[ "$current" != "$ARG_BRANCH" ]]; then
          step "Checking out branch '$ARG_BRANCH'"
          git checkout "$ARG_BRANCH"
        fi
      fi

      cur_branch="$(git rev-parse --abbrev-ref HEAD)"
      if [[ "$cur_branch" == "HEAD" ]]; then
        warn "Detached HEAD - skipping pull. Pass --branch to check out a branch."
      else
        step "Pulling '$cur_branch' (fast-forward only)"
        git pull --ff-only "$ARG_REMOTE" "$cur_branch"
      fi

      head_after="$(git rev-parse HEAD 2>/dev/null || true)"
      if [[ -n "$head_before" && -n "$head_after" && "$head_before" != "$head_after" ]]; then
        step "Repo updated: ${head_before:0:7} -> ${head_after:0:7}"
        if [[ $ARG_SKIP_INSTALL -eq 0 && -f "$STAMP_FILE" ]]; then
          rm -f "$STAMP_FILE"
        fi
      else
        step "Repo already up to date"
      fi
    fi
  else
    target_branch="${ARG_BRANCH:-main}"
    step "No git repo here - bootstrapping '$ARG_REPO_URL' ($target_branch) into $SCRIPT_DIR"
    [[ -d "$SCRIPT_DIR/.git" ]] || git init

    if git remote get-url origin >/dev/null 2>&1; then
      git remote set-url origin "$ARG_REPO_URL"
    else
      git remote add origin "$ARG_REPO_URL"
    fi

    step "Fetching '$target_branch' from origin"
    git fetch origin "$target_branch"

    step "Populating the working tree (force checkout '$target_branch')"
    git checkout -f -B "$target_branch" FETCH_HEAD
    git branch --set-upstream-to "origin/$target_branch" "$target_branch" >/dev/null 2>&1 || true

    step "Codebase bootstrapped in place (branch '$target_branch'). Re-run if this script was updated."
    if [[ $ARG_SKIP_INSTALL -eq 0 && -f "$STAMP_FILE" ]]; then
      rm -f "$STAMP_FILE"
    fi
  fi
fi

resolve_base_python() {
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return 0
  fi
  return 1
}

if [[ $ARG_RECREATE -eq 1 && -d "$VENV_DIR" ]]; then
  step "Removing existing virtual environment"
  rm -rf "$VENV_DIR"
fi

if [[ ! -x "$VENV_PYTHON" ]]; then
  base_python="$(resolve_base_python || true)"
  [[ -n "$base_python" ]] || die "No Python interpreter found. Install Python 3.11+ and ensure python3 or python is on PATH."
  step "Using base interpreter: $base_python ($($base_python --version 2>&1))"
  step "Creating virtual environment at .venv"
  "$base_python" -m venv "$VENV_DIR"
fi

[[ -x "$VENV_PYTHON" ]] || die "Virtual environment python not found at $VENV_PYTHON"

if [[ $ARG_SKIP_INSTALL -eq 1 ]]; then
  step "Skipping dependency installation (--skip-install)"
elif [[ $ARG_REINSTALL -eq 1 || ! -f "$STAMP_FILE" ]]; then
  step "Upgrading pip"
  "$VENV_PYTHON" -m pip install --upgrade pip --quiet

  step "Installing project dependencies from pyproject.toml"
  "$VENV_PYTHON" -m pip install -e . --quiet

  : > "$STAMP_FILE"
  step "Dependencies installed"
else
  step "Dependencies already installed (use --reinstall to refresh)"
fi

[[ -z "$ARG_CONTROL_PORT" ]] || export CONTROL_PORT="$ARG_CONTROL_PORT"
[[ -z "$ARG_CONTROL_HOST" ]] || export CONTROL_HOST="$ARG_CONTROL_HOST"
[[ -z "$ARG_AGENT_PORT" ]] || export PORT="$ARG_AGENT_PORT"
[[ -z "$ARG_AGENT_BIND_HOST" ]] || export HOST="$ARG_AGENT_BIND_HOST"
[[ -z "$ARG_DB_URL" ]] || export DB_URL="$ARG_DB_URL"
[[ -z "$ARG_TABLE_FORMAT" ]] || export TABLE_FORMAT="$ARG_TABLE_FORMAT"
[[ -z "$ARG_HEARTBEAT_MS" ]] || export HEARTBEAT_MS="$ARG_HEARTBEAT_MS"
[[ -z "$ARG_AGENT_COUNT" ]] || export AGENT_COUNT="$ARG_AGENT_COUNT"
[[ $ARG_GATEWAY -eq 0 ]] || export ENABLE_GATEWAY="1"
[[ $ARG_ADMIN_UI -eq 0 ]] || export ENABLE_ADMIN_UI="1"
[[ $ARG_ADMIN_TOKEN_SET -eq 0 ]] || export ADMIN_TOKEN="$ARG_ADMIN_TOKEN"
[[ $ARG_HA -eq 0 ]] || export MANAGER_HA="1"
[[ $ARG_RETENTION_GC -eq 0 ]] || export RETENTION_GC="1"

eff_ctl_host="${CONTROL_HOST:-127.0.0.1}"
eff_ctl_port="${CONTROL_PORT:-9200}"
eff_agent_host="${HOST:-0.0.0.0}"
eff_agent_port="${PORT:-9000}"
eff_format="${TABLE_FORMAT:-iceberg}"
eff_count="${AGENT_COUNT:-1}"
eff_gateway="${ENABLE_GATEWAY:-0}"
eff_admin_ui="${ENABLE_ADMIN_UI:-0}"
eff_ha="${MANAGER_HA:-0}"

agent_dial_host="$eff_agent_host"
if [[ "$eff_agent_host" == "0.0.0.0" || "$eff_agent_host" == "::" ]]; then
  agent_dial_host="127.0.0.1"
fi

ctl_dial_host="$eff_ctl_host"
if [[ "$eff_ctl_host" == "0.0.0.0" || "$eff_ctl_host" == "::" ]]; then
  ctl_dial_host="127.0.0.1"
fi

agent_port_last=$((eff_agent_port + eff_count - 1))
db_url_raw="${DB_URL:-sqlite+aiosqlite:///./poc_source.db}"
db_url_masked="$(printf '%s' "$db_url_raw" | sed -E 's#(://[^:/@]+:)[^@/]*(@)#\1***\2#')"

printf '\n'
step "Starting Manager (control plane + ${eff_count} supervised Agent(s))"
printf '    Manager control : http://%s:%s   (/healthz  /readyz  /agents)\n' "$ctl_dial_host" "$eff_ctl_port"
if [[ "$eff_admin_ui" == "1" ]]; then
  printf '    Operator console: http://%s:%s/_manager   <- start/stop/restart/drain + monitor\n' "$ctl_dial_host" "$eff_ctl_port"
fi
if [[ "$eff_ha" == "1" ]]; then
  printf '    Manager HA      : leader lease (primary supervises; run a 2nd --ha Manager as standby)\n'
fi
if [[ "$eff_gateway" == "1" ]]; then
  printf '    S3 gateway (Fabric): http://%s:%s   <- point the Fabric shortcut here (round-robins the fleet)\n' "$ctl_dial_host" "$eff_ctl_port"
  printf '    Agents          : http://%s:%s .. :%s  (%s, sharded materialize)\n' "$agent_dial_host" "$eff_agent_port" "$agent_port_last" "$eff_count"
else
  printf '    Agent S3 (Fabric): http://%s:%s   <- point the Fabric shortcut here\n' "$agent_dial_host" "$eff_agent_port"
  if (( eff_count > 1 )); then
    printf '    Extra Agents    : http://%s:%s .. :%s  (add --gateway to load-balance them)\n' "$agent_dial_host" "$((eff_agent_port + 1))" "$agent_port_last"
  fi
fi
printf '    Table format    : %s\n' "$eff_format"
printf '    Source DB       : %s\n' "$db_url_masked"
printf '    The Manager restarts any Agent automatically if it crashes.\n'
printf '    Press Ctrl+C to stop (also stops the supervised Agents).\n\n'

exec "$VENV_PYTHON" manager.py
