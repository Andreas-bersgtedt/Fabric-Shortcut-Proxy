#!/bin/sh
#
# Fabric Shortcut Proxy SSH-safe installer.
#
# This installer collects and validates setup decisions, writes only
# non-secret checkpoint data, and requires APPLY before host mutations.

set -eu

NO_COLOR=0
DRY_RUN=0
CHECK_ONLY=0
RESUME=0
ANSWERS_FILE=
STEP=1
TOTAL_STEPS=8
STATE_DIR=${FSP_INSTALLER_STATE_DIR:-/var/lib/fabric-shortcut-proxy}
STATE_FILE=
TTY=/dev/tty
SECRET_TEMP_PATH=

INSTALL_DIR=${FSP_INSTALL_DIR:-/opt/fabric-shortcut-proxy}
SERVICE_USER=${FSP_SERVICE_USER:-fsp}
SERVICE_GROUP=${FSP_SERVICE_GROUP:-fsp}
UNIT_NAME=${FSP_UNIT_NAME:-fabric-shortcut-proxy.service}
IDENTITY_MODE=
TENANT_ID=
CLIENT_ID=
CLIENT_SECRET_REF=
CLIENT_SECRET_VALUE=
KEYVAULT_MODE=disabled
KEYVAULT_URI=
TLS_MODE=disabled
TLS_HOSTNAME=
TLS_CERT_FILE=
TLS_KEY_FILE=
SECRET_BACKEND=
ENV_FILE=${FSP_ENV_FILE:-/etc/fabric-shortcut-proxy.env}
START_SERVICE=no
HEALTH_URL=${FSP_HEALTH_URL:-http://127.0.0.1:9200/healthz}
MANAGER_AUTH_USERNAME=operator
GENERATE_AGENT_TOKEN=no
GENERATE_S3_CREDENTIALS=yes
GENERATE_ADMIN_CREDENTIALS=yes
AGENT_TOKEN_VALUE=
ADMIN_TOKEN_VALUE=
MANAGER_AUTH_PASSWORD_VALUE=
S3_ACCESS_KEY_VALUE=
S3_SECRET_KEY_VALUE=
APPLY_REQUESTED=0
RESET_ADMIN_PASSWORD=0
RESTART_AFTER_RESET=no

usage() {
    cat <<'EOF'
Fabric Shortcut Proxy installer

Usage:
  sudo ./installer.sh
  sudo ./installer.sh --resume
  sudo ./installer.sh --answers FILE
  sudo ./installer.sh --check
  ./installer.sh --dry-run

Options:
  --resume       Resume the last incomplete run.
  --answers FILE Read non-secret answers from KEY=VALUE lines.
  --check        Run read-only host checks and exit.
  --dry-run      Run the wizard without changing the host.
  --reset-admin-password
                  Generate and store a new Manager admin password.
  --restart       Restart the service after resetting the password.
  --no-color     Disable ANSI color output.
  --help         Show this help.

Secret values must be supplied through protected files or environment
variables referenced by the answers file. They are never checkpointed.
The installer does not configure config.*.json files.
EOF
}

die() {
    printf '%s\n' "Error: $*" >&2
    exit 1
}

cleanup() {
    stty echo 2>/dev/null || true
    if [ -n "$SECRET_TEMP_PATH" ]; then
        rm -f "$SECRET_TEMP_PATH"
    fi
}

trap cleanup EXIT INT TERM

print_logo() {
    if [ "$NO_COLOR" -eq 1 ]; then
        printf '%s\n' '  FSP'
    else
        printf '\033[3m%s\033[0m\n' '  FSP'
    fi
    printf '%s\n' '  FABRIC SHORTCUT PROXY'
}

print_rule() {
    printf '%s\n' '========================================================================'
}

read_answer() {
    prompt=$1
    default=${2-}
    current=${3-}
    key=${4:-$prompt}
    if [ -n "$current" ]; then
        default=$current
    fi
    if [ -n "$default" ]; then
        printf '%s [%s]: ' "$prompt" "$default" >&2
    else
        printf '%s: ' "$prompt" >&2
    fi
    if [ -n "$ANSWERS_FILE" ]; then
        answer=$(answer_value "$key")
    elif [ "$DRY_RUN" -eq 1 ]; then
        answer=$default
    else
        [ -t 0 ] || [ -r "$TTY" ] || die 'interactive input is required; use --answers FILE'
        IFS= read -r answer < "$TTY" || true
    fi
    if [ -z "$answer" ]; then
        answer=$default
    fi
    printf '%s' "$answer"
}

read_secret_reference() {
    prompt=$1
    key=${2:-$prompt}
    if [ -n "$ANSWERS_FILE" ]; then
        answer=$(answer_value "$key")
    else
        printf '%s' "$prompt" >&2
        stty -echo < "$TTY"
        IFS= read -r answer < "$TTY" || true
        stty echo < "$TTY"
        printf '%s\n' >&2
    fi
    case "$answer" in
        env:*) printf '%s' "$answer" ;;
        file:*) printf '%s' "$answer" ;;
        '') die 'a secret reference is required (env:NAME or file:/path)' ;;
        *) die 'secret values must use env:NAME or file:/path references' ;;
    esac
}

answer_value() {
    key=$1
    [ -f "$ANSWERS_FILE" ] || die "answers file not found: $ANSWERS_FILE"
    value=$(awk -F= -v wanted="$key" '
        /^[[:space:]]*#/ { next }
        index($0, "=") == 0 { next }
        {
          key=$1
          sub(/^[[:space:]]+/, "", key)
          sub(/[[:space:]]+$/, "", key)
          if (key == wanted) {
            value=substr($0, index($0, "=") + 1)
            sub(/\r$/, "", value)
            print value
            exit
          }
        }' "$ANSWERS_FILE")
    printf '%s' "$value"
}

validate_answers() {
    [ -n "$ANSWERS_FILE" ] || return 0
    [ -f "$ANSWERS_FILE" ] || die "answers file not found: $ANSWERS_FILE"
    awk -F= '
        /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
        index($0, "=") == 0 { print "line without KEY=VALUE"; exit 1 }
        {
          key=$1
          sub(/^[[:space:]]+/, "", key)
          sub(/[[:space:]]+$/, "", key)
          if (key !~ /^[A-Za-z_][A-Za-z0-9_]*$/) { print "invalid key: " key; exit 1 }
          if (seen[key]++) { print "duplicate key: " key; exit 1 }
          if (key != "APPLY" && key != "install_dir" && key != "service_user" &&
              key != "service_group" && key != "unit_name" && key != "identity_mode" &&
              key != "tenant_id" && key != "client_id" && key != "client_secret_reference" &&
              key != "keyvault_mode" && key != "keyvault_uri" && key != "secret_backend" &&
              key != "manager_auth_username" && key != "generate_admin_credentials" &&
              key != "generate_s3_credentials" && key != "generate_agent_token" &&
              key != "tls_mode" && key != "tls_hostname" && key != "tls_cert_file" &&
              key != "tls_key_file" && key != "start_service") {
              print "unknown key: " key; exit 1
          }
        }
    ' "$ANSWERS_FILE" || die 'answers file validation failed'
}

validate_install_dir() {
    case "$INSTALL_DIR" in
        ''|/|/etc|/usr|/var|/home|/root|/opt) die "unsafe installation directory: $INSTALL_DIR" ;;
        /*) ;;
        *) die 'installation directory must be absolute' ;;
    esac
    if [ -e "$INSTALL_DIR" ] && [ ! -d "$INSTALL_DIR" ]; then
        die "installation path is not a directory: $INSTALL_DIR"
    fi
}

atomic_replace() {
    source=$1
    target=$2
    if [ -e "$target" ]; then
        backup="${target}.bak.$(date -u +%Y%m%dT%H%M%SZ)"
        cp -p "$target" "$backup" || die "could not back up $target"
        chmod 600 "$backup" 2>/dev/null || true
    fi
    mv "$source" "$target" || die "could not install $target"
    if command -v sync >/dev/null 2>&1; then
        sync
    fi
}

save_state() {
    [ "$DRY_RUN" -eq 1 ] && return 0
    mkdir -p "$STATE_DIR"
    umask 077
    tmp="$STATE_FILE.tmp.$$"
    {
        printf '%s\n' 'version=1'
        printf 'step=%s\n' "$STEP"
        printf 'install_dir=%s\n' "$INSTALL_DIR"
        printf 'service_user=%s\n' "$SERVICE_USER"
        printf 'service_group=%s\n' "$SERVICE_GROUP"
        printf 'unit_name=%s\n' "$UNIT_NAME"
        printf 'identity_mode=%s\n' "$IDENTITY_MODE"
        printf 'tenant_id=%s\n' "$TENANT_ID"
        printf 'client_id=%s\n' "$CLIENT_ID"
        printf 'client_secret_ref=%s\n' "$CLIENT_SECRET_REF"
        printf 'keyvault_mode=%s\n' "$KEYVAULT_MODE"
        printf 'keyvault_uri=%s\n' "$KEYVAULT_URI"
        printf 'tls_mode=%s\n' "$TLS_MODE"
        printf 'tls_hostname=%s\n' "$TLS_HOSTNAME"
        printf 'tls_cert_file=%s\n' "$TLS_CERT_FILE"
        printf 'tls_key_file=%s\n' "$TLS_KEY_FILE"
        printf 'secret_backend=%s\n' "$SECRET_BACKEND"
        printf 'env_file=%s\n' "$ENV_FILE"
        printf 'manager_auth_username=%s\n' "$MANAGER_AUTH_USERNAME"
    } > "$tmp"
    atomic_replace "$tmp" "$STATE_FILE"
}

load_state() {
    [ -f "$STATE_FILE" ] || return 0
    state_version=$(awk -F= '$1 == "version" { print $2; exit }' "$STATE_FILE")
    [ "$state_version" = 1 ] ||
        die "installer state is corrupt or incompatible: $STATE_FILE; move it aside and rerun without --resume"
    awk -F= '
        /^[[:space:]]*$/ { next }
        NF < 2 { exit 1 }
        $1 !~ /^[A-Za-z_][A-Za-z0-9_]*$/ { exit 1 }
    ' "$STATE_FILE" ||
        die "installer state is unreadable: $STATE_FILE; move it aside and rerun without --resume"
    # State contains only installer-generated, non-secret scalar values.
    while IFS='=' read -r key value; do
        case "$key" in
            step) STEP=$value ;;
            install_dir) INSTALL_DIR=$value ;;
            service_user) SERVICE_USER=$value ;;
            service_group) SERVICE_GROUP=$value ;;
            unit_name) UNIT_NAME=$value ;;
            identity_mode) IDENTITY_MODE=$value ;;
            tenant_id) TENANT_ID=$value ;;
            client_id) CLIENT_ID=$value ;;
            client_secret_ref) CLIENT_SECRET_REF=$value ;;
            keyvault_mode) KEYVAULT_MODE=$value ;;
            keyvault_uri) KEYVAULT_URI=$value ;;
            tls_mode) TLS_MODE=$value ;;
            tls_hostname) TLS_HOSTNAME=$value ;;
            tls_cert_file) TLS_CERT_FILE=$value ;;
            tls_key_file) TLS_KEY_FILE=$value ;;
            secret_backend) SECRET_BACKEND=$value ;;
            env_file) ENV_FILE=$value ;;
            manager_auth_username) MANAGER_AUTH_USERNAME=$value ;;
        esac
    done < "$STATE_FILE"
}

recover_existing_configuration() {
    if [ ! -r "$ENV_FILE" ] && command -v systemctl >/dev/null 2>&1; then
        discovered_env=$(systemctl show "$UNIT_NAME" --property=EnvironmentFiles --value 2>/dev/null |
            awk '{for (i = 1; i <= NF; i++) {value=$i; sub(/^-/, "", value); if (value ~ /^\//) {print value; exit}}}')
        [ -n "$discovered_env" ] && ENV_FILE=$discovered_env
    fi
    [ -r "$ENV_FILE" ] || die "existing service environment file not found: $ENV_FILE"
    KEYVAULT_URI=$(awk -F= '$1 == "FSP_KEYVAULT_URI" {print substr($0, index($0, "=") + 1); exit}' "$ENV_FILE")
    MANAGER_AUTH_USERNAME=$(awk -F= '$1 == "MANAGER_AUTH_USERNAME" {print substr($0, index($0, "=") + 1); exit}' "$ENV_FILE")
    [ -n "$MANAGER_AUTH_USERNAME" ] || MANAGER_AUTH_USERNAME=operator
    if [ -n "$KEYVAULT_URI" ]; then
        SECRET_BACKEND=keyvault
        if grep -q '^FSP_REQUIRE_KEYVAULT=1$' "$ENV_FILE"; then
            KEYVAULT_MODE=required
        else
            KEYVAULT_MODE=read-through
        fi
    else
        SECRET_BACKEND=env-file
        KEYVAULT_MODE=disabled
    fi
}

step_header() {
    printf '%s\n' ''
    print_rule
    printf '  Step %s of %s: %s\n' "$STEP" "$TOTAL_STEPS" "$1"
    print_rule
}

run_check() {
    printf '%s\n' 'Fabric Shortcut Proxy installer checks'
    printf '%s\n' ''
    printf '  OS:          %s\n' "$(uname -s 2>/dev/null || printf '%s' unknown)"
    printf '  Host:        %s\n' "$(hostname 2>/dev/null || printf '%s' unknown)"
    printf '  User:        %s\n' "$(id -un 2>/dev/null || printf '%s' unknown)"
    printf '  Python:      '
    if command -v python3 >/dev/null 2>&1; then
        python3 --version 2>&1
    else
        printf '%s\n' 'missing'
    fi
    printf '  Git:         %s\n' "$(command -v git >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    printf '  Curl:        %s\n' "$(command -v curl >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    printf '  Systemd:     %s\n' "$(command -v systemctl >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    if command -v systemctl >/dev/null 2>&1; then
        printf '  Service:      '
        systemctl is-active "$UNIT_NAME" 2>/dev/null || printf '%s\n' 'inactive'
    fi
    if [ -n "$TLS_CERT_FILE" ] && [ -n "$TLS_KEY_FILE" ]; then
        printf '  TLS files:    '
        if [ -r "$TLS_CERT_FILE" ] && [ -r "$TLS_KEY_FILE" ]; then
            printf '%s\n' 'readable'
        else
            printf '%s\n' 'missing or unreadable'
        fi
    fi
    printf '  State file:  %s\n' "$STATE_FILE"
}

welcome() {
    print_logo
    print_rule
    printf '%s\n' '  Welcome to the Fabric Shortcut Proxy setup wizard.'
    printf '%s\n' ''
    printf '%s\n' '  This installer prepares identity, secrets, TLS, and the system service.'
    printf '%s\n' '  It does not create source tables, proxy settings, or Open Mirror'
    printf '%s\n' '  mappings. Existing config.*.json files are left unchanged.'
    printf '%s\n' ''
    printf '  Host:        %s\n' "$(hostname 2>/dev/null || printf '%s' unknown)"
    printf '  Target:      %s\n' "$INSTALL_DIR"
    printf '  Mode:        %s\n' "$([ "$DRY_RUN" -eq 1 ] && printf '%s' dry-run || printf '%s' interactive)"
    printf '%s\n' ''
    printf '%s\n' '  Planned setup areas:'
    printf '%s\n' '    [ ] Host and service identity'
    printf '%s\n' '    [ ] SPA / MSAL identity'
    printf '%s\n' '    [ ] Azure Key Vault'
    printf '%s\n' '    [ ] Agent and Manager credentials'
    printf '%s\n' '    [ ] SSL/TLS and certificate checks'
    printf '%s\n' '    [ ] Runtime and systemd'
    printf '%s\n' '    [ ] Health checks'
    printf '%s\n' ''
    print_rule
}

confirm() {
    [ "$DRY_RUN" -eq 1 ] && return 0
    if [ -n "$ANSWERS_FILE" ]; then
        answer=$(answer_value APPLY)
    else
        answer=$(read_answer 'Type APPLY to continue' '')
    fi
    [ "$answer" = APPLY ] || die 'setup cancelled; APPLY was not entered'
}

host_step() {
    step_header 'Host and service identity'
    INSTALL_DIR=$(read_answer 'Installation directory' "$INSTALL_DIR" "$INSTALL_DIR" install_dir)
    SERVICE_USER=$(read_answer 'Service user' "$SERVICE_USER" "$SERVICE_USER" service_user)
    SERVICE_GROUP=$(read_answer 'Service group' "$SERVICE_GROUP" "$SERVICE_GROUP" service_group)
    UNIT_NAME=$(read_answer 'systemd unit name' "$UNIT_NAME" "$UNIT_NAME" unit_name)
    validate_install_dir
    command -v python3 >/dev/null 2>&1 || die 'python3 is required'
    command -v git >/dev/null 2>&1 || die 'git is required'
    command -v curl >/dev/null 2>&1 || die 'curl is required'
    command -v systemctl >/dev/null 2>&1 || die 'systemd is required'
    python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' ||
        die 'Python 3.11 or newer is required'
    STEP=2
    save_state
}

identity_step() {
    step_header 'SPA / MSAL identity'
    IDENTITY_MODE=$(read_answer 'Identity (managed_identity, service_principal, default)' 'managed_identity' "$IDENTITY_MODE" identity_mode)
    case "$IDENTITY_MODE" in
        managed_identity|default) ;;
        service_principal)
            TENANT_ID=$(read_answer 'Tenant ID' '' "$TENANT_ID" tenant_id)
            CLIENT_ID=$(read_answer 'Client/application ID' '' "$CLIENT_ID" client_id)
            [ -n "$TENANT_ID" ] && [ -n "$CLIENT_ID" ] || die 'tenant and client IDs are required'
            printf '%s\n' '  Client secret reference (not stored):'
            CLIENT_SECRET_REF=$(read_secret_reference '  Reference: ' client_secret_reference)
            ;;
        *) die 'unsupported identity mode' ;;
    esac
    STEP=3
    save_state
}

keyvault_step() {
    step_header 'Azure Key Vault'
    KEYVAULT_MODE=$(read_answer 'Key Vault mode (disabled, read-through, write-back, required)' 'disabled' "$KEYVAULT_MODE" keyvault_mode)
    case "$KEYVAULT_MODE" in
        disabled) KEYVAULT_URI= ;;
        read-through|write-back|required)
            KEYVAULT_URI=$(read_answer 'Key Vault URI' '' "$KEYVAULT_URI" keyvault_uri)
            [ -n "$KEYVAULT_URI" ] || die 'Key Vault URI is required'
            case "$KEYVAULT_URI" in https://*.vault.azure.net/*|https://*.vault.azure.net) ;; *) die 'Key Vault URI must use https://<name>.vault.azure.net' ;; esac
            ;;
        *) die 'unsupported Key Vault mode' ;;
    esac
    STEP=4
    save_state
}

credentials_step() {
    step_header 'Agent and Manager credentials'
    backend_default=env-file
    [ "$KEYVAULT_MODE" != disabled ] && backend_default=keyvault
    SECRET_BACKEND=$(read_answer 'Secret backend (keyvault, env-file)' "$backend_default" "$SECRET_BACKEND" secret_backend)
    case "$SECRET_BACKEND" in keyvault|env-file) ;; *) die 'unsupported secret backend' ;; esac
    if [ "$SECRET_BACKEND" = keyvault ] && [ "$KEYVAULT_MODE" = disabled ]; then
        die 'keyvault secret backend requires an enabled Key Vault mode'
    fi
    if [ "$SECRET_BACKEND" = keyvault ] && [ "$IDENTITY_MODE" = service_principal ]; then
        die 'service-principal identity requires env-file bootstrap; choose env-file or managed_identity'
    fi
    MANAGER_AUTH_USERNAME=$(read_answer 'Manager auth username' 'operator' "$MANAGER_AUTH_USERNAME" manager_auth_username)
    GENERATE_ADMIN_CREDENTIALS=$(read_answer 'Generate admin token and password (yes/no)' 'yes' "$GENERATE_ADMIN_CREDENTIALS" generate_admin_credentials)
    case "$GENERATE_ADMIN_CREDENTIALS" in yes) ;; no) ;; *) die 'answer yes or no' ;; esac
    GENERATE_S3_CREDENTIALS=$(read_answer 'Generate S3 access credentials (yes/no)' 'yes' "$GENERATE_S3_CREDENTIALS" generate_s3_credentials)
    case "$GENERATE_S3_CREDENTIALS" in yes) ;; no) ;; *) die 'answer yes or no' ;; esac
    GENERATE_AGENT_TOKEN=$(read_answer 'Generate an unused AGENT_TOKEN placeholder (yes/no)' 'no' "$GENERATE_AGENT_TOKEN" generate_agent_token)
    case "$GENERATE_AGENT_TOKEN" in yes) ;; no) ;; *) die 'answer yes or no' ;; esac
    if [ "$GENERATE_ADMIN_CREDENTIALS" = yes ] || [ "$GENERATE_S3_CREDENTIALS" = yes ] || [ "$GENERATE_AGENT_TOKEN" = yes ]; then
        command -v openssl >/dev/null 2>&1 || die 'openssl is required to generate credentials'
    fi
    printf '%s\n' '  Credentials are separate: Agent token, admin password, and admin token.'
    [ "$GENERATE_AGENT_TOKEN" = yes ] && printf '%s\n' '  AGENT_TOKEN is not consumed by the current Manager/Agent runtime.'
    STEP=5
    save_state
}

tls_step() {
    step_header 'SSL/TLS and certificate checks'
    TLS_MODE=$(read_answer 'TLS mode (disabled, nginx, direct)' 'disabled' "$TLS_MODE" tls_mode)
    case "$TLS_MODE" in
        disabled) TLS_HOSTNAME=; TLS_CERT_FILE=; TLS_KEY_FILE= ;;
        nginx|direct)
            TLS_HOSTNAME=$(read_answer 'DNS hostname' '' "$TLS_HOSTNAME" tls_hostname)
            TLS_CERT_FILE=$(read_answer 'Certificate/full-chain path' '' "$TLS_CERT_FILE" tls_cert_file)
            TLS_KEY_FILE=$(read_answer 'Private-key path' '' "$TLS_KEY_FILE" tls_key_file)
            [ -n "$TLS_HOSTNAME" ] && [ -n "$TLS_CERT_FILE" ] && [ -n "$TLS_KEY_FILE" ] || die 'TLS hostname, certificate, and key are required'
            [ -r "$TLS_CERT_FILE" ] || die "certificate is not readable: $TLS_CERT_FILE"
            [ -r "$TLS_KEY_FILE" ] || die "private key is not readable: $TLS_KEY_FILE"
            command -v openssl >/dev/null 2>&1 || die 'openssl is required for TLS validation'
            openssl x509 -in "$TLS_CERT_FILE" -noout >/dev/null 2>&1 || die 'certificate validation failed'
            openssl x509 -noout -checkend 0 -in "$TLS_CERT_FILE" >/dev/null 2>&1 || die 'certificate is expired'
            cert_public_key=$(openssl x509 -in "$TLS_CERT_FILE" -pubkey -noout |
                openssl pkey -pubin -outform DER 2>/dev/null | openssl sha256)
            key_public_key=$(openssl pkey -in "$TLS_KEY_FILE" -pubout -outform DER 2>/dev/null |
                openssl sha256)
            [ -n "$cert_public_key" ] && [ "$cert_public_key" = "$key_public_key" ] ||
                die 'certificate and private key do not match'
            openssl x509 -in "$TLS_CERT_FILE" -noout -checkhost "$TLS_HOSTNAME" >/dev/null 2>&1 ||
                die "certificate does not cover hostname: $TLS_HOSTNAME"
            if [ "$TLS_MODE" = nginx ]; then
                command -v nginx >/dev/null 2>&1 || die 'nginx is required for nginx TLS mode'
            fi
            ;;
        *) die 'unsupported TLS mode' ;;
    esac
    STEP=6
    save_state
}

runtime_step() {
    step_header 'Runtime and systemd'
    printf '%s\n' "  Unit: $UNIT_NAME"
    printf '%s\n' "  User: $SERVICE_USER"
    printf '%s\n' "  Working directory: $INSTALL_DIR"
    printf '%s\n' '  No unit is written until APPLY is confirmed on the review screen.'
    STEP=7
    save_state
}

checks_step() {
    step_header 'Health checks'
    printf '%s\n' '  Read-only checks:'
    printf '    Python:       %s\n' "$(command -v python3 >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    printf '    Git:          %s\n' "$(command -v git >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    printf '    Systemd:      %s\n' "$(command -v systemctl >/dev/null 2>&1 && printf '%s' found || printf '%s' missing)"
    if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$UNIT_NAME"; then
        printf '    Service:      active\n'
        if command -v curl >/dev/null 2>&1; then
            printf '    Health:       '
            curl --fail --silent --show-error --max-time 15 "$HEALTH_URL" >/dev/null 2>&1 &&
                printf '%s\n' 'healthy' ||
                printf '%s\n' "failed ($HEALTH_URL)"
        fi
    else
        printf '    Service:      inactive\n'
    fi
    if [ "$TLS_MODE" != disabled ]; then
        printf '    Certificate:  %s\n' "$TLS_CERT_FILE"
        printf '%s\n' '    Private key:  configured (path withheld from logs)'
    else
        printf '%s\n' '    TLS:          disabled'
    fi
    if [ "$KEYVAULT_MODE" != disabled ] && [ -n "$KEYVAULT_URI" ]; then
        printf '    Key Vault:    '
        vault_name=$(printf '%s' "$KEYVAULT_URI" | awk -F/ '{print $3}' | sed 's/\.vault\.azure\.net$//')
        if command -v az >/dev/null 2>&1 &&
            az keyvault secret list --vault-name "$vault_name" --query '[].name' -o tsv >/dev/null 2>&1; then
            printf '%s\n' 'readable'
        else
            printf '%s\n' 'unavailable'
        fi
    fi
    STEP=9
    save_state
}

review_step() {
    step_header 'Review and apply'
    printf '%s\n' '  Redacted setup summary:'
    printf '    Installation: %s\n' "$INSTALL_DIR"
    printf '    Service:      %s:%s\n' "$SERVICE_USER" "$SERVICE_GROUP"
    printf '    Identity:     %s\n' "$IDENTITY_MODE"
    printf '    Key Vault:    %s\n' "$KEYVAULT_MODE"
    [ -n "$KEYVAULT_URI" ] && printf '    Vault URI:    %s\n' "$KEYVAULT_URI"
    printf '    Credentials:  %s\n' "$SECRET_BACKEND"
    printf '    TLS:          %s\n' "$TLS_MODE"
    [ -n "$TLS_HOSTNAME" ] && printf '    TLS host:     %s\n' "$TLS_HOSTNAME"
    printf '%s\n' ''
    printf '%s\n' '  No config.*.json files will be changed.'
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '%s\n' '  Dry-run complete. No host changes were made.'
    else
        START_SERVICE=$(read_answer 'Start and enable the systemd service (yes/no)' 'no' "$START_SERVICE" start_service)
        case "$START_SERVICE" in yes|no) ;; *) die 'answer yes or no' ;; esac
        confirm
        APPLY_REQUESTED=1
        apply_setup
    fi
    STEP=8
    save_state
}

apply_setup() {
    [ "$APPLY_REQUESTED" -eq 1 ] || return 0
    [ "$(id -u)" -eq 0 ] || die 'apply requires root; rerun with sudo'
    [ -d "$INSTALL_DIR" ] || die "installation directory does not exist: $INSTALL_DIR"
    command -v systemctl >/dev/null 2>&1 || die 'systemctl is required to apply service setup'
    if [ "$IDENTITY_MODE" = service_principal ]; then
        command -v az >/dev/null 2>&1 || die 'Azure CLI is required to validate service-principal identity'
    fi
    if [ "$KEYVAULT_MODE" != disabled ]; then
        command -v az >/dev/null 2>&1 || die 'Azure CLI is required for Key Vault provisioning'
    fi

    if [ "$GENERATE_ADMIN_CREDENTIALS" = yes ]; then
        ADMIN_TOKEN_VALUE=$(openssl rand -hex 32)
        MANAGER_AUTH_PASSWORD_VALUE=$(openssl rand -base64 32 2>/dev/null | tr -d '\n' | cut -c1-32)
        [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] || die 'failed to generate manager password'
    fi
    if [ "$GENERATE_S3_CREDENTIALS" = yes ]; then
        S3_ACCESS_KEY_VALUE="fsp-$(openssl rand -hex 12)"
        S3_SECRET_KEY_VALUE=$(openssl rand -hex 32)
    fi
    if [ "$GENERATE_AGENT_TOKEN" = yes ]; then
        AGENT_TOKEN_VALUE=$(openssl rand -hex 32)
    fi
    if [ -n "$CLIENT_SECRET_REF" ]; then
        case "$CLIENT_SECRET_REF" in
            env:*)
                secret_env_name=${CLIENT_SECRET_REF#env:}
                [ -n "$secret_env_name" ] || die 'empty client secret environment reference'
                case "$secret_env_name" in
                    ''|*[!A-Za-z0-9_]*)
                        die 'client secret environment reference must contain only letters, numbers, and underscores'
                        ;;
                esac
                CLIENT_SECRET_VALUE=$(printenv "$secret_env_name" 2>/dev/null || true)
                ;;
            file:*)
                secret_file=${CLIENT_SECRET_REF#file:}
                [ -r "$secret_file" ] || die "client secret file is not readable: $secret_file"
                IFS= read -r CLIENT_SECRET_VALUE < "$secret_file" || true
                ;;
            *) die 'invalid client secret reference' ;;
        esac
        [ -n "$CLIENT_SECRET_VALUE" ] || die 'client secret reference resolved to an empty value'
    fi
    if [ "$IDENTITY_MODE" = service_principal ]; then
        AZURE_TENANT_ID=$TENANT_ID AZURE_CLIENT_ID=$CLIENT_ID \
            AZURE_CLIENT_SECRET=$CLIENT_SECRET_VALUE \
            az account get-access-token --resource https://api.fabric.microsoft.com \
            --output none >/dev/null 2>&1 ||
            die 'service-principal could not obtain a Fabric token; verify tenant, application, secret, and permissions'
    fi

    if ! getent group "$SERVICE_GROUP" >/dev/null 2>&1; then
        groupadd --system "$SERVICE_GROUP"
    fi
    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        useradd --system --create-home --home-dir "/var/lib/$SERVICE_USER" \
            --shell /usr/sbin/nologin --gid "$SERVICE_GROUP" "$SERVICE_USER"
    fi
    chown "$SERVICE_USER:$SERVICE_GROUP" "$INSTALL_DIR"

    umask 077
    env_tmp="$ENV_FILE.tmp.$$"
    mkdir -p "$(dirname "$ENV_FILE")"
    {
        printf '%s\n' "AUTH_MODE=$IDENTITY_MODE"
        [ -n "$TENANT_ID" ] && printf '%s\n' "AZURE_TENANT_ID=$TENANT_ID"
        [ -n "$CLIENT_ID" ] && printf '%s\n' "AZURE_CLIENT_ID=$CLIENT_ID"
        [ -n "$CLIENT_SECRET_VALUE" ] && printf '%s\n' "AZURE_CLIENT_SECRET=$CLIENT_SECRET_VALUE"
        [ -n "$KEYVAULT_URI" ] && printf '%s\n' "FSP_KEYVAULT_URI=$KEYVAULT_URI"
        [ "$KEYVAULT_MODE" = required ] && printf '%s\n' 'FSP_REQUIRE_KEYVAULT=1'
        [ -n "$TLS_CERT_FILE" ] && printf '%s\n' "TLS_CERT_FILE=$TLS_CERT_FILE"
        [ -n "$TLS_KEY_FILE" ] && printf '%s\n' "TLS_KEY_FILE=$TLS_KEY_FILE"
        if [ "$SECRET_BACKEND" = env-file ]; then
            [ -n "$ADMIN_TOKEN_VALUE" ] && printf '%s\n' "ADMIN_TOKEN=$ADMIN_TOKEN_VALUE"
            [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] && printf '%s\n' 'MANAGER_AUTH_ENABLED=1'
            [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] && printf '%s\n' "MANAGER_AUTH_USERNAME=$MANAGER_AUTH_USERNAME"
            [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] && printf '%s\n' "MANAGER_AUTH_PASSWORD=$MANAGER_AUTH_PASSWORD_VALUE"
            [ -n "$S3_ACCESS_KEY_VALUE" ] && printf '%s\n' "S3_ACCESS_KEY_ID=$S3_ACCESS_KEY_VALUE"
            [ -n "$S3_SECRET_KEY_VALUE" ] && printf '%s\n' "S3_SECRET_ACCESS_KEY=$S3_SECRET_KEY_VALUE"
            [ -n "$AGENT_TOKEN_VALUE" ] && printf '%s\n' "AGENT_TOKEN=$AGENT_TOKEN_VALUE"
        fi
    } > "$env_tmp"
    chmod 600 "$env_tmp"
    chown "$SERVICE_USER:$SERVICE_GROUP" "$env_tmp"
    atomic_replace "$env_tmp" "$ENV_FILE"

    if [ "$SECRET_BACKEND" = keyvault ]; then
        vault_name=$(printf '%s' "$KEYVAULT_URI" | awk -F/ '{print $3}' | sed 's/\.vault\.azure\.net$//')
        [ -n "$vault_name" ] || die 'could not derive Key Vault name'
        az keyvault secret list --vault-name "$vault_name" --query '[].name' -o tsv >/dev/null ||
            die 'Key Vault read permission check failed'
        set_keyvault_secret() {
            kv_name=$1
            kv_value=$2
            kv_tmp="$STATE_DIR/.$$.secret"
            SECRET_TEMP_PATH=$kv_tmp
            umask 077
            printf '%s' "$kv_value" > "$kv_tmp"
            chmod 600 "$kv_tmp"
            az keyvault secret set --vault-name "$vault_name" --name "$kv_name" --file "$kv_tmp" >/dev/null ||
                die "Key Vault write failed for $kv_name"
            rm -f "$kv_tmp"
            SECRET_TEMP_PATH=
        }
        [ -n "$ADMIN_TOKEN_VALUE" ] && set_keyvault_secret admin-token "$ADMIN_TOKEN_VALUE"
        [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] && set_keyvault_secret manager-auth-password "$MANAGER_AUTH_PASSWORD_VALUE"
        [ -n "$S3_SECRET_KEY_VALUE" ] && set_keyvault_secret s3-secret-access-key "$S3_SECRET_KEY_VALUE"
        [ -n "$AGENT_TOKEN_VALUE" ] && set_keyvault_secret agent-token "$AGENT_TOKEN_VALUE"
    fi

    unit_tmp="/etc/systemd/system/$UNIT_NAME.tmp.$$"
    {
        printf '%s\n' '[Unit]'
        printf '%s\n' 'Description=Fabric Shortcut Proxy Manager'
        printf '%s\n' 'After=network-online.target'
        printf '%s\n' 'Wants=network-online.target'
        printf '%s\n' ''
        printf '%s\n' '[Service]'
        printf '%s\n' 'Type=simple'
        printf '%s\n' "User=$SERVICE_USER"
        printf '%s\n' "Group=$SERVICE_GROUP"
        printf '%s\n' "WorkingDirectory=$INSTALL_DIR"
        [ "$SECRET_BACKEND" = env-file ] && printf '%s\n' "EnvironmentFile=$ENV_FILE"
        printf '%s\n' "ExecStart=/bin/bash $INSTALL_DIR/Manager.sh --admin-ui --config-ui --auto-stash"
        printf '%s\n' 'Restart=on-failure'
        printf '%s\n' 'RestartSec=5'
        printf '%s\n' 'TimeoutStartSec=600'
        printf '%s\n' 'LimitNOFILE=65536'
        printf '%s\n' 'NoNewPrivileges=true'
        printf '%s\n' 'PrivateTmp=true'
        printf '%s\n' 'ProtectSystem=full'
        printf '%s\n' 'ProtectHome=true'
        printf '%s\n' 'KillMode=control-group'
        printf '%s\n' ''
        printf '%s\n' '[Install]'
        printf '%s\n' 'WantedBy=multi-user.target'
    } > "$unit_tmp"
    chmod 644 "$unit_tmp"
    atomic_replace "$unit_tmp" "/etc/systemd/system/$UNIT_NAME"
    systemd-analyze verify "/etc/systemd/system/$UNIT_NAME"
    systemctl daemon-reload
    if [ "$START_SERVICE" = yes ]; then
        systemctl enable "$UNIT_NAME"
        systemctl start "$UNIT_NAME"
        systemctl is-active --quiet "$UNIT_NAME" ||
            die "service did not become active; inspect journalctl -u $UNIT_NAME"
        if command -v curl >/dev/null 2>&1; then
            curl --fail --silent --show-error --max-time 15 "$HEALTH_URL" >/dev/null ||
                die "health check failed at $HEALTH_URL; inspect journalctl -u $UNIT_NAME"
        fi
    fi
    printf '%s\n' "  Applied service unit: /etc/systemd/system/$UNIT_NAME"
    printf '%s\n' "  Environment file: $ENV_FILE (mode 0600)"
}

reset_admin_password() {
    [ "$RESET_ADMIN_PASSWORD" -eq 1 ] || return 0
    [ "$(id -u)" -eq 0 ] || die 'admin password reset requires root; rerun with sudo'
    [ -n "$SECRET_BACKEND" ] || die 'no saved installer configuration; run the setup wizard first'
    command -v openssl >/dev/null 2>&1 || die 'openssl is required to reset the admin password'
    MANAGER_AUTH_PASSWORD_VALUE=$(openssl rand -base64 32 2>/dev/null | tr -d '\n' | cut -c1-32)
    [ -n "$MANAGER_AUTH_PASSWORD_VALUE" ] || die 'failed to generate manager password'

    if [ "$SECRET_BACKEND" = keyvault ]; then
        [ "$KEYVAULT_MODE" != disabled ] || die 'saved Key Vault mode is disabled'
        command -v az >/dev/null 2>&1 || die 'Azure CLI is required for Key Vault password reset'
        vault_name=$(printf '%s' "$KEYVAULT_URI" | awk -F/ '{print $3}' | sed 's/\.vault\.azure\.net$//')
        [ -n "$vault_name" ] || die 'could not derive Key Vault name'
        kv_tmp="$STATE_DIR/.$$.secret"
        SECRET_TEMP_PATH=$kv_tmp
        umask 077
        printf '%s' "$MANAGER_AUTH_PASSWORD_VALUE" > "$kv_tmp"
        chmod 600 "$kv_tmp"
        az keyvault secret set --vault-name "$vault_name" --name manager-auth-password \
            --file "$kv_tmp" >/dev/null || die 'Key Vault admin password reset failed'
        rm -f "$kv_tmp"
        SECRET_TEMP_PATH=
    else
        umask 077
        mkdir -p "$(dirname "$ENV_FILE")"
        env_tmp="$ENV_FILE.reset.tmp.$$"
        if [ -f "$ENV_FILE" ]; then
            cp "$ENV_FILE" "$env_tmp" || die "could not stage $ENV_FILE for password reset"
        else
            : > "$env_tmp"
        fi
        printf '\nMANAGER_AUTH_ENABLED=1\nMANAGER_AUTH_USERNAME=%s\nMANAGER_AUTH_PASSWORD=%s\n' \
            "$MANAGER_AUTH_USERNAME" "$MANAGER_AUTH_PASSWORD_VALUE" >> "$env_tmp"
        chmod 600 "$env_tmp"
        chown "$SERVICE_USER:$SERVICE_GROUP" "$env_tmp" 2>/dev/null || true
        atomic_replace "$env_tmp" "$ENV_FILE"
    fi

    if [ "$RESTART_AFTER_RESET" = yes ]; then
        command -v systemctl >/dev/null 2>&1 || die 'systemctl is required to restart the service'
        systemctl restart "$UNIT_NAME"
        systemctl is-active --quiet "$UNIT_NAME" ||
            die "service did not become active after password reset; inspect journalctl -u $UNIT_NAME"
    fi
    printf '%s\n' "  Manager admin password reset completed using $SECRET_BACKEND."
    [ "$RESTART_AFTER_RESET" = yes ] || printf '%s\n' "  Restart $UNIT_NAME before using the new password."
}

main() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --resume) RESUME=1 ;;
            --answers) shift; [ "$#" -gt 0 ] || die '--answers requires a file'; ANSWERS_FILE=$1 ;;
            --check) CHECK_ONLY=1 ;;
            --dry-run) DRY_RUN=1 ;;
            --reset-admin-password) RESET_ADMIN_PASSWORD=1 ;;
            --restart) RESTART_AFTER_RESET=yes ;;
            --no-color) NO_COLOR=1 ;;
            --help|-h) usage; exit 0 ;;
            *) die "unknown option: $1" ;;
        esac
        shift
    done

    STATE_FILE=$STATE_DIR/installer-state
    validate_answers
    [ "$RESUME" -eq 1 ] && load_state
    if [ "$CHECK_ONLY" -eq 1 ] && [ -f "$STATE_FILE" ]; then
        load_state
    fi
    if [ "$CHECK_ONLY" -eq 1 ]; then
        run_check
        exit 0
    fi
    if [ "$RESET_ADMIN_PASSWORD" -eq 1 ]; then
        if [ -f "$STATE_FILE" ]; then
            load_state
        else
            recover_existing_configuration
        fi
        reset_admin_password
        exit 0
    fi
    welcome
    [ "$STEP" -gt 1 ] || confirm
    [ "$STEP" -le 1 ] && host_step
    [ "$STEP" -le 2 ] && identity_step
    [ "$STEP" -le 3 ] && keyvault_step
    [ "$STEP" -le 4 ] && credentials_step
    [ "$STEP" -le 5 ] && tls_step
    [ "$STEP" -le 6 ] && runtime_step
    [ "$STEP" -le 7 ] && review_step
    [ "$STEP" -le 8 ] && checks_step
    printf '%s\n' ''
    printf '%s\n' '  Installer flow complete. Run --check to inspect the host.'
}

main "$@"
