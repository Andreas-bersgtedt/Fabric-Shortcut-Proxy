#!/bin/sh
#
# Fabric Shortcut Proxy installer entry point.
#
# The first installer phase is intentionally read-only. It establishes the
# terminal-safe welcome flow before privileged setup steps are added.

set -eu

NO_COLOR=0
DRY_RUN=0

usage() {
    cat <<'EOF'
Fabric Shortcut Proxy installer

Usage:
  ./install.sh [--dry-run] [--no-color]
  ./install.sh --help

Options:
  --dry-run   Show the welcome flow without changing the host.
  --no-color  Disable ANSI color output.
  --help      Show this help.

The installer does not configure config.*.json files.
EOF
}

die() {
    printf '%s\n' "Error: $*" >&2
    exit 1
}

print_logo() {
    # Keep the logo within 80 columns for SSH sessions and TERM=dumb.
    printf '%s\n' \
        '  ______ _   ________' \
        ' / ____// | / / ____/' \
        '/ /_   /  |/ / /     ' \
        '/ __/  / /|  / /___   ' \
        '/_/    /_/ |_/\____/  ' \
        '' \
        '  FABRIC SHORTCUT PROXY'
}

print_rule() {
    printf '%s\n' '========================================================================'
}

print_host_summary() {
    os_name=$(uname -s 2>/dev/null || printf '%s' 'unknown')
    host_name=$(hostname 2>/dev/null || printf '%s' 'unknown')
    target_dir=${FSP_INSTALL_DIR:-/opt/fabric-shortcut-proxy}

    printf '%s\n' "  Host:        $host_name"
    printf '%s\n' "  Platform:    $os_name"
    printf '%s\n' "  Target:      $target_dir"
    printf '%s\n' "  Mode:        $([ "$DRY_RUN" -eq 1 ] && printf '%s' 'dry-run' || printf '%s' 'interactive')"
}

print_welcome() {
    print_logo
    print_rule
    printf '%s\n' '  Welcome to the Fabric Shortcut Proxy setup wizard.'
    printf '%s\n' ''
    printf '%s\n' '  This installer prepares identity, secrets, and the system service.'
    printf '%s\n' '  It does not create source tables, proxy settings, or Open Mirror'
    printf '%s\n' '  mappings. Existing config.*.json files are left unchanged.'
    printf '%s\n' ''
    print_host_summary
    printf '%s\n' ''
    printf '%s\n' '  Planned setup areas:'
    printf '%s\n' '    [ ] SPA / MSAL identity'
    printf '%s\n' '    [ ] Azure Key Vault'
    printf '%s\n' '    [ ] Agent and Manager credentials'
    printf '%s\n' '    [ ] SSL/TLS and certificate checks'
    printf '%s\n' '    [ ] Service user and systemd'
    printf '%s\n' '    [ ] Health checks'
    printf '%s\n' ''
    print_rule
}

confirm_start() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '%s\n' '  Dry-run selected. No host changes will be made.'
        return 0
    fi

    if [ ! -t 0 ]; then
        die 'interactive input is required; use --dry-run or a future --answers file'
    fi

    printf '%s' '  Press Enter to continue, or Q to quit: '
    IFS= read -r answer || true
    case "$answer" in
        q|Q)
            printf '%s\n' 'Setup cancelled.'
            exit 0
            ;;
        '')
            return 0
            ;;
        *)
            die 'enter only Enter or Q'
            ;;
    esac
}

main() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --dry-run)
                DRY_RUN=1
                ;;
            --no-color)
                NO_COLOR=1
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                die "unknown option: $1"
                ;;
        esac
        shift
    done

    print_welcome
    confirm_start
    printf '%s\n' ''
    printf '%s\n' '  Installer foundation complete. No setup steps were applied.'
    printf '%s\n' '  Future phases will add resumable identity, Key Vault, credential,'
    printf '%s\n' '  systemd, and health-check screens.'
}

main "$@"
