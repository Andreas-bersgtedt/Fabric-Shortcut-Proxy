# ASCII installer plan

## Scope

Build a Linux-only, SSH-safe setup wizard for:

- SPA, meaning the Entra service-principal application used by the Manager;
- MSAL-compatible identity setup and token validation;
- Azure Key Vault connection and baseline secret seeding;
- Agent token generation or entry;
- Manager admin password and admin token setup;
- SSL/TLS termination and certificate validation;
- service user, filesystem permissions, systemd unit, enablement, and health checks.

The installer must **not** configure source tables, proxy ports, output formats, Open Mirror
mappings, cleanup policies, or other application settings. Users continue to manage those
through the existing `config.*.json` files and Config Builder. The installer prepares the
identity and secret plumbing those settings consume.

`Manager.sh` remains the application launcher. The root `installer.sh` rebuilds the C++
frontend in `installer/fsp-installer` before every launch. A failed build stops the
installer instead of executing a stale binary. The C++ frontend owns terminal navigation;
the shell installer remains the provisioning implementation until each step is migrated
and tested.

The installer prepares the host and service,
then performs the first service checks. It must be safe to rerun after a failed or
disconnected SSH session.

## User experience

Default command:

```bash
sudo ./installer.sh
```

The flow is one screen at a time:

```text
Fabric Shortcut Proxy setup
===========================
Step 3 of 8: Azure application identity

  1) Existing managed identity
  2) Existing service principal
  3) Show service-principal setup commands

Select [1-3], or B to go back, Q to quit:
```

Every step:

1. explains what it changes;
2. shows existing non-secret values when rerun;
3. masks secrets;
4. validates input before advancing;
5. supports `B`, `N`, and `Q` where applicable;
6. saves a non-secret checkpoint after success.

The final screen shows a redacted summary, service name, secret backend, and verification
commands. It never prints passwords, client secrets, database URLs containing credentials,
access keys, tokens, or token contents.

## Proposed steps

### 1. Welcome and safety

- Display the target directory, detected distribution, and current service status.
- Explain that the wizard can install packages, create a service account, write secret
  material, and install a systemd unit.
- Require explicit confirmation before host changes.
- Use `sudo` only for privileged operations. Keep application files owned by the service
  user.
- Refuse to overwrite an active deployment without confirmation.

### 2. Host prerequisites and service identity

- Detect `apt`, `dnf`, or `yum`.
- Check Python 3.11+, `venv`, `pip`, Git, `curl`, and systemd.
- Install only dependencies needed for the selected identity and secret backend.
- Ask for installation directory, default `/opt/fabric-shortcut-proxy`.
- Ask for service user, default `fsp`, group, and unit name.
- Create a non-login service account when needed.
- Refuse unsafe paths such as `/`, `/etc`, `/usr`, or an unrelated existing directory.
- Clone or update the repository without destructive pulls.
- Store installer state under `/var/lib/fabric-shortcut-proxy/`, never in the repository.

### 3. SPA and MSAL identity

Treat “SPA” as the Entra service-principal application used by the Manager.

- Offer:
  - existing managed identity;
  - existing service principal;
  - reviewed Azure CLI commands for creating or assigning a service principal.
- Do not guess tenant, subscription, application, or permissions.
- Collect tenant ID, client/application ID, and service-principal secret through masked
  input or protected file references.
- Configure the existing identity path in `security.azure_credential.proxy_credential`.
- Validate the credential by requesting a token for Fabric or Key Vault.
- Never print or persist the access token, and never pass the client secret in argv.
- Explain required Azure permissions and stop on authorization errors. Do not silently
  switch to another identity.

### 4. Azure Key Vault

- Offer disabled, read-through, write-back, or required modes matching the existing
  Key Vault settings.
- Collect the vault URI and non-secret identity settings.
- Offer to seed baseline secrets using the existing naming convention:
  - Manager/admin password;
  - Agent token;
  - S3 access key and secret;
  - optional Manager authentication password.
- Show secret names, never values.
- Test read permissions and, only when selected, write permissions through the existing
  Key Vault adapter.
- Prefer a service identity that reads Key Vault at runtime, so local files contain only
  non-secret vault settings.

### 5. Agent token and administrator credentials

- Generate cryptographically random values or accept masked operator input.
- Treat the Agent token, Manager admin password, and Manager admin token as separate
  credentials. They must not be interchangeable.
- Enforce minimum lengths and reject placeholders, repeated-character values, and empty
  values.
- Store values in Key Vault, the encrypted credential store, or a local environment file
  with mode `0600`.
- Show only a masked fingerprint for each stored value.
- Test the Manager-to-Agent authenticated handshake after service startup.

### 6. SSL/TLS

- Offer TLS disabled, existing certificate files, Let's Encrypt through nginx, or an
  enterprise CA certificate and private key.
- Ask for the DNS name and public listener choices. Do not treat a bare IP or self-signed
  certificate as suitable for the Fabric data plane.
- Keep application listeners on loopback when nginx terminates public HTTPS.
- Store private keys with mode `0600`, owned by the selected service or nginx account.
- Validate certificate and key pairing, hostname coverage, expiration, full-chain ordering,
  and nginx configuration before reload.
- Show certificate paths and public endpoints in the redacted review screen, never private
  key contents.
- For Let's Encrypt, install renewal hooks and verify that renewal reloads nginx.
- Permit self-signed certificates only for explicitly marked lab or operator-console use.
- Keep certificate provisioning and nginx files installer-owned. Do not rewrite
  user-owned `config.*.json` files.

### 7. Runtime and secret backend

- Offer Lite or Enterprise, default Enterprise because `Manager.sh` targets the
  Manager/Agent service.
- Create `.venv` as the service user.
- Install only selected extras:
  - `azureblob` for Azure identity support;
  - `keyvault` for Azure Key Vault;
  - `credentials` for the encrypted local store.
- Prefer Key Vault references. Fall back to the encrypted store, then a protected
  environment file.
- Never put secrets in repository files, shell history, argv, journal output, temporary
  files with default permissions, or installer state.

### 8. Review, apply, and systemd

Show a redacted setup summary grouped by host, identity, Key Vault, credentials, and
service. Require an explicit `APPLY` confirmation.

Write only installer-owned files. Do not rewrite user-owned `config.*.json` files.
Use mode `0600` temporary files, `fsync`, and atomic rename. Back up existing
installer-owned files before replacement.

Render a systemd unit with:

- selected `User` and `Group`;
- `WorkingDirectory`;
- `ExecStart` invoking `Manager.sh`;
- `EnvironmentFile` only for the selected local secret backend;
- restart policy and startup timeout;
- resource limits;
- hardening settings compatible with the application filesystem and network behavior.

Run `systemd-analyze verify`, then require confirmation before `daemon-reload`, enable,
and start operations.

### 9. Start and verify

- Start or restart the service with a bounded timeout.
- Check systemd active state and `/healthz`.
- Check Key Vault access when enabled.
- Check HTTPS endpoints, certificate hostname coverage, and TLS handshake when enabled.
- Check the Manager-to-Agent authenticated handshake.
- Check service-user ownership and permissions for installer-owned files.
- Print `journalctl -u ...` and the existing Linux troubleshooting guide when checks fail.
- Offer to leave the service stopped after installation for manual review.

### 10. C++ provisioning migration

- Keep `installer/install.sh` as the provisioning authority until a step has parity tests.
- Migrate one bounded step at a time; preserve `--answers`, `--resume`, `--check`, and
  `--dry-run` behavior during each migration.
- Keep secrets out of the C++ process arguments, logs, checkpoints, and frontend output.
- The first migration slice is answers-file contract validation and interactive
  answer collection in the C++ frontend.
- A failed C++ validation must stop before the shell installer starts.
- The default Start setup wizard must not expose the shell installer's prompts. It may
  invoke the shell backend non-interactively through a protected answers file until
  provisioning logic has parity tests in C++.
- Each later slice must include a Linux build check, shell parity tests, and an explicit
  fallback to the shell implementation until the C++ path is accepted.

The C++ menu also exposes a password-reset action. It generates a new Manager admin
password in the shell backend, stores it in the configured secret backend, and never
displays or places the value in process arguments. Reset mode can recover a
pre-installer deployment from its systemd environment file when no installer checkpoint
exists.

## SSH and terminal safety

- Use POSIX shell and standard utilities. Do not require `dialog`, `whiptail`, `fzf`,
  curses, terminal resizing, or mouse input.
- Support `TERM=dumb`, no color, narrow terminals, pasted values, and interrupted
  sessions.
- Detect non-interactive stdin. Require an answers file in that mode; reject missing
  required values instead of guessing.
- Provide `--no-color` even when color detection would disable color automatically.
- Trap `INT`, `TERM`, and `EXIT`; restore terminal echo after hidden input.
- Use `printf`, not `echo`, for user-controlled text.
- Never pipe secret input through `tee`, `sed`, `env`, `ps`, or shell tracing.

## Rerun and recovery

Use `/var/lib/fabric-shortcut-proxy/installer-state.json`. Record completed step names,
selected paths, package checks, identity mode, secret backend, and a hash of non-secret
inputs. Never record secret values.

On rerun:

- offer resume or a fresh identity/service setup;
- preserve existing valid secret material by default;
- create timestamped backups before replacing installer-owned files;
- detect and report drift in the systemd unit or secret backend;
- refuse destructive cleanup unless the operator explicitly confirms it.

## Command-line modes

```text
installer.sh               Interactive SSH-safe setup
installer.sh --resume      Resume the last incomplete run
installer.sh --answers FILE Non-interactive setup for automation
installer.sh --check       Check identity, Key Vault, service, and permissions
installer.sh --dry-run     Show actions without changing the host
installer.sh --help        Show options
```

The first shell implementation uses a strict `KEY=VALUE` answers file documented in
`docs/ASCII_INSTALLER_ANSWERS.example`. Secrets are provided through environment-variable
names or protected file references, not inline values.

The current foundation implements host validation, identity selection, Key Vault settings,
Key Vault or protected environment-file provisioning, TLS file checks, systemd unit
rendering, and read-only health checks. Encrypted credential-store provisioning remains
outside the installer because that store manages database connection URLs, not the
Manager's generated credentials. `ADMIN_TOKEN`, `MANAGER_AUTH_PASSWORD`, and S3 credentials
are generated only after `APPLY`; generated values are written to the selected protected
backend and never to installer state. The current application has no separate
`AGENT_TOKEN` setting, so the wizard labels that optional value as unused rather than
pretending it authenticates Manager-to-Agent traffic.

## Implementation layout

```text
installer.sh              C++ frontend dispatcher with shell fallback
installer/
  install.sh               Line-based provisioning fallback
 main.cpp                 Arrow-key SSH frontend
 Makefile                 Linux build
 build.sh                 Build wrapper
 README.md                Build and fallback behavior
  test_installer_systemd.py
```

Keep privileged commands behind an allowlisted runner. Unit tests mock that boundary and
never invoke `sudo`, package managers, or systemd on the development host.

## Delivery phases

1. Foundation: prompts, masking, navigation, answers files, checkpoints, and dry-run.
2. Identity: SPA/MSAL modes, Azure CLI command preview, token validation, and permissions.
3. Key Vault: connection validation, secret naming, baseline seeding, and fail-closed
   behavior for required mode.
4. Credentials: Agent token, admin password, admin token, fingerprints, and local store.
5. SSL/TLS: nginx or direct certificate setup, certificate checks, renewal, and endpoint
   verification.
6. Service: service account, atomic installer-owned files, systemd unit, and health checks.
7. Documentation and release: Linux guide, answers schema, troubleshooting, CI tests,
   and an SSH acceptance script.

## Acceptance criteria

- A fresh Ubuntu or Debian host completes setup over SSH without a browser.
- The flow works with `TERM=dumb` and no ANSI color.
- A disconnected session resumes without re-entering saved non-secret answers.
- Secrets never appear in output, logs, argv, repository config, or installer state.
- `--answers` produces the same identity and service setup as interactive mode.
- `--dry-run` performs no package, file, Key Vault, systemd, or network mutation.
- Invalid or unauthorized SPA/MSAL credentials stop setup with a useful remediation.
- Required Key Vault mode fails closed when the vault cannot be reached.
- Agent and admin credentials remain separate and are verified independently.
- TLS certificates are validated before activation, and private keys never appear in output,
  logs, argv, or installer state.
- Generated units pass `systemd-analyze verify`.
- Existing user-owned `config.*.json` files remain unchanged.
- Existing unattended `Manager.sh` operation remains unchanged.
