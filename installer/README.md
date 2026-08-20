# C++ installer

The C++ frontend provides SSH-safe arrow-key navigation, collects the setup
answers, and validates the non-secret answers-file contract before delegation.
The Start setup wizard action is now the default interactive path. It passes a
protected temporary answers file to `installer/install.sh`, so the shell backend
applies the reviewed values without showing its line-based prompts. The line-based
installer remains an explicit fallback and remains the source of truth for
checkpointing, secret handling, TLS validation, and systemd setup.

The root dispatcher always rebuilds the C++ binary before launching it. A failed
build stops the installer instead of running a stale binary. Build directly on Linux:

```sh
./installer/build.sh
```

Confirm the deployed cutover with `sudo ./installer.sh --version`. The version must be
`2026.08.20` or newer before using the admin-password reset recovery path.

The build requires `make` and a C++17 compiler. Set `CXX` when the compiler is
not named `c++`, for example `CXX=g++ ./installer/build.sh`.

Run the already-built binary directly:

```sh
./installer/fsp-installer
```

The binary falls back to `installer/install.sh` when stdin is not a terminal. Command-line
arguments are passed through to the shell installer, so `--answers`, `--resume`,
`--check`, and `--dry-run` retain their existing behavior. When `--answers FILE` is
supplied, the frontend rejects missing files, duplicate keys, unknown keys, and malformed
lines before starting the shell installer. Secret values remain references such as
`env:NAME` or `file:/absolute/path`; the frontend never reads or prints their contents.

The interactive menu exposes the same common actions: start the C++ setup wizard,
resume setup, preview the setup with a dry run, run read-only checks, or open the
line-based installer fallback. It also provides **Reset Manager admin password**.
The reset generates a new password, stores it in the configured Key Vault or
protected environment file, and never prints the value. The operator can restart
the service immediately or restart it later.
If the deployment predates installer checkpoints, reset mode recovers the backend
and username from the existing systemd environment file instead of requiring a
new setup run.
Actions can be selected with the arrow keys or numeric shortcuts `1` through `6`.

This is the first migration slice for point 10. Further provisioning steps must keep the
shell parity tests passing before moving their implementation into C++.
