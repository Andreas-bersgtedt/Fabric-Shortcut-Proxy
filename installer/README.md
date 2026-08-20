# C++ installer

The C++ frontend provides SSH-safe arrow-key navigation. It does not duplicate
the provisioning logic yet. Selecting a setup action delegates to the root
`installer/install.sh`, which remains the source of truth for checkpointing, secret
handling, TLS validation, and systemd setup.

The root dispatcher always rebuilds the C++ binary before launching it. A failed
build stops the installer instead of running a stale binary. Build directly on Linux:

```sh
./installer/build.sh
```

Run the already-built binary directly:

```sh
./installer/fsp-installer
```

The binary falls back to `installer/install.sh` when stdin is not a terminal. Command-line
arguments are passed through to the shell installer, so `--answers`, `--resume`,
`--check`, and `--dry-run` retain their existing behavior.

The interactive menu exposes the same common actions: start or resume setup, preview
the setup with a dry run, run read-only checks, or open the line-based installer.
Actions can be selected with the arrow keys or numeric shortcuts `1` through `6`.
