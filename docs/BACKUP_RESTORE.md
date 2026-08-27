# Encrypted Backup and Restore

Version 2.6.0 adds portable, password-protected backup and restore to the Config Builder.
Open `/_config`, select **Security**, and use the **Backup and restore** controls.

## Backup scope

Each `.fspbackup` archive contains the FSP-managed state needed to rebuild a deployment:

- `config.system.json`, `config.connection.json`, `config.performance.json`,
  `config.freshness.json`, `config.tables.json`, `config.mounts.json`, and
  `config.open_mirror.json`, when present;
- logical connection and generic-secret records from the encrypted credential store;
- scoped S3 access keys and their authorization settings;
- Open Mirroring cursor, pending-file, key, and recovery state from
  `OPEN_MIRROR_STATE_DIR`.

The archive does not include source database rows, OneLake data, generated Parquet or metadata
artifacts, memory or disk caches, logs, environment-only secrets, external TLS certificate or
key files, or secret values held only in a remote Key Vault. Protect and recover those systems
separately.

## Protection and portability

Enter a unique password containing at least 12 characters. The backup derives a 256-bit key with
scrypt and encrypts and authenticates the complete payload with AES-256-GCM. A modified archive
or incorrect password is rejected before any destination data is changed. Archives are limited
to 512 MiB.

Windows DPAPI ciphertext and non-Windows Fernet ciphertext are host-specific. The backup exports
logical decrypted records into the protected archive instead of copying that ciphertext. Restore
encrypts each record with the destination host's credential store. Open Mirroring sensitive state
is handled the same way, and pending sidecar paths are rewritten for the destination state
directory.

## Create a backup

1. Open the Config Builder on the Manager control plane, normally
   `http://localhost:9200/_config/`.
2. Select **Security**, then **Backup and restore**.
3. Enter and confirm a unique backup password.
4. Download the generated `.fspbackup` file and record the displayed item counts.
5. Store the archive and password separately in approved protected locations.

Creating an archive does not stop the Manager. For the most consistent Open Mirroring recovery
point, avoid starting a publish while the backup is being created.

## Restore a backup

1. Take a fresh backup of the destination before restoring.
2. Stop scheduled or manual Open Mirroring publishes on the destination.
3. Open **Security → Backup and restore**, select the `.fspbackup` file, and enter its password.
4. Review the restore summary. Wrong passwords, damaged archives, unsupported config files, and
   unsafe state paths fail before replacement begins.
5. Restart the Manager. The running process does not reload every restored setting immediately.
6. Verify sources, table mappings, scoped access keys, and Open Mirroring status before resuming
   normal publishing.

Restore replaces the supported split config set, credential-store records, and Open Mirroring
state as one operation. If a write fails, the previous config, credentials, and state are restored.
Files from the supported config set that are absent from the archive are removed so the
destination matches the backup.

## HTTP API

The browser uses these Manager-authenticated endpoints:

| Endpoint | Request | Result |
|---|---|---|
| `POST /_config/api/backup` | JSON body `{"password":"..."}` | `.fspbackup` response with `Cache-Control: no-store` |
| `POST /_config/api/restore` | Multipart fields `password` and `backup` | Restore counts and `restart_required: true` |

Keep passwords in request bodies. Do not place them in URLs, shell history, logs, or automation
output. Apply the same Manager Basic authentication and network restrictions used for the rest
of `/_config`.