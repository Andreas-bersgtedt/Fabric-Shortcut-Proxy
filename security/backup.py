"""Portable, password-encrypted backup and restore for FSP-managed state."""
from __future__ import annotations

import base64
import json
import os
import pathlib
import shutil
import tempfile
from datetime import datetime, timezone

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

from security.credential_store import CredentialStore

_FORMAT = "fabric-shortcut-proxy-backup"
_VERSION = 1
_AAD = b"fabric-shortcut-proxy-backup-v1"
_CONFIG_FILES = (
    "config.system.json",
    "config.connection.json",
    "config.performance.json",
    "config.freshness.json",
    "config.tables.json",
    "config.mounts.json",
    "config.open_mirror.json",
)
_MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


class BackupError(ValueError):
    pass


def _derive_key(password: str, salt: bytes) -> bytes:
    if len(password) < 12:
        raise BackupError("backup password must contain at least 12 characters")
    return Scrypt(salt=salt, length=32, n=2**15, r=8, p=1).derive(password.encode("utf-8"))


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(value: str, label: str) -> bytes:
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as exc:
        raise BackupError(f"invalid {label} encoding") from exc


def _read_json_file(path: pathlib.Path) -> bytes:
    raw = path.read_bytes()
    try:
        parsed = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise BackupError(f"{path.name} is not valid JSON") from exc
    if not isinstance(parsed, dict):
        raise BackupError(f"{path.name} must contain a JSON object")
    return raw


def _portable_mirror_state(raw: bytes, path: pathlib.Path, store: CredentialStore) -> bytes:
    if path.suffix.lower() != ".json":
        return raw
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise BackupError(f"Open Mirroring state file {path.name} is invalid") from exc
    envelope = data.pop("encrypted_sensitive_state", None)
    if envelope is not None:
        try:
            if not isinstance(envelope, dict) or envelope.get("version") != 1:
                raise ValueError("invalid encrypted state envelope")
            ciphertext = _unb64(envelope["ciphertext"], "mirror state ciphertext")
            sensitive = json.loads(store.decrypt_blob(ciphertext).decode("utf-8"))
        except Exception as exc:
            raise BackupError(f"could not decrypt Open Mirroring state file {path.name}") from exc
        if not isinstance(sensitive, dict):
            raise BackupError(f"Open Mirroring state file {path.name} has invalid encrypted data")
        data.update(sensitive)
    return json.dumps(data, separators=(",", ":")).encode("utf-8")


def _mirror_state_files(state_dir: pathlib.Path, store: CredentialStore) -> dict[str, str]:
    files: dict[str, str] = {}
    if not state_dir.is_dir():
        return files
    for path in sorted(state_dir.rglob("*")):
        if path.is_symlink() or not path.is_file():
            continue
        relative = path.relative_to(state_dir).as_posix()
        files[relative] = _b64(_portable_mirror_state(path.read_bytes(), path, store))
    return files


def create_backup(
    password: str,
    *,
    root: str | os.PathLike[str] = ".",
    store: CredentialStore | None = None,
    include_mirror_state: bool = True,
    mirror_state_dir: str | os.PathLike[str] = "./.open_mirror_state",
) -> tuple[bytes, dict]:
    root_path = pathlib.Path(root).resolve()
    credential_store = store or CredentialStore()
    if not credential_store.available:
        raise BackupError("credential store encryption is unavailable")

    configs = {
        name: _b64(_read_json_file(root_path / name))
        for name in _CONFIG_FILES if (root_path / name).is_file()
    }
    credentials = credential_store.export_records()
    mirror_files = (
        _mirror_state_files(pathlib.Path(mirror_state_dir).resolve(), credential_store)
        if include_mirror_state else {}
    )
    created_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload = {
        "format": _FORMAT,
        "version": _VERSION,
        "created_at": created_at,
        "configs": configs,
        "credentials": credentials,
        "open_mirror_state": mirror_files,
    }
    plaintext = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    salt, nonce = os.urandom(16), os.urandom(12)
    ciphertext = AESGCM(_derive_key(password, salt)).encrypt(nonce, plaintext, _AAD)
    envelope = {
        "format": _FORMAT,
        "version": _VERSION,
        "kdf": {"name": "scrypt", "n": 2**15, "r": 8, "p": 1, "salt": _b64(salt)},
        "cipher": {"name": "aes-256-gcm", "nonce": _b64(nonce)},
        "ciphertext": _b64(ciphertext),
    }
    archive = json.dumps(envelope, separators=(",", ":")).encode("utf-8")
    if len(archive) > _MAX_ARCHIVE_BYTES:
        raise BackupError("backup exceeds the 512 MiB limit")
    return archive, {
        "created_at": created_at,
        "config_files": len(configs),
        "connections": len(credentials["connections"]),
        "secrets": len(credentials["secrets"]),
        "access_keys": len(credentials["access_keys"]),
        "mirror_state_files": len(mirror_files),
    }


def _decrypt_archive(archive: bytes, password: str) -> dict:
    if not archive or len(archive) > _MAX_ARCHIVE_BYTES:
        raise BackupError("backup file is empty or exceeds the 512 MiB limit")
    try:
        envelope = json.loads(archive.decode("utf-8"))
        if not isinstance(envelope, dict):
            raise BackupError("backup envelope must be an object")
        if envelope.get("format") != _FORMAT or envelope.get("version") != _VERSION:
            raise BackupError("unsupported backup format or version")
        kdf = envelope["kdf"]
        cipher = envelope["cipher"]
        if kdf.get("name") != "scrypt" or cipher.get("name") != "aes-256-gcm":
            raise BackupError("unsupported backup encryption")
        salt = _unb64(kdf["salt"], "salt")
        nonce = _unb64(cipher["nonce"], "nonce")
        ciphertext = _unb64(envelope["ciphertext"], "ciphertext")
        plaintext = AESGCM(_derive_key(password, salt)).decrypt(nonce, ciphertext, _AAD)
        payload = json.loads(plaintext.decode("utf-8"))
    except BackupError:
        raise
    except (InvalidTag, KeyError, TypeError, ValueError, UnicodeDecodeError) as exc:
        raise BackupError("wrong password or damaged backup file") from exc
    if payload.get("format") != _FORMAT or payload.get("version") != _VERSION:
        raise BackupError("unsupported encrypted payload")
    for key in ("configs", "credentials", "open_mirror_state"):
        if not isinstance(payload.get(key), dict):
            raise BackupError(f"backup payload is missing {key}")
    return payload


def _validated_files(payload: dict) -> tuple[dict[str, bytes], dict[str, bytes]]:
    configs: dict[str, bytes] = {}
    for name, encoded in payload["configs"].items():
        if name not in _CONFIG_FILES:
            raise BackupError(f"backup contains unsupported config file {name!r}")
        raw = _unb64(encoded, name)
        try:
            parsed = json.loads(raw.decode("utf-8-sig"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise BackupError(f"backup contains invalid JSON in {name}") from exc
        if not isinstance(parsed, dict):
            raise BackupError(f"backup config {name} must contain an object")
        configs[name] = raw

    state_files: dict[str, bytes] = {}
    for relative, encoded in payload["open_mirror_state"].items():
        candidate = pathlib.PurePosixPath(relative)
        if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
            raise BackupError("backup contains an unsafe Open Mirroring state path")
        state_files[candidate.as_posix()] = _unb64(encoded, f"state file {relative}")
    return configs, state_files


def _atomic_write(path: pathlib.Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def _state_encryption_enabled(configs: dict[str, bytes]) -> bool:
    raw = configs.get("config.system.json")
    if raw is None:
        return False
    data = json.loads(raw.decode("utf-8-sig"))
    system = data.get("system", data)
    return bool(system.get("open_mirror_encrypt_state", False)) if isinstance(system, dict) else False


def _destination_mirror_state(
    raw: bytes,
    relative: str,
    state_root: pathlib.Path,
    store: CredentialStore,
    encrypt: bool,
) -> bytes:
    if not relative.lower().endswith(".json"):
        return raw
    data = json.loads(raw.decode("utf-8"))
    pending = data.get("pending")
    sidecar_relative = f"{relative}.pending.parquet"
    if isinstance(pending, dict) and pending.get("payload_path"):
        pending["payload_path"] = str((state_root / pathlib.PurePosixPath(sidecar_relative)).resolve())
    if encrypt:
        sensitive = {
            key: data.pop(key)
            for key in ("committed", "pending", "keys")
            if key in data
        }
        ciphertext = store.encrypt_blob(
            json.dumps(sensitive, separators=(",", ":")).encode("utf-8")
        )
        data["encrypted_sensitive_state"] = {
            "version": 1,
            "ciphertext": _b64(ciphertext),
        }
    return json.dumps(data, separators=(",", ":")).encode("utf-8")


def restore_backup(
    archive: bytes,
    password: str,
    *,
    root: str | os.PathLike[str] = ".",
    store: CredentialStore | None = None,
    mirror_state_dir: str | os.PathLike[str] = "./.open_mirror_state",
) -> dict:
    payload = _decrypt_archive(archive, password)
    configs, state_files = _validated_files(payload)
    credential_store = store or CredentialStore()
    if not credential_store.available:
        raise BackupError("credential store encryption is unavailable")
    credential_store.validate_records(payload["credentials"])

    root_path = pathlib.Path(root).resolve()
    state_root = pathlib.Path(mirror_state_dir).resolve()
    state_root.parent.mkdir(parents=True, exist_ok=True)
    staged_state = pathlib.Path(tempfile.mkdtemp(prefix=f".{state_root.name}.restore-", dir=state_root.parent))
    encrypt_state = _state_encryption_enabled(configs)
    for relative, content in state_files.items():
        restored_content = _destination_mirror_state(
            content, relative, state_root, credential_store, encrypt_state
        )
        _atomic_write(staged_state / pathlib.PurePosixPath(relative), restored_content)
    previous_configs = {
        name: (root_path / name).read_bytes() if (root_path / name).is_file() else None
        for name in _CONFIG_FILES
    }
    previous_credentials = credential_store.export_records()
    previous_state = state_root.with_name(f".{state_root.name}.pre-restore-{os.urandom(6).hex()}")
    state_swapped = False
    state_existed = state_root.exists()
    try:
        credential_store.replace_records(payload["credentials"])
        for name in _CONFIG_FILES:
            path = root_path / name
            if name in configs:
                _atomic_write(path, configs[name])
            elif path.exists():
                path.unlink()
        if state_existed:
            os.replace(state_root, previous_state)
        os.replace(staged_state, state_root)
        state_swapped = True
    except Exception:
        credential_store.replace_records(previous_credentials)
        for name, content in previous_configs.items():
            path = root_path / name
            if content is None:
                if path.exists():
                    path.unlink()
            else:
                _atomic_write(path, content)
        if state_swapped and state_root.exists():
            shutil.rmtree(state_root)
        if previous_state.exists():
            os.replace(previous_state, state_root)
        elif state_swapped and not state_existed:
            shutil.rmtree(state_root, ignore_errors=True)
        raise
    finally:
        shutil.rmtree(staged_state, ignore_errors=True)
    if previous_state.exists():
        shutil.rmtree(previous_state)

    credentials = payload["credentials"]
    return {
        "created_at": payload.get("created_at"),
        "config_files": len(configs),
        "connections": len(credentials["connections"]),
        "secrets": len(credentials["secrets"]),
        "access_keys": len(credentials["access_keys"]),
        "mirror_state_files": len(state_files),
        "restart_required": True,
    }