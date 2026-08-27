from __future__ import annotations

import base64
import json

import httpx
import pytest
from fastapi import FastAPI

import configbuilder.router as backup_router
from configbuilder.router import router
from security.backup import BackupError, create_backup, restore_backup
from security.credential_store import CredentialStore


class _SourceCipher:
    name = "test"

    def encrypt(self, plaintext: bytes) -> bytes:
        return bytes(value ^ 0x5A for value in plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return bytes(value ^ 0x5A for value in ciphertext)


class _DestinationCipher:
    name = "test"

    def encrypt(self, plaintext: bytes) -> bytes:
        return bytes(value ^ 0xA5 for value in plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return bytes(value ^ 0xA5 for value in ciphertext)


def test_backup_restores_config_credentials_and_mirror_state(tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir()
    (source_root / "config.system.json").write_text(
        json.dumps({"system": {"port": 9000}}), encoding="utf-8"
    )
    (source_root / "config.tables.json").write_text(
        json.dumps({"tables": [{"name": "orders"}]}), encoding="utf-8"
    )
    source_state = source_root / ".open_mirror_state"
    source_state.mkdir()
    (source_state / "orders.json").write_text('{"cursor":42}', encoding="utf-8")
    source_store = CredentialStore(str(source_root / "credentials.json"), cipher=_SourceCipher())
    source_store.set_url("warehouse", "postgresql://user:password@host/db")
    source_store.set_secret("mount/blob", {"account_key": "secret"})
    source_store.set_access_key("FSPTEST", {"secret_key": "key-secret", "buckets": ["data"]})

    archive, created = create_backup(
        "correct horse battery staple",
        root=source_root,
        store=source_store,
        mirror_state_dir=source_state,
    )
    assert created == {
        "created_at": created["created_at"],
        "config_files": 2,
        "connections": 1,
        "secrets": 1,
        "access_keys": 1,
        "mirror_state_files": 1,
    }

    destination_root = tmp_path / "destination"
    destination_root.mkdir()
    (destination_root / "config.connection.json").write_text("{}", encoding="utf-8")
    destination_store = CredentialStore(
        str(destination_root / "credentials.json"), cipher=_DestinationCipher()
    )
    destination_store.set_url("stale", "sqlite:///stale.db")
    result = restore_backup(
        archive,
        "correct horse battery staple",
        root=destination_root,
        store=destination_store,
        mirror_state_dir=destination_root / ".open_mirror_state",
    )

    assert result["restart_required"] is True
    assert json.loads((destination_root / "config.system.json").read_text())["system"]["port"] == 9000
    assert not (destination_root / "config.connection.json").exists()
    assert destination_store.list_ids() == ["warehouse"]
    assert destination_store.get_url("warehouse") == "postgresql://user:password@host/db"
    assert destination_store.get_secret("mount/blob") == {"account_key": "secret"}
    assert destination_store.get_access_key("FSPTEST") == {
        "secret_key": "key-secret",
        "buckets": ["data"],
    }
    assert (destination_root / ".open_mirror_state" / "orders.json").read_text() == '{"cursor":42}'


@pytest.mark.parametrize("mode", ["wrong-password", "tampered"])
def test_failed_decryption_does_not_modify_destination(tmp_path, mode):
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.system.json").write_text('{"system":{"port":9000}}')
    source_store = CredentialStore(str(source / "credentials.json"), cipher=_SourceCipher())
    archive, _ = create_backup("correct horse battery staple", root=source, store=source_store)

    destination = tmp_path / "destination"
    destination.mkdir()
    config_path = destination / "config.system.json"
    config_path.write_text('{"system":{"port":8000}}')
    destination_store = CredentialStore(
        str(destination / "credentials.json"), cipher=_DestinationCipher()
    )
    destination_store.set_url("existing", "sqlite:///existing.db")
    attempted_archive = archive
    password = "incorrect password value"
    if mode == "tampered":
        attempted_archive = archive[:-2] + (b"A}" if archive[-2:] != b"A}" else b"B}")
        password = "correct horse battery staple"

    with pytest.raises(BackupError, match="wrong password or damaged"):
        restore_backup(
            attempted_archive,
            password,
            root=destination,
            store=destination_store,
        )

    assert config_path.read_text() == '{"system":{"port":8000}}'
    assert destination_store.get_url("existing") == "sqlite:///existing.db"


def test_malformed_archive_is_rejected(tmp_path):
    store = CredentialStore(str(tmp_path / "credentials.json"), cipher=_SourceCipher())
    with pytest.raises(BackupError, match="envelope must be an object"):
        restore_backup(b"[]", "correct horse battery staple", root=tmp_path, store=store)


def test_encrypted_mirror_state_is_reencrypted_and_paths_are_rewritten(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "config.system.json").write_text(
        '{"system":{"open_mirror_encrypt_state":true}}', encoding="utf-8"
    )
    source_store = CredentialStore(str(source / "credentials.json"), cipher=_SourceCipher())
    sensitive = {
        "committed": None,
        "pending": {"payload_path": "C:/old/state/orders.json.pending.parquet"},
        "keys": {"1": {"h": "abc"}},
    }
    source_state = source / ".open_mirror_state"
    source_state.mkdir()
    (source_state / "orders.json.pending.parquet").write_bytes(b"pending rows")
    (source_state / "orders.json").write_text(json.dumps({
        "version": 2,
        "strategy": "snapshot",
        "initialized": True,
        "encrypted_sensitive_state": {
            "version": 1,
            "ciphertext": base64.b64encode(source_store.encrypt_blob(
                json.dumps(sensitive).encode("utf-8")
            )).decode("ascii"),
        },
    }), encoding="utf-8")
    archive, _ = create_backup(
        "correct horse battery staple",
        root=source,
        store=source_store,
        mirror_state_dir=source_state,
    )

    destination = tmp_path / "destination"
    destination.mkdir()
    destination_state = destination / ".open_mirror_state"
    destination_state.mkdir()
    (destination_state / "stale.json").write_text("{}", encoding="utf-8")
    destination_store = CredentialStore(
        str(destination / "credentials.json"), cipher=_DestinationCipher()
    )
    restore_backup(
        archive,
        "correct horse battery staple",
        root=destination,
        store=destination_store,
        mirror_state_dir=destination_state,
    )

    restored = json.loads((destination_state / "orders.json").read_text(encoding="utf-8"))
    encrypted = base64.b64decode(restored["encrypted_sensitive_state"]["ciphertext"])
    restored_sensitive = json.loads(destination_store.decrypt_blob(encrypted))
    assert restored_sensitive["keys"] == sensitive["keys"]
    assert restored_sensitive["pending"]["payload_path"] == str(
        (destination_state / "orders.json.pending.parquet").resolve()
    )
    assert (destination_state / "orders.json.pending.parquet").read_bytes() == b"pending rows"
    assert not (destination_state / "stale.json").exists()


@pytest.mark.asyncio
async def test_backup_and_restore_api_contract(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mirror_state = tmp_path / ".open_mirror_state"
    monkeypatch.setattr(backup_router.config, "OPEN_MIRROR_STATE_DIR", str(mirror_state))
    store = CredentialStore(str(tmp_path / "credentials.json"), cipher=_SourceCipher())
    monkeypatch.setattr(backup_router, "_store", lambda: store)
    (tmp_path / "config.system.json").write_text('{"system":{"port":9000}}')
    app = FastAPI()
    app.include_router(router)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        backup_response = await client.post(
            "/_config/api/backup", json={"password": "correct horse battery staple"}
        )
        assert backup_response.status_code == 200
        assert backup_response.headers["content-type"] == "application/vnd.fsp.backup"
        assert ".fspbackup" in backup_response.headers["content-disposition"]

        (tmp_path / "config.system.json").write_text('{"system":{"port":8000}}')
        restore_response = await client.post(
            "/_config/api/restore",
            data={"password": "correct horse battery staple"},
            files={"backup": ("test.fspbackup", backup_response.content, "application/vnd.fsp.backup")},
        )

    assert restore_response.status_code == 200
    assert restore_response.json()["restart_required"] is True
    assert json.loads((tmp_path / "config.system.json").read_text())["system"]["port"] == 9000