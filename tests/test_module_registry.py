import json
import platform

import pytest

import module_registry


def test_required_modules_from_config(monkeypatch, tmp_path):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    (tmp_path / "config.connection.json").write_text(
        json.dumps({"connections": [{"id": "pg", "db_url": "postgresql+asyncpg://db"}]}),
        encoding="utf-8",
    )
    (tmp_path / "config.mounts.json").write_text(
        json.dumps({"mounts": [{"bucket": "files", "backend": "azure"}]}),
        encoding="utf-8",
    )
    assert module_registry.required_modules()["postgres"] == "configured PostgreSQL source"
    assert module_registry.required_modules()["azureblob"] == "configured Azure mount"
    if platform.system().lower() in ("linux", "darwin"):
        assert "credentials" in module_registry.required_modules()


def test_module_plan_blocks_required_disable(monkeypatch, tmp_path):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    (tmp_path / "config.connection.json").write_text(
        json.dumps({"connections": [{"id": "pg", "db_url": "postgresql+asyncpg://db"}]}),
        encoding="utf-8",
    )
    plan = module_registry.module_plan([])
    expected = ["postgres"]
    if platform.system().lower() in ("linux", "darwin"):
        expected.insert(0, "credentials")
    assert plan["blocked"] == expected


def test_save_desired_profile_is_atomic_and_allowlisted(monkeypatch, tmp_path):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    result = module_registry.save_desired_profile(["postgres", "unknown", "postgres"])
    saved = json.loads((tmp_path / "config.modules.json").read_text(encoding="utf-8"))
    assert result["desired"] == ["postgres"]
    assert saved == {"schema_version": 1, "modules": {"desired": ["postgres"]}}


def test_status_reports_installed_and_active(monkeypatch):
    monkeypatch.setattr(module_registry.importlib.metadata, "version", lambda package: "1.0")
    monkeypatch.setattr(module_registry.importlib.util, "find_spec", lambda name: object())
    status = module_registry.module_status()
    postgres = next(row for row in status["modules"] if row["id"] == "postgres")
    assert postgres["installed"] is True
    assert postgres["active"] is True
    assert postgres["restart_required"] is True


@pytest.mark.asyncio
async def test_install_uses_allowlisted_profile(monkeypatch, tmp_path):
    import module_installer

    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    calls = []

    class FakeProcess:
        returncode = 0

        async def communicate(self):
            return b"installed", None

    async def fake_create(*command, **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(module_installer.asyncio, "create_subprocess_exec", fake_create)
    module_installer._TASK = None
    accepted = module_installer.start_install(["postgres", "not-a-package"])
    await module_installer._TASK

    assert accepted["desired"] == ["postgres"]
    assert module_installer.status()["status"] == "succeeded"
    command = calls[0][0]
    assert ".[postgres]" in command
    assert "not-a-package" not in command
