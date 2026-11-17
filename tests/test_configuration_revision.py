import json
import os
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.configbuilder import router as router_module
from fabric_shortcut_proxy.configbuilder.revision import (
    check_revision,
    configuration_revision,
    configuration_transaction,
)


@pytest.fixture
def configuration_app(tmp_path, monkeypatch):
    monkeypatch.setenv("FSP_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("FSP_AUTHZ_ENFORCE", "0")
    monkeypatch.delenv("NUM_SPLITS", raising=False)
    monkeypatch.setattr(router_module, "_BUILDER_TABLES_OVERRIDE", None)
    app = FastAPI()
    app.include_router(router_module.router)
    return app


@pytest.mark.asyncio
async def test_current_preserves_native_shape_and_emits_stable_revision(configuration_app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=configuration_app), base_url="http://test",
    ) as client:
        first = await client.get("/_config/api/current")
        second = await client.get("/_config/api/current")

    assert first.status_code == 200
    assert isinstance(first.json()["settings"], list)
    assert first.headers["etag"] == second.headers["etag"]
    assert first.headers["etag"].startswith('"')
    assert first.headers["cache-control"] == "no-store"
    assert all(not setting["secret"] or setting["value"] in ("", "***set***")
               for setting in first.json()["settings"])


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["save", "apply"])
async def test_stale_write_rejected_before_live_or_persisted_changes(
    configuration_app, tmp_path, monkeypatch, route,
):
    live_calls = []
    monkeypatch.setattr(config, "apply_live_settings", lambda updates: live_calls.append(updates))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=configuration_app), base_url="http://test",
    ) as client:
        initial = await client.get("/_config/api/current")
        saved = await client.post(
            "/_config/api/save",
            json={"settings": {"num_splits": 7}},
            headers={"If-Match": initial.headers["etag"]},
        )
        assert saved.status_code == 200
        before = (tmp_path / "config.performance.json").read_bytes()
        rejected = await client.post(
            f"/_config/api/{route}",
            json={"settings": {"num_splits": 9}},
            headers={
                "If-Match": initial.headers["etag"],
                "X-Correlation-ID": "revision-test",
            },
        )
        after = await client.get("/_config/api/current")

    assert rejected.status_code == 409
    assert rejected.headers["content-type"] == "application/problem+json"
    assert rejected.json()["code"] == "stale_revision"
    assert rejected.json()["correlationId"] == rejected.headers["x-correlation-id"]
    assert rejected.headers["etag"] == saved.headers["etag"] == after.headers["etag"]
    assert (tmp_path / "config.performance.json").read_bytes() == before
    assert live_calls == []


@pytest.mark.asyncio
async def test_read_after_write_and_legacy_callers(configuration_app):
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=configuration_app), base_url="http://test",
    ) as client:
        initial = await client.get("/_config/api/current")
        legacy = await client.post("/_config/api/save", json={"settings": {"num_splits": 8}})
        current = await client.get("/_config/api/current")
        matched = await client.post(
            "/_config/api/save", json={"settings": {"num_splits": 9}},
            headers={"If-Match": current.headers["etag"]},
        )
        final = await client.get("/_config/api/current")

    assert legacy.status_code == matched.status_code == 200
    assert initial.headers["etag"] != legacy.headers["etag"] == current.headers["etag"]
    assert matched.headers["etag"] == final.headers["etag"] != current.headers["etag"]
    assert next(s["value"] for s in final.json()["settings"] if s["key"] == "num_splits") == 9


@pytest.mark.asyncio
async def test_apply_changes_revision_and_keeps_native_response(configuration_app, monkeypatch):
    monkeypatch.setattr(config, "apply_live_settings", lambda _: {
        "applied": [], "restart_required": ["tables"],
    })
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=configuration_app), base_url="http://test",
    ) as client:
        initial = await client.get("/_config/api/current")
        result = await client.post(
            "/_config/api/apply", json={"settings": {"tables": []}},
            headers={"If-Match": initial.headers["etag"]},
        )
        final = await client.get("/_config/api/current")

    assert result.status_code == 200
    assert result.json()["ok"] is True
    assert result.json()["restart_required"] == ["tables"]
    assert result.headers["etag"] == final.headers["etag"] != initial.headers["etag"]


@pytest.mark.asyncio
async def test_secret_changes_revision_without_disclosing_value(
    configuration_app, monkeypatch, caplog,
):
    env = config._SETTINGS_REGISTRY["secret_access_key"]["env"]
    secret = "revision-test-not-a-real-secret"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=configuration_app), base_url="http://test",
    ) as client:
        initial = await client.get("/_config/api/current")
        monkeypatch.setenv(env, secret)
        current = await client.get("/_config/api/current")
        stale = await client.post(
            "/_config/api/save", json={"settings": {"num_splits": 9}},
            headers={"If-Match": initial.headers["etag"]},
        )

    assert initial.headers["etag"] != current.headers["etag"]
    assert stale.status_code == 409
    assert secret not in current.text + stale.text + str(current.headers) + caplog.text
    setting = next(s for s in current.json()["settings"] if s["key"] == "secret_access_key")
    assert setting["value"] == "***set***"


def test_concurrent_compare_and_write_has_one_winner(configuration_app, tmp_path):
    with configuration_transaction():
        initial = configuration_revision()
    barrier = threading.Barrier(2)

    def write(value):
        barrier.wait(timeout=5)
        with configuration_transaction():
            failure = check_revision(initial, "concurrent-test")
            if failure is not None:
                return failure.status_code
            config.write_config_updates({"num_splits": value})
            return 200

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(write, [7, 8]))

    assert sorted(results) == [200, 409]
    saved = json.loads((tmp_path / "config.performance.json").read_text())
    assert saved["performance"]["num_splits"] in [7, 8]


@pytest.mark.parametrize("if_match", ["*", 'W/"weak"', "invalid", ""])
def test_only_exact_strong_revision_can_match(configuration_app, if_match):
    with configuration_transaction():
        rejected = check_revision(if_match, "")
    assert rejected is not None
    assert rejected.status_code == 409


def test_other_process_cannot_write_until_transaction_releases(configuration_app, tmp_path):
    script = """
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.configbuilder.revision import configuration_transaction
print("ready", flush=True)
with configuration_transaction():
    config.write_config_updates({"num_splits": 11})
"""
    env = {**os.environ, "PYTHONPATH": str(Path(config.__file__).parent.parent)}
    with configuration_transaction():
        initial = configuration_revision()
        child = subprocess.Popen(
            [sys.executable, "-c", script], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, env=env,
        )
        try:
            assert child.stdout.readline().strip() == "ready"
            with pytest.raises(subprocess.TimeoutExpired):
                child.communicate(timeout=0.3)
            assert not (tmp_path / "config.performance.json").exists()
        except BaseException:
            child.kill()
            child.communicate()
            raise
    try:
        _, stderr = child.communicate(timeout=15)
        assert child.returncode == 0, stderr
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    with configuration_transaction():
        assert check_revision(initial, "process-test").status_code == 409
    saved = json.loads((tmp_path / "config.performance.json").read_text())
    assert saved["performance"]["num_splits"] == 11


def test_simultaneous_process_writes_have_one_winner(configuration_app):
    script = """
import sys
from fabric_shortcut_proxy import config
from fabric_shortcut_proxy.configbuilder.revision import (
    configuration_transaction, check_revision,
)
print("ready", flush=True)
sys.stdin.readline()
with configuration_transaction():
    rejected = check_revision(sys.argv[1], "process-race")
    if rejected is None:
        config.write_config_updates({"num_splits": int(sys.argv[2])})
    print(409 if rejected is not None else 200, flush=True)
"""
    env = {**os.environ, "PYTHONPATH": str(Path(config.__file__).parent.parent)}
    with configuration_transaction():
        initial = configuration_revision()
    children = []
    try:
        for value in [12, 13]:
            children.append(subprocess.Popen(
                [sys.executable, "-c", script, initial, str(value)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, env=env,
            ))
        for child in children:
            assert child.stdout.readline().strip() == "ready"
        for child in children:
            child.stdin.write("\n")
            child.stdin.flush()
        results = []
        for child in children:
            stdout, stderr = child.communicate(timeout=15)
            assert child.returncode == 0, stderr
            results.append(int(stdout.splitlines()[-1]))
        assert sorted(results) == [200, 409]
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.communicate()


def test_revision_changes_on_external_saved_state_edit(configuration_app, tmp_path):
    path = tmp_path / "config.tables.json"
    path.write_text('{"tables": []}')
    with configuration_transaction():
        initial = configuration_revision()
    path.write_text('{"tables": [{"name": "new_table"}]}')
    with configuration_transaction():
        assert check_revision(initial, "external-edit").status_code == 409


def test_revision_headers_are_documented(configuration_app):
    paths = configuration_app.openapi()["paths"]
    assert "ETag" in paths["/_config/api/current"]["get"]["responses"]["200"]["headers"]
    for route in ["save", "apply"]:
        operation = paths[f"/_config/api/{route}"]["post"]
        assert any(p["name"] == "If-Match" for p in operation["parameters"])
        assert "application/problem+json" in operation["responses"]["409"]["content"]
