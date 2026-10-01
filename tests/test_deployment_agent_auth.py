from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
K8S_BASE = ROOT / "deploy" / "kubernetes" / "base"
KUBECTL = shutil.which("kubectl")


def _read(name: str) -> str:
    return (K8S_BASE / name).read_text(encoding="utf-8")


def test_kubernetes_workloads_read_active_token_from_secret() -> None:
    for manifest in (
        _read("manager-deployment.yaml"),
        _read("python-statefulset.yaml"),
        _read("cpp-deployment.yaml"),
    ):
        assert "name: AGENT_TOKEN" in manifest
        assert "name: fsp-agent-auth" in manifest
        assert "key: AGENT_TOKEN" in manifest


def test_rotation_material_is_manager_only() -> None:
    manager = _read("manager-deployment.yaml")
    agents = _read("python-statefulset.yaml") + _read("cpp-deployment.yaml")

    assert "name: AGENT_AUTH_MODE\n              value: required" in manager
    assert "AGENT_TOKEN_PREVIOUS" in manager
    assert "AGENT_TOKEN_PREVIOUS_VALID_UNTIL" in manager
    assert "AGENT_TOKEN_PREVIOUS" not in agents
    assert "AGENT_TOKEN_PREVIOUS_VALID_UNTIL" not in agents


def test_agent_workloads_do_not_receive_manager_basic_credentials() -> None:
    agents = _read("python-statefulset.yaml") + _read("cpp-deployment.yaml")

    assert "MANAGER_AUTH_USERNAME" not in agents
    assert "MANAGER_AUTH_PASSWORD" not in agents
    assert "replace-with-at-least-32-random-bytes" not in agents


def test_secret_examples_do_not_contain_agent_token_values() -> None:
    examples = ROOT / "deploy" / "kubernetes" / "examples"
    agent_auth = (examples / "agent-auth-secret.example.yaml").read_text(
        encoding="utf-8"
    )
    source = (examples / "source-secret.example.yaml").read_text(encoding="utf-8")

    assert "stringData: {}" in agent_auth
    assert "AGENT_TOKEN:" not in agent_auth
    assert "MANAGER_AUTH_USERNAME" not in source
    assert "MANAGER_AUTH_PASSWORD" not in source


@pytest.mark.skipif(KUBECTL is None, reason="kubectl is not installed")
@pytest.mark.parametrize("overlay", ["kind", "kind-tls"])
def test_kind_manager_basic_secret_is_manager_only(overlay: str) -> None:
    assert KUBECTL is not None
    result = subprocess.run(
        [KUBECTL, "kustomize", str(ROOT / "deploy" / "kubernetes" / "overlays" / overlay)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    documents = result.stdout.split("\n---\n")

    manager_secret = next(
        document
        for document in documents
        if "kind: Secret" in document and "name: fsp-manager-auth" in document
    )
    manager = next(
        document
        for document in documents
        if "kind: Deployment" in document and "\n  name: fsp-manager\n" in document
    )
    materializer = next(
        document
        for document in documents
        if "kind: StatefulSet" in document
        and "\n  name: fsp-materializer\n" in document
    )
    cpp_agent = next(
        document
        for document in documents
        if "kind: Deployment" in document
        and "\n  name: fsp-cpp-agent\n" in document
    )

    assert "MANAGER_AUTH_ENABLED:" in manager_secret
    assert "MANAGER_AUTH_USERNAME:" in manager_secret
    assert "MANAGER_AUTH_PASSWORD:" in manager_secret
    assert "name: fsp-manager-auth" in manager
    assert "fsp-manager-auth" not in materializer
    assert "fsp-manager-auth" not in cpp_agent
