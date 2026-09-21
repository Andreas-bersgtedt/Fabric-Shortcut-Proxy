from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy" / "helm" / "fabric-shortcut-proxy"
EXAMPLE_VALUES = CHART / "values-enterprise-demo.example.yaml"
HELM = shutil.which("helm")


pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is not installed")


def _helm(*arguments: str, expect_success: bool = True) -> subprocess.CompletedProcess[str]:
    assert HELM is not None
    result = subprocess.run(
        [HELM, *arguments],
        cwd=ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )
    if expect_success:
        assert result.returncode == 0, result.stdout + result.stderr
    return result


def _render(*extra_arguments: str) -> str:
    return _helm(
        "template",
        "fsp",
        str(CHART),
        "--namespace",
        "fabric-shortcut-proxy",
        *extra_arguments,
    ).stdout


def _kind_counts(rendered: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for kind in re.findall(r"^kind:\s+(\S+)\s*$", rendered, re.MULTILINE):
        counts[kind] = counts.get(kind, 0) + 1
    return counts


def test_chart_lints_with_enterprise_example() -> None:
    _helm("lint", str(CHART), "-f", str(EXAMPLE_VALUES))


def test_base_render_contains_core_fleet_only() -> None:
    counts = _kind_counts(_render())

    assert sum(counts.values()) == 12
    assert counts["Deployment"] == 2
    assert counts["StatefulSet"] == 1
    assert counts["Service"] == 3
    assert "Ingress" not in counts
    assert "PersistentVolume" not in counts


def test_enterprise_render_contains_complete_demo() -> None:
    rendered = _render("-f", str(EXAMPLE_VALUES))
    counts = _kind_counts(rendered)

    assert sum(counts.values()) == 27
    assert counts["Deployment"] == 3
    assert counts["Service"] == 6
    assert counts["PersistentVolume"] == 2
    assert counts["Ingress"] == 2
    assert counts["Certificate"] == 1
    assert counts["ClusterIssuer"] == 1
    assert 'S3_BUCKET: "fsp-demo"' in rendered
    assert 'S3_BUCKET: "fabric-iceberg-poc"' not in rendered
    assert "TOKENIZATION_POLICY_FILE: /config/config.tokenization.json" in rendered


@pytest.mark.parametrize(
    ("nginx_enabled", "tls_enabled"),
    [(True, False), (False, True)],
)
def test_nginx_and_tls_must_be_enabled_together(
    nginx_enabled: bool,
    tls_enabled: bool,
) -> None:
    result = _helm(
        "template",
        "fsp",
        str(CHART),
        "--set",
        f"nginx.enabled={str(nginx_enabled).lower()}",
        "--set",
        f"tls.enabled={str(tls_enabled).lower()}",
        expect_success=False,
    )

    assert result.returncode != 0
    assert "nginx.enabled and tls.enabled must be enabled or disabled together" in result.stderr


@pytest.mark.parametrize(
    ("subnet", "ip_address", "expected_message"),
    [
        ("", "10.240.4.100", "nginx.privateService.subnet is required"),
        ("snet-aks-app", "", "nginx.privateService.ipAddress is required"),
    ],
)
def test_enabled_private_service_requires_azure_network_values(
    subnet: str,
    ip_address: str,
    expected_message: str,
) -> None:
    result = _helm(
        "template",
        "fsp",
        str(CHART),
        "--set",
        "nginx.enabled=true",
        "--set",
        "tls.enabled=true",
        "--set",
        "tls.hostname=fsp.example.com",
        "--set",
        f"nginx.privateService.subnet={subnet}",
        "--set",
        f"nginx.privateService.ipAddress={ip_address}",
        expect_success=False,
    )

    assert result.returncode != 0
    assert expected_message in result.stderr
