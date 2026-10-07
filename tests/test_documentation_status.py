from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

from fabric_shortcut_proxy.db.capabilities import capability_matrix


ROOT = Path(__file__).parents[1]


def test_unreleased_changelog_starts_at_package_release():
    package = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    version = package["project"]["version"]
    changelog = (ROOT / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")

    assert (
        f"[Unreleased]: "
        "https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy/"
        f"compare/{version}...HEAD"
    ) in changelog
    assert f"[{version}]:" in changelog
    manual = (ROOT / "docs" / "manual" / "README.md").read_text(
        encoding="utf-8"
    )
    assert f"Version {version}" in manual
    chart = (
        ROOT
        / "deploy"
        / "helm"
        / "fabric-shortcut-proxy"
        / "Chart.yaml"
    ).read_text(encoding="utf-8")
    assert f"version: {version}" in chart
    assert f'appVersion: "{version}"' in chart
    enterprise = tomllib.loads(
        (ROOT / "enterprise" / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert enterprise["project"]["version"] == version
    assert f"fabric-shortcut-proxy=={version}" in enterprise["project"][
        "dependencies"
    ]
    enterprise_readme = (
        ROOT / "enterprise" / "README.md"
    ).read_text(encoding="utf-8")
    assert f"Enterprise {version} requires" in enterprise_readme
    expected_runtime_versions = {
        "src/fabric_shortcut_proxy/main.py": f'version="{version}"',
        "enterprise/agent_link.py": f'_APP_VERSION = "{version}"',
        "enterprise/control/admin.py": f'return "{version}"',
        "enterprise/control/manager_app.py": f'version="{version}"',
        "src/fabric_shortcut_proxy/configbuilder/router.py": f'return "{version}"',
    }
    for relative, expected in expected_runtime_versions.items():
        assert expected in (ROOT / relative).read_text(encoding="utf-8")


def test_major_release_compatibility_contract_is_documented():
    package = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    version = package["project"]["version"]
    if not version.startswith("3."):
        return

    upgrade = (ROOT / "docs" / "UPGRADE_3_0.md").read_text(encoding="utf-8")
    changelog = (ROOT / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")
    normalized_upgrade = " ".join(upgrade.split())
    assert (
        "does not introduce a storage-format or configuration-schema migration"
        in normalized_upgrade
    )
    assert f"fabric-shortcut-proxy=={version}" in upgrade
    assert "C++ serving Agent 1.0.0" in upgrade
    assert "Databricks, Redshift, and Teradata remain beta" in upgrade
    assert "Impala 3.4" in upgrade
    assert "Issue #94 remains open" in changelog


def test_current_diagrams_do_not_hard_code_fsp_release_version():
    stale_pattern = re.compile(
        r"\b(?:FSP|fsp release)\s+\d+\.\d+\.\d+\b"
    )
    for path in (ROOT / "docs").glob("*.excalidraw"):
        diagram = json.loads(path.read_text(encoding="utf-8"))
        for element in diagram.get("elements", []):
            if element.get("type") != "text":
                continue
            assert int(element.get("width", 0)) > 0, path
            assert int(element.get("height", 0)) > 0, path
            assert not stale_pattern.search(str(element.get("text", ""))), path
    for relative in (
        "docs/TechnicalArchitecture.md",
        "docs/manual/03-architecture.md",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "2.9.3" not in text, relative


def test_current_status_docs_match_delivered_features_and_issue_links():
    faq = (ROOT / "FAQ.md").read_text(encoding="utf-8")
    assert "row-target split sizing and range/date/auto planning" in faq
    assert "In progress / planned:** split" not in faq
    for issue in range(89, 97):
        assert (
            "https://github.com/Andreas-bersgtedt/"
            f"Fabric-Shortcut-Proxy/issues/{issue}"
        ) in faq

    mounts = (
        ROOT / "src" / "fabric_shortcut_proxy" / "storage" / "mounts.py"
    ).read_text(encoding="utf-8")
    assert "local, S3-compatible, and Azure" in mounts
    assert "supports the ``local`` backend only" not in mounts

    control = (
        ROOT / "enterprise" / "control" / "server.py"
    ).read_text(encoding="utf-8")
    assert "materialization work‑queue are stubs" not in control
    assert "#90 work queue" in control
    assert "#96 generation-membership" in control

    for flavor in ("oracle", "databricks", "redshift", "teradata", "impala"):
        row = next(
            line for line in faq.splitlines()
            if line.startswith(
                {
                    "oracle": "| Oracle |",
                    "databricks": "| Databricks SQL |",
                    "redshift": "| Amazon Redshift |",
                    "teradata": "| Teradata |",
                    "impala": "| Apache Impala |",
                }[flavor]
            )
        )
        assert capability_matrix()[flavor]["support_status"] in row.lower()


def test_archive_is_identified_as_historical():
    archive = (ROOT / "docs" / "archive" / "README.md").read_text(
        encoding="utf-8"
    )
    assert "historical" in archive
    assert "do not define current runtime behavior" in " ".join(
        archive.split()
    )


def test_kubernetes_enables_elastic_membership_with_materializer_hpa():
    config = (
        ROOT / "deploy" / "kubernetes" / "base" / "common-configmap.yaml"
    ).read_text(encoding="utf-8")
    kustomization = (
        ROOT / "deploy" / "kubernetes" / "base" / "kustomization.yaml"
    ).read_text(encoding="utf-8")
    hpa = (
        ROOT / "deploy" / "kubernetes" / "base" / "python-hpa.yaml"
    ).read_text(encoding="utf-8")

    assert "GENERATION_MEMBERSHIP_POLICY: elastic" in config
    assert "python-hpa.yaml" in kustomization
    assert "kind: HorizontalPodAutoscaler" in hpa
    assert "kind: StatefulSet" in hpa
