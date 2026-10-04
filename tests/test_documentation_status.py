from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path

from db.capabilities import capability_matrix


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

    mounts = (ROOT / "storage" / "mounts.py").read_text(encoding="utf-8")
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
