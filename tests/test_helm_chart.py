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


def _workload_names(rendered: str) -> set[str]:
    return set(
        re.findall(
            r"(?m)^kind: (?:Deployment|StatefulSet)\nmetadata:\n  name: (\S+)$",
            rendered,
        )
    )


def _network_policy_names(rendered: str) -> set[str]:
    return set(
        re.findall(
            r"(?ms)^kind: NetworkPolicy\nmetadata:\n  name: (\S+)$",
            rendered,
        )
    )


def test_chart_lints_with_enterprise_example() -> None:
    _helm("lint", str(CHART), "-f", str(EXAMPLE_VALUES))


def test_manager_binds_operator_artifact_profile() -> None:
    rendered = _render("--set", "manager.artifactStoreProfile=eu-published")
    assert 'name: FSP_ARTIFACT_STORE_PROFILE\n              value: "eu-published"' in rendered


def test_split_row_and_compressed_byte_targets_render() -> None:
    rendered = _render(
        "--set-string", "fsp.splitTargetRows=250000",
        "--set-string", "fsp.splitTargetBytes=134217728",
        "--set-string", "fsp.splitEstimatedBytesPerRow=96",
    )

    assert 'SPLIT_TARGET_ROWS: "250000"' in rendered
    assert 'SPLIT_TARGET_BYTES: "134217728"' in rendered
    assert 'SPLIT_ESTIMATED_BYTES_PER_ROW: "96"' in rendered


def test_manager_renders_signed_policy_bundle_and_public_key() -> None:
    rendered = _render(
        "--set", "manager.policyBundle.enabled=true",
        "--set", "manager.policyBundle.url=https://policy.example.test/placement.json",
        "--set", "manager.policyBundle.publicKeySecretName=fsp-policy-key",
        "--set", "manager.policyBundle.keyId=global-2026",
    )

    assert "name: FSP_POLICY_BUNDLE_URL" in rendered
    assert 'value: "https://policy.example.test/placement.json"' in rendered
    assert "name: FSP_POLICY_BUNDLE_PUBLIC_KEY_FILE" in rendered
    assert "name: policy-bundle-public-key" in rendered
    assert "secretName: fsp-policy-key" in rendered
    assert "key: public-key.pem" in rendered
    assert "path: public-key.pem" in rendered


def test_manager_renders_private_azure_policy_bundle() -> None:
    rendered = _render(
        "--set", "manager.policyBundle.enabled=true",
        "--set",
        (
            "manager.policyBundle.azure.accountUrl="
            "https://catalog.blob.core.windows.net"
        ),
        "--set", "manager.policyBundle.azure.container=catalog",
        "--set",
        "manager.policyBundle.azure.blobName=policies/phase5.json",
        "--set",
        "manager.policyBundle.publicKeySecretName=fsp-policy-key",
    )

    assert "name: FSP_POLICY_BUNDLE_ACCOUNT_URL" in rendered
    assert "https://catalog.blob.core.windows.net" in rendered
    assert "name: FSP_POLICY_BUNDLE_CONTAINER" in rendered
    assert 'value: "catalog"' in rendered
    assert "name: FSP_POLICY_BUNDLE_BLOB_NAME" in rendered
    assert 'value: "policies/phase5.json"' in rendered
    assert "name: FSP_POLICY_BUNDLE_URL" not in rendered


def test_manager_rejects_multiple_policy_bundle_sources() -> None:
    result = _helm(
        "template",
        "fsp",
        str(CHART),
        "--set", "manager.policyBundle.enabled=true",
        "--set",
        "manager.policyBundle.url=https://policy.example.test/bundle.json",
        "--set",
        (
            "manager.policyBundle.azure.accountUrl="
            "https://catalog.blob.core.windows.net"
        ),
        expect_success=False,
    )

    assert "requires exactly one of url or azure.accountUrl" in result.stderr


def test_manager_renders_operator_supplied_egress_pricing() -> None:
    rendered = _render(
        "--set-json",
        (
            'manager.egressPricing={"currency":"USD","rates":'
            '{"azure:swedencentral->azure:northeurope":0.02}}'
        ),
    )

    assert "name: FSP_EGRESS_PRICING" in rendered
    assert "azure:swedencentral-\\\\u003eazure:northeurope" in rendered
    assert '\\"currency\\":\\"USD\\"' in rendered


def test_signed_manager_does_not_receive_static_placement_policy() -> None:
    rendered = _render(
        "--set", "manager.policyBundle.enabled=true",
        "--set",
        "manager.policyBundle.url=https://policy.example.test/bundle.json",
        "--set",
        "manager.policyBundle.publicKeySecretName=fsp-policy-key",
        "--set",
        "manager.agentPlacementConfig.pools[0].pool_id=pool-a",
    )

    manager = next(
        document
        for document in rendered.split("\n---")
        if "# Source: fabric-shortcut-proxy/templates/manager.yaml"
        in document
        and "kind: Deployment" in document
    )
    assert (
        'name: AGENT_PLACEMENT_CONFIG\n              value: "{}"'
        in manager
    )
    assert '"pool_id":"pool-a"' not in manager


def test_federation_renders_catalog_and_regional_manager_identity() -> None:
    rendered = _render(
        "--set", "federation.enabled=true",
        "--set", "federation.deployCatalog=true",
        "--set", "federation.managerId=manager-swc",
        "--set", "federation.managerLocation=azure:swedencentral",
        "--set", "federation.catalog.artifactStoreAccountUrl=https://catalog.blob.core.windows.net",
        "--set", "manager.artifactStoreProfile=swc-published",
    )
    assert "name: fsp-global-catalog" in rendered
    assert "python\n            - -m\n            - enterprise.catalog" in rendered
    assert 'name: FSP_REGIONAL_MANAGER_ID\n              value: "manager-swc"' in rendered
    assert 'name: FSP_CATALOG_OWNER_LEASE_MS\n              value: "60000"' in rendered
    assert 'name: FSP_CATALOG_STORE_CONTAINER\n              value: "catalog"' in rendered


def test_regional_serving_renders_read_only_identity_and_no_source_secret() -> None:
    rendered = _render(
        "--set", "federation.regionalServing.enabled=true",
        "--set", "federation.regionalServing.endpointId=neu",
        "--set", "federation.regionalServing.storageProfile=neu-replica",
        "--set", "federation.regionalServing.buckets.sales=source::sales",
        "--set", "federation.regionalServing.serviceAccountName=regional-reader",
        "--set", "federation.regionalServing.clientId=00000000-0000-0000-0000-000000000001",
        "--set", "federation.regionalServing.readerSecretName=reader-sigv4",
        "--set", "federation.regionalServing.artifactStoreAccountUrl=https://replica.blob.core.windows.net",
        "--set", "federation.regionalServing.artifactStoreContainer=published",
        "--set", "federation.catalog.artifactStoreAccountUrl=https://catalog.blob.core.windows.net",
        "--set", "networkPolicy.enabled=true",
    )
    section = "\n".join(
        document for document in rendered.split("\n---")
        if "# Source: fabric-shortcut-proxy/templates/regional-serving.yaml" in document
    )
    assert "enterprise.regional_serving" in section
    assert "kind: ServiceAccount" in section
    assert 'azure.workload.identity/client-id: "00000000-0000-0000-0000-000000000001"' in section
    assert "serviceAccountName: regional-reader" in section
    assert "name: reader-sigv4" in section
    assert "name: fsp-source" not in section
    assert "name: FSP_REGIONAL_SERVING_ENDPOINT_ID" in section
    assert "readOnlyRootFilesystem: true" in section
    assert "fsp-regional-serving" in _network_policy_names(rendered)


def test_regional_serving_rejects_unassigned_buckets() -> None:
    result = _helm("template", "fsp", str(CHART), "--set",
                   "federation.regionalServing.enabled=true", expect_success=False)
    assert "federation.regionalServing.buckets is required" in result.stderr


def test_regional_serving_tls_ingress_is_scoped_to_reader_service() -> None:
    rendered = _render(
        "--set", "federation.regionalServing.enabled=true",
        "--set", "federation.regionalServing.endpointId=primary",
        "--set", "federation.regionalServing.storageProfile=published",
        "--set", "federation.regionalServing.buckets.sales=source::sales",
        "--set", "federation.regionalServing.serviceAccountName=regional-reader",
        "--set", "federation.regionalServing.clientId=00000000-0000-0000-0000-000000000001",
        "--set", "federation.regionalServing.readerSecretName=reader-sigv4",
        "--set", "federation.regionalServing.artifactStoreAccountUrl=https://published.blob.core.windows.net",
        "--set", "federation.regionalServing.artifactStoreContainer=published",
        "--set", "federation.catalog.artifactStoreAccountUrl=https://catalog.blob.core.windows.net",
        "--set", "federation.regionalServing.ingress.enabled=true",
        "--set", "federation.regionalServing.ingress.hostname=regional.example.test",
        "--set", "networkPolicy.enabled=true",
    )
    assert "name: fsp-regional-serving-tls" in rendered
    assert 'host: "regional.example.test"' in rendered
    assert "name: fsp-regional-serving\n                port:\n                  name: s3" in rendered
    assert "name: fsp-acme-http-solver" in rendered
    assert 'acme.cert-manager.io/http01-solver: "true"' in rendered
    assert "port: 8089" in rendered


def test_base_render_contains_core_fleet_only() -> None:
    rendered = _render()
    counts = _kind_counts(rendered)

    assert sum(counts.values()) == 12
    assert counts["Deployment"] == 2
    assert counts["StatefulSet"] == 1
    assert counts["Service"] == 3
    assert "Ingress" not in counts
    assert "PersistentVolume" not in counts
    assert "name: AGENT_AUTH_MODE" in rendered
    assert 'value: "required"' in rendered
    assert len(re.findall(r"^\s+- name: AGENT_TOKEN$", rendered, re.MULTILINE)) == 3
    assert len(
        re.findall(r"^\s+- name: AGENT_TOKEN_PREVIOUS$", rendered, re.MULTILINE)
    ) == 1
    assert len(
        re.findall(
            r"^\s+- name: AGENT_TOKEN_PREVIOUS_VALID_UNTIL$", rendered, re.MULTILINE
        )
    ) == 1
    assert "key: AGENT_TOKEN" in rendered
    assert "name: S3_AUTH_MODE" in rendered
    assert 'value: "trusted-upstream"' in rendered
    assert "name: S3_ACCESS_KEY_ID" in rendered
    assert "name: S3_SECRET_ACCESS_KEY" in rendered
    assert "MANAGER_AUTH_USERNAME" not in rendered
    assert "MANAGER_AUTH_PASSWORD" not in rendered
    assert "replace-with-at-least-32-random-bytes" not in rendered


def test_entra_render_uses_workload_identity_without_agent_secrets() -> None:
    rendered = _render(
        "--set", "agentAuth.mode=entra",
        "--set", "cppAgent.enabled=false",
        "--set", "workloadIdentity.enabled=true",
        "--set", "workloadIdentity.clientId=33333333-3333-4333-8333-333333333333",
        "--set", "agentAuth.entra.tenantId=11111111-1111-4111-8111-111111111111",
        "--set", "agentAuth.entra.audience=22222222-2222-4222-8222-222222222222",
    )
    assert 'AGENT_AUTH_MODE: "entra"' in rendered
    assert 'AGENT_ENTRA_SCOPE: "api://22222222-2222-4222-8222-222222222222/.default"' in rendered
    assert "name: AGENT_TOKEN" not in rendered
    assert "name: AGENT_IDENTITY_TOKENS" not in rendered
    assert "key: AGENT_TOKEN" not in rendered
    assert 'azure.workload.identity/use: "true"' in rendered


def test_mtls_agent_fallback_renders_ca_and_ingress_verification() -> None:
    fingerprint = "a" * 64
    rendered = _render(
        "--set", "agentAuth.mtls.enabled=true",
        "--set", "agentAuth.mtls.caSecretName=fsp-agent-ca",
        "--set-string", f"agentAuth.mtls.identities.{fingerprint}=hybrid-site-01",
        "--set", "agentControlIngress.enabled=true",
        "--set", "agentControlIngress.host=control.example.com",
    )
    assert "name: AGENT_MTLS_IDENTITIES" in rendered
    assert "name: AGENT_MTLS_CA_FILE" in rendered
    assert "secretName: fsp-agent-ca" in rendered
    assert (
        'nginx.ingress.kubernetes.io/auth-tls-secret: '
        '"fabric-shortcut-proxy/fsp-agent-ca"' in rendered
    )
    assert 'nginx.ingress.kubernetes.io/auth-tls-verify-client: "optional"' in rendered
    assert (
        'nginx.ingress.kubernetes.io/auth-tls-pass-certificate-to-upstream: "true"'
        in rendered
    )


@pytest.mark.parametrize(
    "arguments",
    [
        ["--set", "agentAuth.mode=entra"],
        [
            "--set", "agentAuth.mode=entra",
            "--set", "workloadIdentity.enabled=true",
            "--set", "workloadIdentity.clientId=33333333-3333-4333-8333-333333333333",
        ],
    ],
)
def test_entra_chart_rejects_unsupported_identity_configuration(arguments) -> None:
    result = _helm("template", "fsp", str(CHART), *arguments, expect_success=False)
    assert result.returncode != 0
    assert "agentAuth.mode=entra" in result.stderr


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
    ("values_file", "expected_workloads", "expected_network_policies"),
    [
        (
            "values-manager.yaml",
            {"fsp-manager"},
            {
                "fsp-default-deny",
                "fsp-allow-dns",
                "fsp-manager-ingress",
                "fsp-manager-egress",
            },
        ),
        (
            "values-serving-agents.yaml",
            {"fsp-cpp-agent"},
            {
                "fsp-default-deny",
                "fsp-allow-dns",
                "fsp-serving-agent-ingress",
                "fsp-serving-agent-egress",
            },
        ),
        (
            "values-remote-materializer.yaml",
            {"fsp-materializer"},
            {
                "fsp-default-deny",
                "fsp-allow-dns",
                "fsp-materializer-ingress",
                "fsp-materializer-egress",
            },
        ),
    ],
)
def test_role_profiles_render_only_their_workloads(
    values_file: str,
    expected_workloads: set[str],
    expected_network_policies: set[str],
) -> None:
    rendered = _render("-f", str(CHART / values_file))

    assert _workload_names(rendered) == expected_workloads
    assert _network_policy_names(rendered) == expected_network_policies


def test_cpp_sigv4_mode_renders_secret_references_and_prefixes() -> None:
    rendered = _render(
        "--set",
        "cppAgent.s3AuthMode=sigv4",
        "--set-string",
        "cppAgent.s3AllowedPrefixes=tenant/;shared/",
    )
    assert 'value: "sigv4"' in rendered
    assert "name: fsp-cpp-s3-auth" in rendered
    assert "key: S3_ACCESS_KEY_ID" in rendered
    assert "key: S3_SECRET_ACCESS_KEY" in rendered
    assert 'value: "tenant/;shared/"' in rendered


def test_materializer_autoscaling_renders_elastic_membership_hpa() -> None:
    rendered = _render(
        "--set",
        "materializer.autoscaling.enabled=true",
        "--set",
        "fsp.materializeMode=lazy",
    )
    assert "kind: HorizontalPodAutoscaler" in rendered
    assert "name: fsp-materializer" in rendered
    assert 'GENERATION_MEMBERSHIP_POLICY: "elastic"' in rendered


def test_materializer_autoscaling_requires_lazy_elastic_queue() -> None:
    eager = _helm(
        "template",
        "fsp",
        str(CHART),
        "--set",
        "materializer.autoscaling.enabled=true",
        expect_success=False,
    )
    assert eager.returncode != 0
    assert "requires fsp.materializeMode=lazy" in eager.stderr

    fixed = _helm(
        "template",
        "fsp",
        str(CHART),
        "--set",
        "materializer.autoscaling.enabled=true",
        "--set",
        "fsp.materializeMode=lazy",
        "--set",
        "fsp.generationMembershipPolicy=fixed",
        expect_success=False,
    )
    assert fixed.returncode != 0
    assert "requires fsp.generationMembershipPolicy=elastic" in fixed.stderr


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


def test_nginx_requires_materializer_workload() -> None:
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
        "materializer.enabled=false",
        expect_success=False,
    )

    assert result.returncode != 0
    assert "nginx.enabled requires materializer.enabled=true" in result.stderr


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


def test_control_ingress_prefix_preserves_agent_route_allowlist() -> None:
    rendered = _render(
        "--show-only", "templates/agent-control-ingress.yaml",
        "--set", "agentControlIngress.enabled=true",
        "--set", "agentControlIngress.host=control.example.com",
        "--set", "agentControlIngress.pathPrefix=/phase3",
    )
    paths = re.findall(r'^\s+- path: "([^"]+)"$', rendered, re.MULTILINE)
    assert len(paths) == 7
    assert 'nginx.ingress.kubernetes.io/rewrite-target: "$1"' in rendered
    for route in (
        "/readyz", "/control/register", "/control/heartbeat", "/control/task-result",
        "/control/materialize", "/control/assignment/site-a-agent",
        "/control/snapshot/customer",
    ):
        matches = [
            match for path in paths
            if (match := re.fullmatch(path, "/phase3" + route))
        ]
        assert len(matches) == 1
        assert matches[0].group(1) == route
    for route in (
        "/control/work-queue", "/_manager/api/fleet",
        "/control/register/extra", "/control/register-forged",
        "/control/heartbeat/../work-queue",
    ):
        assert not any(re.fullmatch(path, "/phase3" + route) for path in paths)


@pytest.mark.parametrize("prefix", ["/phase3/", "/phase3(.*)", "phase3"])
def test_control_ingress_rejects_invalid_path_prefix(prefix: str) -> None:
    result = _helm(
        "template", "fsp", str(CHART),
        "--set-string", f"agentControlIngress.pathPrefix={prefix}",
        expect_success=False,
    )
    assert result.returncode != 0
    assert "pathPrefix" in result.stderr


def test_azure_files_supports_isolated_volume_names_and_subpaths() -> None:
    rendered = _render(
        "-f", str(EXAMPLE_VALUES),
        "--set", "storage.azureFiles.artifacts.pvName=phase3-artifacts-pv",
        "--set", "storage.azureFiles.managerConfig.pvName=phase3-config-pv",
        "--set", "storage.azureFiles.artifacts.subPath=phase3/artifacts",
        "--set", "storage.azureFiles.managerConfig.subPath=phase3/config",
    )
    assert "name: phase3-artifacts-pv" in rendered
    assert "volumeName: phase3-artifacts-pv" in rendered
    assert "name: phase3-config-pv" in rendered
    assert "volumeName: phase3-config-pv" in rendered
    assert rendered.count('subPath: "phase3/artifacts"') == 2
    assert rendered.count('subPath: "phase3/config"') == 2


def test_materializer_pool_token_can_differ_from_manager_shared_token() -> None:
    rendered = _render(
        "--show-only", "templates/materializer.yaml",
        "--set", "materializer.agentAuthSecretName=site-a-pool-auth",
    )
    assert "name: site-a-pool-auth" in rendered
    assert "name: fsp-agent-auth" not in rendered
