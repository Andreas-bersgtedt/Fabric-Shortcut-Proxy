"""Agent authentication route and response contracts."""
from __future__ import annotations

import json

import pytest

from fabric_shortcut_proxy import config
from enterprise.control.contract import CONTRACT_VERSION
from fabric_shortcut_proxy.security.agent_auth import (
    agent_authentication_required,
    agent_authentication_unavailable,
    is_agent_route,
    is_operator_control_route,
)
from fabric_shortcut_proxy.security.credentials import scrub_dict, scrub_secrets


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/control/register"),
        ("POST", "/control/heartbeat"),
        ("GET", "/control/assignment/agent-1"),
        ("GET", "/control/snapshot/orders"),
        ("POST", "/control/task-result"),
        ("POST", "/control/materialize"),
    ],
)
def test_agent_routes_are_classified(method, path):
    assert is_agent_route(method, path)
    assert not is_operator_control_route(method, path)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/control/work-queue"),
        ("POST", "/control/work-queue/requests/request-1/cancel"),
        ("POST", "/control/work-queue/tasks/task-1/retry"),
    ],
)
def test_operator_routes_are_not_agent_routes(method, path):
    assert is_operator_control_route(method, path)
    assert not is_agent_route(method, path)


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/control/register"),
        ("POST", "/control/assignment/agent-1"),
        ("GET", "/control/assignment/"),
        ("GET", "/control/snapshot/"),
        ("GET", "/control/work-queue/requests/request-1"),
        ("POST", "/control/work-queue/requests/request-1/retry"),
        ("POST", "/control/work-queue/tasks/task-1/retry/extra"),
        ("POST", "/control/work-queue"),
        ("GET", "/control/other"),
        ("GET", "/control/assignment/agent-1/extra"),
        ("POST", "/control/materialize/extra"),
    ],
)
def test_unrecognized_routes_are_not_classified(method, path):
    assert not is_agent_route(method, path)
    assert not is_operator_control_route(method, path)


def test_generic_agent_authentication_responses():
    unauthorized = agent_authentication_required()
    unavailable = agent_authentication_unavailable()

    assert unauthorized.status_code == 401
    assert json.loads(unauthorized.body) == {"detail": "agent authentication required"}
    assert unauthorized.headers["www-authenticate"] == "FSP-Agent"
    assert unavailable.status_code == 503
    assert json.loads(unavailable.body) == {"detail": "agent authentication unavailable"}
    assert "www-authenticate" not in unavailable.headers


def test_agent_tokens_are_secret_and_absent_from_settings_catalog():
    catalog = {item["key"]: item for item in config.settings_catalog()}
    assert "agent_token" not in catalog
    assert "agent_token_previous" not in catalog
    assert catalog["agent_auth_mode"]["default"] == "compatibility"

    current = "current-agent-token-value"
    previous = "previous-agent-token-value"
    scrubbed = scrub_dict({
        "AGENT_TOKEN": current,
        "AGENT_TOKEN_PREVIOUS": previous,
    })
    assert current not in str(scrubbed)
    assert previous not in str(scrubbed)
    assert current not in scrub_secrets(f"AGENT_TOKEN={current}")
    assert previous not in scrub_secrets(f"AGENT_TOKEN_PREVIOUS={previous}")


def test_control_contract_version_remains_1_1():
    assert CONTRACT_VERSION == "1.1"
