from __future__ import annotations

import io
import json


def test_directory_search_returns_safe_user_metadata(monkeypatch):
    import config
    from security.entra_directory import EntraDirectoryClient

    monkeypatch.setattr(config, "ENTRA_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", raising=False)
    monkeypatch.setattr(config, "ENTRA_GRAPH_CLIENT_ID", "graph-client", raising=False)
    client = EntraDirectoryClient()
    monkeypatch.setattr(client, "_access_token", lambda: "graph-token")
    captured = {}

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.headers)
        return Response(json.dumps({"value": [{
            "id": "11111111-2222-3333-4444-555555555555",
            "displayName": "Alex Operator",
            "userPrincipalName": "alex@example.com",
            "mail": "alex@example.com",
            "passwordProfile": {"password": "never return"},
        }]}).encode())

    monkeypatch.setattr("security.entra_directory.urlopen", fake_urlopen)
    results = client.search_users("Alex")

    assert results == [{
        "id": "11111111-2222-3333-4444-555555555555",
        "tenant_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "kind": "user",
        "display_name": "Alex Operator",
        "user_principal_name": "alex@example.com",
        "mail": "alex@example.com",
    }]
    assert "startswith%28displayName%2C%27Alex%27%29" in captured["url"]
    assert captured["headers"]["Authorization"] == "Bearer graph-token"
    assert captured["headers"]["Consistencylevel"] == "eventual"


def test_directory_search_filters_security_groups(monkeypatch):
    import config
    from security.entra_directory import EntraDirectoryClient

    monkeypatch.setattr(config, "ENTRA_TENANT_ID", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", raising=False)
    monkeypatch.setattr(config, "ENTRA_GRAPH_CLIENT_ID", "graph-client", raising=False)
    client = EntraDirectoryClient()
    monkeypatch.setattr(client, "_access_token", lambda: "graph-token")

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        return Response(json.dumps({"value": [
            {"id": "22222222-3333-4444-5555-666666666666", "displayName": "Proxy Operators", "securityEnabled": True},
            {"id": "33333333-4444-5555-6666-777777777777", "displayName": "Mail Group", "securityEnabled": False},
        ]}).encode())

    monkeypatch.setattr("security.entra_directory.urlopen", fake_urlopen)
    results = client.search_security_groups("Proxy")

    assert [item["display_name"] for item in results] == ["Proxy Operators"]
    assert results[0]["tenant_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert results[0]["kind"] == "group"
