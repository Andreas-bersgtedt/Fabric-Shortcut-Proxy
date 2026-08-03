"""Tier 2 nginx renderer: upstream rendering, backend selection, write/reload."""
from __future__ import annotations

from enterprise.control import lb_renderer as lb


def test_render_upstream_two_backends():
    conf = lb.render_upstream([("10.0.0.1", 9400), ("10.0.0.2", 9400)], "fsp_agents")
    assert "upstream fsp_agents {" in conf
    assert "server 10.0.0.1:9400 max_fails=2 fail_timeout=5s;" in conf
    assert "server 10.0.0.2:9400 max_fails=2 fail_timeout=5s;" in conf


def test_render_upstream_empty_has_down_placeholder():
    conf = lb.render_upstream([], "fsp_agents")
    assert "down" in conf and "no ready agents" in conf


def test_select_backends_excludes_dead_and_unready():
    payload = {
        "agents": [
            {"agent_id": "a", "host": "10.0.0.1", "port": 9400},
            {"agent_id": "b", "host": "10.0.0.2", "port": 9400},  # dead
            {"agent_id": "c", "host": "10.0.0.3", "port": 9400},  # unready (draining)
        ],
        "dead": ["b"],
    }
    ready = lambda host, port: host != "10.0.0.3"  # noqa: E731
    assert lb.select_backends(payload, ready) == [("10.0.0.1", 9400)]


def test_select_backends_sorted_and_deduped():
    payload = {"agents": [
        {"agent_id": "a", "host": "10.0.0.2", "port": 9400},
        {"agent_id": "b", "host": "10.0.0.1", "port": 9400},
        {"agent_id": "c", "host": "10.0.0.1", "port": 9400},
    ], "dead": []}
    assert lb.select_backends(payload, lambda h, p: True) == [
        ("10.0.0.1", 9400), ("10.0.0.2", 9400)]


def test_select_backends_skips_bad_port():
    payload = {"agents": [{"agent_id": "a", "host": "10.0.0.1", "port": "nope"}], "dead": []}
    assert lb.select_backends(payload, lambda h, p: True) == []


def test_write_if_changed(tmp_path):
    p = tmp_path / "up.conf"
    assert lb.write_if_changed(str(p), "a") is True
    assert lb.write_if_changed(str(p), "a") is False
    assert lb.write_if_changed(str(p), "b") is True
    assert p.read_text() == "b"


def test_render_once_writes_and_reloads(tmp_path, monkeypatch):
    out = tmp_path / "up.conf"
    monkeypatch.setattr(lb, "fetch_agents", lambda url, timeout=3.0: {
        "agents": [{"agent_id": "a", "host": "10.0.0.9", "port": 9400}], "dead": []})
    calls: list[str] = []
    monkeypatch.setattr(lb, "_run", lambda cmd: (calls.append(cmd) or 0))

    changed = lb.render_once("http://m", str(out), "fsp_agents", lambda h, p: True,
                             "nginx -t", "nginx -s reload")
    assert changed is True
    assert "10.0.0.9:9400" in out.read_text()
    assert calls == ["nginx -t", "nginx -s reload"]

    calls.clear()
    changed2 = lb.render_once("http://m", str(out), "fsp_agents", lambda h, p: True,
                              "nginx -t", "nginx -s reload")
    assert changed2 is False and calls == []


def test_render_once_rolls_back_on_bad_config(tmp_path, monkeypatch):
    out = tmp_path / "up.conf"
    out.write_text("upstream fsp_agents {\n    server 10.0.0.1:9400;\n}\n")
    good = out.read_text()
    monkeypatch.setattr(lb, "fetch_agents", lambda url, timeout=3.0: {
        "agents": [{"agent_id": "z", "host": "10.0.0.9", "port": 9400}], "dead": []})
    # nginx -t fails -> render_once must restore the previous good include.
    monkeypatch.setattr(lb, "_run", lambda cmd: 1 if "test" in cmd or cmd == "nginx -t" else 0)

    changed = lb.render_once("http://m", str(out), "fsp_agents", lambda h, p: True,
                             "nginx -t", "nginx -s reload")
    assert changed is False
    assert out.read_text() == good
