"""AGENT_ADVERTISE_HOST: register payload carries it; registry exposes it as the
routable ``host`` in the public view the LB renderer consumes."""
from __future__ import annotations

from enterprise.control.contract import RegisterRequest
from enterprise.control.registry import Registry


def test_register_request_roundtrip_advertise_host():
    req = RegisterRequest(agent_id="a", host="0.0.0.0", port=9400, os="linux",
                          version="v", advertise_host="10.0.0.7")
    back = RegisterRequest.from_dict(req.to_dict())
    assert back.advertise_host == "10.0.0.7"


def test_register_request_defaults_advertise_host_empty():
    # Older agents omit the field entirely; it must default to "".
    d = {"agent_id": "a", "host": "0.0.0.0", "port": 9400, "os": "linux", "version": "v"}
    assert RegisterRequest.from_dict(d).advertise_host == ""


def test_public_host_prefers_advertised():
    reg = Registry()
    reg.register(RegisterRequest(agent_id="a", host="0.0.0.0", port=9400, os="linux",
                                 version="v", advertise_host="agent-a.internal"))
    pub = reg.list_public()[0]
    assert pub["host"] == "agent-a.internal"
    assert pub["bind_host"] == "0.0.0.0"


def test_public_host_falls_back_to_bind():
    reg = Registry()
    reg.register(RegisterRequest(agent_id="b", host="10.0.0.9", port=9400, os="linux",
                                 version="v"))
    pub = reg.list_public()[0]
    assert pub["host"] == "10.0.0.9"
    assert pub["bind_host"] == "10.0.0.9"
