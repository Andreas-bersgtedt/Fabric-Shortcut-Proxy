"""Phase 1 control-plane tests: registry, REST transport, and the Agent link."""
from __future__ import annotations

import asyncio

import httpx
import pytest
from fastapi import FastAPI

from enterprise.control.contract import (
    RegisterRequest, HeartbeatRequest, AgentHealth, TaskResult,
    ControlCommand, Drain,
)
from enterprise.control.registry import Registry, LeaseError
from enterprise.control.server import ControlService
from enterprise.control.transport import create_control_router, RestControlClient, StaleLeaseError


# ---------------------------------------------------------------------------
# Registry (pure)
# ---------------------------------------------------------------------------

def _reg_req(agent_id="a1"):
    return RegisterRequest(agent_id=agent_id, host="127.0.0.1", port=9000,
                           os="linux", version="v", capacity_hint=4)


def test_register_and_heartbeat():
    reg = Registry(heartbeat_ms=2000, miss_limit=3)
    resp = reg.register(_reg_req())
    assert resp.lease_id and resp.heartbeat_ms == 2000
    hb = HeartbeatRequest(agent_id="a1", lease_id=resp.lease_id,
                          health=AgentHealth(inflight=2), serving_tables=["sales"],
                          epochs={"sales": 1})
    cmds = reg.heartbeat(hb)
    assert cmds == []
    rec = reg.get("a1")
    assert rec.serving_tables == ["sales"] and rec.epochs == {"sales": 1}


def test_stale_lease_raises():
    reg = Registry()
    reg.register(_reg_req())
    with pytest.raises(LeaseError):
        reg.heartbeat(HeartbeatRequest(agent_id="a1", lease_id="wrong"))
    with pytest.raises(LeaseError):
        reg.heartbeat(HeartbeatRequest(agent_id="ghost", lease_id="x"))


def test_command_queue_delivered_on_heartbeat():
    reg = Registry()
    lease = reg.register(_reg_req()).lease_id
    assert reg.queue_command("a1", ControlCommand(kind="drain", drain=Drain(grace_ms=1000)))
    cmds = reg.heartbeat(HeartbeatRequest(agent_id="a1", lease_id=lease))
    assert len(cmds) == 1 and cmds[0].kind == "drain" and cmds[0].drain.grace_ms == 1000
    # consumed — next heartbeat is empty
    assert reg.heartbeat(HeartbeatRequest(agent_id="a1", lease_id=lease)) == []


def test_broadcast_and_dead_detection():
    reg = Registry(heartbeat_ms=1000, miss_limit=3)
    reg.register(_reg_req("a1"))
    reg.register(_reg_req("a2"))
    assert reg.broadcast(ControlCommand(kind="reload")) == 2
    assert reg.dead_agents() == []
    # age a1 past miss_limit * heartbeat without sleeping
    reg.get("a1").last_seen -= 100
    assert reg.dead_agents() == ["a1"]
    assert reg.is_alive("a2") and not reg.is_alive("a1")


# ---------------------------------------------------------------------------
# REST transport (in-process ASGI)
# ---------------------------------------------------------------------------

def _make(registry: Registry, tables=None) -> RestControlClient:
    app = FastAPI()
    app.include_router(create_control_router(ControlService(registry, tables=tables or [])))
    return RestControlClient("http://ctl", transport=httpx.ASGITransport(app=app))


async def test_rest_register_heartbeat_roundtrip():
    reg = Registry(heartbeat_ms=1500)
    client = _make(reg, tables=["sales", "orders"])
    try:
        resp = await client.register(_reg_req())
        assert resp.heartbeat_ms == 1500 and resp.lease_id
        # queue a drain, then heartbeat should return it
        reg.queue_command("a1", ControlCommand(kind="drain", drain=Drain()))
        cmds = await client.heartbeat(HeartbeatRequest(agent_id="a1", lease_id=resp.lease_id))
        assert [c.kind for c in cmds] == ["drain"]
    finally:
        await client.aclose()


async def test_rest_stale_lease_maps_to_error():
    reg = Registry()
    client = _make(reg)
    try:
        await client.register(_reg_req())
        with pytest.raises(StaleLeaseError):
            await client.heartbeat(HeartbeatRequest(agent_id="a1", lease_id="nope"))
    finally:
        await client.aclose()


async def test_rest_assignment_snapshot_taskresult():
    reg = Registry()
    client = _make(reg, tables=["sales"])
    try:
        asg = await client.get_assignment("a1")
        assert asg.tables == ["sales"]
        # no snapshot provider in Phase 1 -> 404 -> None
        assert await client.get_snapshot("sales") is None
        ack = await client.report_task_result(TaskResult(
            agent_id="a1", table="sales", epoch=1, split_index=0, ok=True))
        assert ack.ok is True
    finally:
        await client.aclose()


# ---------------------------------------------------------------------------
# Agent link (register + heartbeat loop, drain dispatch)
# ---------------------------------------------------------------------------

async def test_agent_link_registers_and_handles_drain():
    from enterprise.agent_link import AgentLink
    reg = Registry(heartbeat_ms=20)
    client = _make(reg, tables=["sales"])
    drained = asyncio.Event()
    link = AgentLink(client=client, agent_id="a1", heartbeat_ms=20,
                     on_drain=lambda: drained.set())
    await link.start()
    try:
        assert reg.get("a1") is not None          # registered
        reg.queue_command("a1", ControlCommand(kind="drain", drain=Drain()))
        await asyncio.wait_for(drained.wait(), timeout=2.0)
    finally:
        await link.stop()
