"""
Operator console — the Manager's ``/_manager`` admin page (SCALE_ARCHITECTURE_PLAN
§14, Phase 4).

Serves a **self‑contained** HTML page (no external assets) that monitors the Agent
fleet and lets an operator **start / stop / restart / drain** individual Agents
from a browser, plus a small JSON admin API that backs the page (and is scriptable):

    GET  /_manager                              -> the console HTML
    GET  /_manager/api/fleet                    -> fleet snapshot (JSON)
    POST /_manager/api/agents/{name}/{action}   -> start|stop|restart|drain

start/stop/restart act through the existing :class:`~control.supervisor.AgentSupervisor`
(process control); drain queues a :class:`~control.contract.Drain` command that the
Agent picks up on its next heartbeat (graceful recycle). Mutating actions are
guarded by ``ADMIN_TOKEN`` when set (``X-Admin-Token`` header or ``?token=``);
reads stay open. The whole router is gated behind ``ENABLE_ADMIN_UI`` and mounted
*before* the gateway catch‑all so it never touches the S3 data path.
"""
from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

import config
from control.contract import ControlCommand, Drain
from control.registry import Registry
from control.supervisor import AgentSupervisor
from observability.logging import get_logger

log = get_logger(__name__)

_ACTIONS = ("start", "stop", "restart", "drain")


def fleet_snapshot(
    registry: Registry,
    supervisors: list[AgentSupervisor],
    *,
    gateway: object | None = None,
    token_required: bool = False,
) -> dict:
    """Combine per‑Agent supervisor state (process) with the registry view
    (heartbeat/serving) into one JSON‑able fleet snapshot for the console."""
    public = {r["agent_id"]: r for r in registry.list_public()}
    dead = set(registry.dead_agents())

    agents: list[dict] = []
    alive_procs = 0
    for sup in supervisors:
        env = sup.env or {}
        alive = sup.is_alive
        if alive:
            alive_procs += 1
        rec = public.get(sup.name)
        agents.append({
            "name": sup.name,
            "pid": sup.pid,
            "supervised": sup.is_running,
            "process_alive": alive,
            "crash_looped": sup.crash_looped,
            "restart_count": sup.restart_count,
            "port": int(env["PORT"]) if str(env.get("PORT", "")).isdigit() else None,
            "shard_index": int(env.get("AGENT_SHARD_INDEX", 0) or 0),
            "shard_count": int(env.get("AGENT_SHARD_COUNT", 1) or 1),
            "registered": rec is not None,
            "heartbeat_age": rec["seconds_since_heartbeat"] if rec else None,
            "serving_tables": rec["serving_tables"] if rec else [],
            "epochs": rec["epochs"] if rec else {},
            "pending_commands": rec["pending_commands"] if rec else 0,
            "dead": sup.name in dead,
        })

    registered = registry.count()
    alive_registered = registered - len(dead)
    ready = alive_procs >= 1 and not any(s.crash_looped for s in supervisors)
    return {
        "role": "manager",
        "ready": ready,
        "agents_total": len(supervisors),
        "agents_alive": alive_procs,
        "agents_registered": registered,
        "agents_registered_alive": alive_registered,
        "gateway_enabled": gateway is not None,
        "gateway_targets": alive_registered if gateway is not None else 0,
        "admin_token_required": token_required,
        "agents": agents,
    }


def create_admin_router(
    registry: Registry,
    supervisors: list[AgentSupervisor],
    *,
    gateway: object | None = None,
    token: str = "",
    scale=None,
    shutdown=None,
) -> APIRouter:
    """Build the ``/_manager`` console + admin‑API router."""
    router = APIRouter()
    token_required = bool(token)

    def _by_name(name: str):
        # Rebuild per request so a live-scaled fleet is always actionable.
        for s in supervisors:
            if s.name == name:
                return s
        return None

    def _check_token(request: Request) -> None:
        if not token_required:
            return
        supplied = request.headers.get("x-admin-token") or request.query_params.get("token") or ""
        if supplied != token:
            raise HTTPException(status_code=401, detail="admin token required")

    async def _respawn(sup: AgentSupervisor) -> None:
        await sup.stop()
        await sup.start()

    @router.get("/_manager", response_class=HTMLResponse)
    async def manager_page() -> str:
        return _ADMIN_HTML

    @router.get("/_manager/api/fleet")
    async def fleet() -> dict:
        return fleet_snapshot(registry, supervisors, gateway=gateway, token_required=token_required)

    @router.post("/_manager/api/agents/{name}/{action}")
    async def agent_action(name: str, action: str, request: Request) -> dict:
        _check_token(request)
        sup = _by_name(name)
        if sup is None:
            raise HTTPException(status_code=404, detail=f"unknown agent {name!r}")
        action = action.lower()
        if action not in _ACTIONS:
            raise HTTPException(status_code=400, detail=f"unknown action {action!r}; expected one of {_ACTIONS}")

        if action == "drain":
            ok = registry.queue_command(name, ControlCommand(kind="drain", drain=Drain()))
            log.info("admin_action", action="drain", agent=name, queued=ok)
            return {
                "ok": ok, "action": "drain", "agent": name,
                "note": "queued; the Agent drains on its next heartbeat" if ok
                        else "agent not registered — nothing queued",
            }

        if action == "stop":
            registry.remove(name)          # drop from gateway rotation immediately
            await sup.stop()
        elif action == "restart":
            registry.remove(name)          # drop from rotation; it re-registers on start
            await _respawn(sup)
        else:  # start
            if not sup.is_alive:
                await _respawn(sup)   # revive a stopped or crash‑looped Agent

        log.info("admin_action", action=action, agent=name, pid=sup.pid, alive=sup.is_alive)
        return {"ok": True, "action": action, "agent": name,
                "pid": sup.pid, "process_alive": sup.is_alive}

    @router.post("/_manager/api/rolling-restart")
    async def rolling_restart_action(request: Request) -> dict:
        _check_token(request)
        from control.rolling import rolling_restart
        # Fire-and-forget: recycle Agents one at a time so >= N-1 keep serving.
        # The console's fleet poll shows them cycling.
        asyncio.create_task(rolling_restart(
            supervisors,
            is_healthy=registry.is_alive,
            health_timeout=config.ROLLING_RESTART_HEALTH_TIMEOUT,
            before_stop=registry.remove,
        ))
        log.info("admin_rolling_restart_started", agents=len(supervisors))
        return {"ok": True, "action": "rolling-restart", "agents": len(supervisors),
                "note": "restarting one Agent at a time; watch the fleet table"}

    if scale is not None:
        @router.post("/_manager/api/scale")
        async def scale_action(request: Request) -> dict:
            _check_token(request)
            body = await request.json()
            try:
                count = int(body.get("count"))
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="body must be {\"count\": <int >= 1>}")
            if count < 1:
                raise HTTPException(status_code=400, detail="count must be >= 1")
            try:
                result = await scale(count)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
            log.info("admin_scale", target=count, count=result.get("count"))
            return result

    if shutdown is not None:
        @router.post("/_manager/api/shutdown")
        async def shutdown_action(request: Request) -> dict:
            _check_token(request)
            log.info("admin_shutdown_requested")
            return await shutdown()

    return router


# ---------------------------------------------------------------------------
# Self‑contained console page (no external assets). Polls /_manager/api/fleet.
# ---------------------------------------------------------------------------
_ADMIN_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Fabric Shortcut Proxy — Manager</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.45 -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
         background: #0f1420; color: #e6e9ef; }
  header { padding: 14px 20px; border-bottom: 1px solid #232a3a; display: flex; align-items: center;
           gap: 16px; flex-wrap: wrap; background: #131a29; }
  h1 { font-size: 16px; margin: 0; font-weight: 650; letter-spacing: .2px; }
  .pill { padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .ok { background: #12351f; color: #6ee7a0; }
  .bad { background: #3a1620; color: #ff8098; }
  .muted { color: #8a93a6; }
  main { padding: 18px 20px; }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
  .card { background: #151c2c; border: 1px solid #232a3a; border-radius: 10px; padding: 12px 16px; min-width: 120px; }
  .card .n { font-size: 22px; font-weight: 700; }
  .card .l { font-size: 11px; text-transform: uppercase; letter-spacing: .6px; color: #8a93a6; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 9px 12px; border-bottom: 1px solid #202737; white-space: nowrap; }
  th { font-size: 11px; text-transform: uppercase; letter-spacing: .5px; color: #8a93a6; font-weight: 600; }
  tr:hover td { background: #131a29; }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 7px; vertical-align: middle; }
  .green { background: #35d07f; } .red { background: #ff5c78; } .amber { background: #f5a623; }
  button { font: inherit; font-size: 12px; padding: 4px 10px; margin-right: 5px; border-radius: 7px;
           border: 1px solid #313a4f; background: #1c2740; color: #dfe6f2; cursor: pointer; }
  button:hover { background: #24365c; }
  button.stop { border-color: #5a2230; } button.stop:hover { background: #3a1622; }
  button:disabled { opacity: .4; cursor: not-allowed; }
  .tok { margin-left: auto; display: flex; align-items: center; gap: 8px; }
  input { font: inherit; padding: 4px 8px; border-radius: 7px; border: 1px solid #313a4f;
          background: #0d1320; color: #e6e9ef; }
  #msg { margin: 0 0 12px; min-height: 18px; font-size: 13px; }
  .err { color: #ff8098; } .good { color: #6ee7a0; }
  code { color: #9fb4ff; }
</style>
</head>
<body>
<header>
  <h1>Fabric Shortcut Proxy · Manager</h1>
  <span id="ready" class="pill muted">…</span>
  <span id="clock" class="muted" style="font-size:12px"></span>
  <div class="tok">
    <label class="muted" id="toklabel" style="display:none">admin token
      <input id="token" type="password" size="16" placeholder="X-Admin-Token"/>
    </label>
    <label class="muted"><input id="auto" type="checkbox" checked/> auto‑refresh</label>
  </div>
</header>
<main>
  <div class="cards" id="cards"></div>
  <p id="msg"></p>
  <div style="margin:0 0 12px">
    <button onclick="rollingRestart()">Rolling restart (one at a time)</button>
    <button class="stop" onclick="shutdownManager()">Shutdown Manager + all Agents</button>
  </div>
  <table>
    <thead><tr>
      <th>Agent</th><th>State</th><th>PID</th><th>Port</th><th>Shard</th>
      <th>Restarts</th><th>Heartbeat</th><th>Serving</th><th>Actions</th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
</main>
<script>
const $ = (id) => document.getElementById(id);
let tokenRequired = false;

function tok() { return $("token").value.trim(); }
function headers() {
  const h = { "Content-Type": "application/json" };
  const t = tok(); if (t) h["X-Admin-Token"] = t;
  return h;
}
function msg(text, cls) { const m = $("msg"); m.textContent = text || ""; m.className = cls || ""; }

async function refresh() {
  try {
    const r = await fetch("/_manager/api/fleet", { cache: "no-store" });
    const d = await r.json();
    render(d);
  } catch (e) { msg("failed to load fleet: " + e, "err"); }
}

function stateCell(a) {
  let dot = "green", label = "alive";
  if (a.crash_looped) { dot = "red"; label = "crash‑loop"; }
  else if (!a.process_alive) { dot = "red"; label = "stopped"; }
  else if (a.dead || !a.registered) { dot = "amber"; label = a.registered ? "no heartbeat" : "unregistered"; }
  return `<span class="dot ${dot}"></span>${label}`;
}

function render(d) {
  tokenRequired = !!d.admin_token_required;
  $("toklabel").style.display = tokenRequired ? "" : "none";
  const ready = $("ready");
  ready.textContent = d.ready ? "READY" : "NOT READY";
  ready.className = "pill " + (d.ready ? "ok" : "bad");
  $("clock").textContent = "updated " + new Date().toLocaleTimeString();

  const cards = [
    ["Agents alive", d.agents_alive + " / " + d.agents_total],
    ["Registered", d.agents_registered_alive + " / " + d.agents_registered],
    ["Gateway", d.gateway_enabled ? (d.gateway_targets + " targets") : "off"],
  ];
  $("cards").innerHTML = cards.map(c =>
    `<div class="card"><div class="n">${c[1]}</div><div class="l">${c[0]}</div></div>`).join("");

  $("rows").innerHTML = d.agents.map(a => {
    const serving = (a.serving_tables && a.serving_tables.length)
      ? a.serving_tables.map(t => `${t}@${a.epochs[t] ?? "?"}`).join(", ")
      : '<span class="muted">—</span>';
    const hb = (a.heartbeat_age == null) ? '<span class="muted">—</span>' : a.heartbeat_age + "s";
    const startDisabled = a.process_alive && !a.crash_looped ? "disabled" : "";
    const stopDisabled = a.process_alive ? "" : "disabled";
    return `<tr>
      <td><b>${a.name}</b></td>
      <td>${stateCell(a)}</td>
      <td>${a.pid ?? '<span class="muted">—</span>'}</td>
      <td>${a.port ?? "—"}</td>
      <td>${a.shard_index}/${a.shard_count}</td>
      <td>${a.restart_count}</td>
      <td>${hb}</td>
      <td>${serving}</td>
      <td>
        <button ${startDisabled} onclick="act('${a.name}','start')">Start</button>
        <button class="stop" ${stopDisabled} onclick="act('${a.name}','stop')">Stop</button>
        <button ${stopDisabled} onclick="act('${a.name}','restart')">Restart</button>
        <button ${stopDisabled} onclick="act('${a.name}','drain')">Drain</button>
      </td></tr>`;
  }).join("");
}

async function act(name, action) {
  if ((action === "stop" || action === "restart") &&
      !confirm(`${action} ${name}?`)) return;
  msg(`${action} ${name}…`);
  try {
    const r = await fetch(`/_manager/api/agents/${name}/${action}`, { method: "POST", headers: headers() });
    const d = await r.json();
    if (!r.ok) { msg(`${action} ${name} failed: ${d.detail || r.status}`, "err"); }
    else { msg(d.note || `${action} ${name}: ok`, "good"); }
  } catch (e) { msg(`${action} ${name} error: ${e}`, "err"); }
  setTimeout(refresh, 400);
}

async function rollingRestart() {
  if (!confirm("Rolling restart the whole fleet, one Agent at a time?")) return;
  msg("rolling restart started…");
  try {
    const r = await fetch("/_manager/api/rolling-restart", { method: "POST", headers: headers() });
    const d = await r.json();
    if (!r.ok) { msg(`rolling restart failed: ${d.detail || r.status}`, "err"); }
    else { msg(d.note || "rolling restart started", "good"); }
  } catch (e) { msg(`rolling restart error: ${e}`, "err"); }
  setTimeout(refresh, 400);
}

async function shutdownManager() {
  if (!confirm("Shut down the Manager AND kill all Agents? Reads through this Manager/gateway will stop.")) return;
  msg("shutting down…");
  try {
    const r = await fetch("/_manager/api/shutdown", { method: "POST", headers: headers() });
    const d = await r.json();
    if (!r.ok) { msg(`shutdown failed: ${d.detail || r.status}`, "err"); }
    else { msg(d.note || "shutting down…", "good"); if (timer) clearInterval(timer); }
  } catch (e) { msg(`shutdown error: ${e}`, "err"); }
}

$("auto").addEventListener("change", loop);
let timer = null;
function loop() {
  if (timer) { clearInterval(timer); timer = null; }
  if ($("auto").checked) timer = setInterval(refresh, 2000);
}
refresh(); loop();
</script>
</body>
</html>
"""
