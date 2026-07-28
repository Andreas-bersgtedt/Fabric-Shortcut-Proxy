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
            # Memory monitoring
            "rss_mb": round(sup.rss_mb, 2),
            "avg_rss_mb": round(sup.avg_rss_mb, 2),
            "peak_rss_mb": round(sup.peak_rss_mb, 2),
            "memory_alert_threshold_mb": sup.memory_alert_threshold_mb,
            "memory_restart_threshold_mb": sup.memory_restart_threshold_mb,
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
# Self‑contained console page with Fleet & Monitor tabs.
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
  header { padding: 12px 20px; border-bottom: 1px solid #232a3a; display: flex; align-items: center;
           gap: 12px; flex-wrap: wrap; background: #131a29; }
  h1 { font-size: 16px; margin: 0; font-weight: 650; letter-spacing: .2px; flex: 0 0 auto; }
  .tabs { display: flex; gap: 4px; margin-left: 0; border-bottom: none; }
  .tab-btn { background: transparent; border: none; border-bottom: 2px solid transparent; 
             padding: 8px 14px; cursor: pointer; color: #8a93a6; font-weight: 500; font-size: 13px;
             transition: all 0.2s; }
  .tab-btn.active { border-bottom-color: #3b82f6; color: #e6e9ef; }
  .tab-btn:hover { color: #c5cdd8; }
  .pill { padding: 3px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .ok { background: #12351f; color: #6ee7a0; }
  .bad { background: #3a1620; color: #ff8098; }
  .muted { color: #8a93a6; }
  main { padding: 18px 20px; overflow-y: auto; height: calc(100vh - 52px); }
  .tab-content { display: none; }
  .tab-content.active { display: block; }
  .cards { display: flex; gap: 12px; flex-wrap: wrap; margin-bottom: 16px; }
  .card { background: #151c2c; border: 1px solid #232a3a; border-radius: 10px; padding: 12px 16px; min-width: 120px; }
  .card .n { font-size: 22px; font-weight: 700; }
  .card .l { font-size: 11px; text-transform: uppercase; letter-spacing: .6px; color: #8a93a6; }
  .card .sub { font-size: 11px; color: #8a93a6; margin-top: 4px; }
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
  .hdr-right { margin-left: auto; display: flex; align-items: center; gap: 8px; }
  .navbtn { display: inline-block; font-size: 12px; font-weight: 600; text-decoration: none;
            padding: 5px 10px; border-radius: 7px; border: 1px solid #313a4f;
            background: #1c2740; color: #dfe6f2; }
  .navbtn:hover { background: #24365c; }
  input, select { font: inherit; padding: 4px 8px; border-radius: 7px; border: 1px solid #313a4f;
                  background: #0d1320; color: #e6e9ef; }
  #msg { margin: 0 0 12px; min-height: 18px; font-size: 13px; }
  .err { color: #ff8098; } .good { color: #6ee7a0; }
  code { color: #9fb4ff; }
  h2 { font-size: 13px; color: #8a93a6; text-transform: uppercase; letter-spacing: .05em;
       margin: 18px 0 8px; font-weight: 600; }
  .panel { background: #151c2c; border: 1px solid #232a3a; border-radius: 10px; overflow: hidden; margin-bottom: 16px; }
  .legend { display: flex; gap: 16px; color: #8a93a6; font-size: 12px; margin: 6px 2px 0; }
  .legend b { display: inline-block; width: 10px; height: 10px; border-radius: 2px; margin-right: 5px; vertical-align: middle; }
  .rq { display: grid; grid-template-columns: 150px 46px 1fr 90px; gap: 10px; align-items: center;
        padding: 6px 12px; border-bottom: 1px solid #202737; font-size: 12px; }
  .rq:hover { background: #131a29; }
  .rq .lag { font-variant-numeric: tabular-nums; }
  .empty { padding: 24px; color: #8a93a6; text-align: center; }
  .bar { height: 12px; border-radius: 3px; background: #202737; overflow: hidden; display: flex; min-width: 120px; }
  .bar > span { display: block; height: 100%; }
  .seg-sql { background: #8b5cf6; } .seg-gen { background: #22c55e; } .seg-cache { background: #38bdf8; }
  .tname { font-weight: 600; }
  .warnrow td { background: rgba(239,68,68,.06); }
  .monitor-controls { display: flex; gap: 12px; margin-bottom: 16px; align-items: center; }
  .monitor-controls label { color: #8a93a6; font-size: 12px; }
</style>
</head>
<body>
<header>
  <h1>Fabric Shortcut Proxy · Manager</h1>
  <div class="tabs" id="tabs"></div>
  <span id="ready" class="pill muted">…</span>
  <span id="clock" class="muted" style="font-size:12px"></span>
  <div class="hdr-right">
    <a class="navbtn" href="/_config">Config UI</a>
    <label class="muted" id="toklabel" style="display:none">admin token
      <input id="token" type="password" size="16" placeholder="X-Admin-Token"/>
    </label>
    <label class="muted"><input id="auto" type="checkbox" checked/> auto‑refresh</label>
  </div>
</header>
<main>
  <!-- FLEET TAB -->
  <div id="fleetTab" class="tab-content active">
    <div class="cards" id="cards"></div>
    <p id="msg"></p>
    <div style="margin:0 0 12px">
      <button onclick="rollingRestart()">Rolling restart (one at a time)</button>
      <button class="stop" onclick="shutdownManager()">Shutdown Manager + all Agents</button>
    </div>
    <div style="overflow-x:auto">
      <table>
        <thead><tr>
          <th>Agent</th><th>State</th><th>PID</th><th>Port</th><th>Shard</th>
          <th>Restarts</th><th>Heartbeat</th><th>Memory (MB)</th><th>Serving</th><th>Actions</th>
        </tr></thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
  </div>

  <!-- MONITOR TAB -->
  <div id="monitorTab" class="tab-content">
    <div class="monitor-controls">
      <label>Refresh every
        <select id="interval">
          <option value="2000">2s</option>
          <option value="5000" selected>5s</option>
          <option value="10000">10s</option>
          <option value="30000">30s</option>
        </select>
      </label>
      <button onclick="monitorRefresh()">Refresh</button>
      <button onclick="monitorReset()">Reset stats</button>
    </div>
    <div class="cards" id="monitorCards"></div>
    <h2>Per-table read &amp; query statistics</h2>
    <div class="panel">
      <div style="overflow-x:auto">
        <table id="tbl">
          <thead><tr>
            <th>Table</th><th>Ver</th><th>Splits</th><th>Rows</th>
            <th>Requests</th><th id="thMeta">Meta</th><th id="thManifest">Manifest</th><th>Data</th><th>Errors</th>
            <th>Cache&nbsp;hit</th><th>Avg&nbsp;SQL</th><th>p95&nbsp;SQL</th>
            <th>Avg&nbsp;gen</th><th>Avg&nbsp;lag</th><th>p95&nbsp;lag</th><th>Bytes</th><th>Last&nbsp;read</th>
          </tr></thead>
          <tbody id="tblBody"></tbody>
        </table>
      </div>
    </div>
    <h2>Query lag — Fabric → SQL → Parquet → Fabric (avg per table)</h2>
    <div class="legend">
      <span><b class="seg-sql"></b>SQL execution</span>
      <span><b class="seg-gen"></b>Parquet generation</span>
      <span><b class="seg-cache"></b>served from cache (no SQL)</span>
    </div>
    <div class="panel">
      <div class="rq muted"><div>Table</div><div>Cache</div><div>Avg Fabric→SQL→Parquet lag</div><div>Avg total</div></div>
      <div id="recentBody"></div>
    </div>
  </div>
</main>

<script>
const $ = (id) => document.getElementById(id);
let tokenRequired = false;
let monitorTimer = null;
let fleetTimer = null;

// ====== TAB MANAGEMENT ======
function switchTab(tabName) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.getElementById(tabName + 'Tab').classList.add('active');
  document.getElementById(tabName + 'Btn').classList.add('active');
  
  if (tabName === 'monitor') {
    scheduleMonitor();
    monitorRefresh();
  } else if (monitorTimer) {
    clearInterval(monitorTimer);
  }
}

const tabs = [{ id: 'fleet', label: 'Fleet' }, { id: 'monitor', label: 'Monitor' }];
$("tabs").innerHTML = tabs.map(t => 
  `<button class="tab-btn${t.id === 'fleet' ? ' active' : ''}" id="${t.id}Btn" onclick="switchTab('${t.id}')">${t.label}</button>`
).join('');

// ====== FLEET FUNCTIONS ======
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

  // Aggregate memory stats
  const totalMem = d.agents.reduce((s, a) => s + (a.rss_mb || 0), 0);
  const avgMem = d.agents.length > 0 ? totalMem / d.agents.length : 0;
  const peakMem = Math.max(...d.agents.map(a => a.peak_rss_mb || 0));
  const maxThreshold = Math.max(...d.agents.map(a => a.memory_restart_threshold_mb || 0));

  const cards = [
    ["Agents alive", d.agents_alive + " / " + d.agents_total],
    ["Registered", d.agents_registered_alive + " / " + d.agents_registered],
    ["Gateway", d.gateway_enabled ? (d.gateway_targets + " targets") : "off"],
    ["Fleet memory", totalMem.toFixed(0) + " MB", "avg " + avgMem.toFixed(1) + " MB"],
    ["Peak memory", peakMem.toFixed(1) + " MB", maxThreshold > 0 ? "limit " + maxThreshold + " MB" : ""],
  ];
  $("cards").innerHTML = cards.map(c =>
    `<div class="card"><div class="n">${c[1]}</div><div class="l">${c[0]}</div>${c[2]?`<div class="sub">${c[2]}</div>`:"" }</div>`).join("");

  $("rows").innerHTML = d.agents.map(a => {
    const serving = (a.serving_tables && a.serving_tables.length)
      ? a.serving_tables.map(t => `${t}@${a.epochs[t] ?? "?"}`).join(", ")
      : '<span class="muted">—</span>';
    const hb = (a.heartbeat_age == null) ? '<span class="muted">—</span>' : a.heartbeat_age + "s";
    const memClass = a.memory_restart_threshold_mb > 0 && a.rss_mb >= a.memory_restart_threshold_mb 
      ? 'err' : (a.memory_alert_threshold_mb > 0 && a.rss_mb >= a.memory_alert_threshold_mb ? 'muted' : '');
    const memText = `<span class="${memClass}">${a.rss_mb.toFixed(1)} / ${a.peak_rss_mb.toFixed(1)}</span><span class="muted"> avg ${a.avg_rss_mb.toFixed(1)}</span>`;
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
      <td>${memText}</td>
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
    else { msg(d.note || "shutting down…", "good"); if (fleetTimer) clearInterval(fleetTimer); }
  } catch (e) { msg(`shutdown error: ${e}`, "err"); }
}

$("auto").addEventListener("change", loop);
function loop() {
  if (fleetTimer) { clearInterval(fleetTimer); fleetTimer = null; }
  if ($("auto").checked && document.getElementById('fleetTab').classList.contains('active')) 
    fleetTimer = setInterval(refresh, 2000);
}

// ====== MONITOR FUNCTIONS ======
function fmtBytes(n){ if(n==null) return "–"; const u=["B","KB","MB","GB"]; let i=0,x=n;
  while(x>=1024&&i<u.length-1){x/=1024;i++;} return x.toFixed(x<10&&i>0?1:0)+u[i]; }
function fmtNum(n){ return n==null? "–" : Intl.NumberFormat().format(n); }
function fmtMs(n){ if(n==null) return "–"; return n>=1000? (n/1000).toFixed(2)+"s" : Math.round(n)+"ms"; }
function ago(ts){ if(!ts) return "–"; const s=Math.max(0,Date.now()/1000-ts);
  if(s<60) return Math.round(s)+"s ago"; if(s<3600) return Math.round(s/60)+"m ago"; return Math.round(s/3600)+"h ago"; }
function pct(r){ return r==null? "–" : Math.round(r*100)+"%"; }

async function monitorRefresh(){
  try{
    const r = await fetch("/_monitor/api/summary", {cache:"no-store"});
    if(!r.ok) throw new Error("HTTP "+r.status);
    const d = await r.json();
    monitorRender(d);
  }catch(e){
    console.error("Monitor error:", e);
  }
}

function monitorRender(d){
  const fmt = (d.table_format||'iceberg');
  const isDelta = fmt==='delta';
  $("thMeta").textContent = isDelta ? "Δ log" : "Meta";
  $("thManifest").textContent = isDelta ? "—" : "Manifest";
  
  const t = d.totals||{}, c = d.cache||{};
  const pinned = c.parquet_pinned||{};
  $("monitorCards").innerHTML = [
    ["Tables", fmtNum(t.tables)],
    ["Data requests", fmtNum(t.data_requests), "cache hit "+pct(t.cache_hit_ratio)],
    ["Parquet gens", fmtNum(t.parquet_generations)],
    ["Bytes served", fmtBytes(t.bytes_served)],
    ["Pinned splits", fmtNum(pinned.entries)+" · "+fmtBytes(pinned.bytes)],
    ["SQL p (avg)", d.sql_latency? fmtMs((d.sql_latency.avg_seconds||0)*1000):"–", d.sql_latency? fmtNum(d.sql_latency.count)+" queries":""],
    ["Source errors", fmtNum(t.source_unavailable), (t.source_unavailable>0?"":"none")],
  ].map(([k,v,sub])=>`<div class="card"><div class="l">${k}</div><div class="n">${v}</div>${sub?`<div class="sub">${sub}</div>`:""}</div>`).join("");

  const rows = (d.tables||[]);
  $("tblBody").innerHTML = rows.length ? rows.map(x=>{
    const warn = !isDelta && (x.metadata_reads|0)>=3 && (x.data_reads|0)===0 && (x.manifest_reads|0)===0;
    const probes = (x.probe_404s|0)>0 ? ` <span class="muted" title="expected probes">(${fmtNum(x.probe_404s)}p)</span>` : '';
    const metaCol = isDelta ? fmtNum(x.delta_log_reads) : fmtNum(x.metadata_reads);
    const manifestCol = isDelta ? '<span class="muted">–</span>' : fmtNum(x.manifest_reads);
    return `<tr class="${warn?'warnrow':''}">
      <td class="tname">${x.table}${warn?' <span class="err">⚠</span>':''}</td>
      <td>${x.version??'–'}</td><td>${fmtNum(x.splits)}</td><td>${fmtNum(x.total_records)}</td>
      <td>${fmtNum(x.requests)}</td><td>${metaCol}</td><td>${manifestCol}</td>
      <td>${fmtNum(x.data_reads)}</td><td class="${(x.errors|0)>0?'err':''}">${fmtNum(x.errors)}${probes}</td>
      <td>${pct(x.cache_hit_ratio)}</td><td>${fmtMs(x.avg_sql_ms)}</td><td>${fmtMs(x.p95_sql_ms)}</td>
      <td>${fmtMs(x.avg_gen_ms)}</td><td>${fmtMs(x.avg_total_ms)}</td><td>${fmtMs(x.p95_total_ms)}</td>
      <td>${fmtBytes(x.bytes_served)}</td><td class="muted">${ago(x.last_read_ts)}</td>
    </tr>`;
  }).join("") : `<tr><td colspan="17" class="empty">No table activity yet.</td></tr>`;

  const qtabs = (d.tables||[]).filter(t=>(t.data_requests|0)>0);
  const maxLag = Math.max(1, ...qtabs.map(t=>t.avg_total_ms||0));
  $("recentBody").innerHTML = qtabs.length ? qtabs.map(t=>{
    const sqlW = Math.round(100*(t.avg_sql_ms||0)/maxLag);
    const genW = Math.round(100*(t.avg_gen_ms||0)/maxLag);
    const allCache = (t.avg_sql_ms||0)===0 && (t.avg_gen_ms||0)===0;
    const seg = allCache
      ? `<span class="seg-cache" style="width:${Math.max(4,Math.round(100*(t.avg_total_ms||0)/maxLag))}%"></span>`
      : `<span class="seg-sql" style="width:${sqlW}%" title="avg SQL ${fmtMs(t.avg_sql_ms)}"></span><span class="seg-gen" style="width:${genW}%" title="avg gen ${fmtMs(t.avg_gen_ms)}"></span>`;
    return `<div class="rq">
      <div class="tname">${t.table}</div>
      <div class="muted">${pct(t.cache_hit_ratio)}</div>
      <div><div class="bar">${seg}</div></div>
      <div class="lag">${fmtMs(t.avg_total_ms)} <span class="muted">${t.data_requests}×</span></div>
    </div>`;
  }).join("") : `<div class="empty">No data requests captured yet.</div>`;
}

async function monitorReset() {
  try {
    await fetch("/_monitor/api/reset", {method:"POST"});
    monitorRefresh();
  } catch (e) {
    console.error("Reset error:", e);
  }
}

function scheduleMonitor(){
  if(monitorTimer) clearInterval(monitorTimer);
  if($("auto").checked) monitorTimer = setInterval(monitorRefresh, parseInt($("interval").value));
}
$("interval").onchange = scheduleMonitor;

// Initial load
refresh(); loop();
</script>
</body>
</html>
"""
