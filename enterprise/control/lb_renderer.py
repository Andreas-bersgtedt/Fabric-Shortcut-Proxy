"""
Tier 2 nginx backend renderer (external load balancer, Option A).

Polls the Manager's ``GET /agents``, keeps the agents that are alive (not in the
Manager's ``dead`` set) AND currently ready (``GET /readyz`` == 200, so draining
agents are dropped promptly), renders an nginx ``upstream`` block from their
routable ``host:port`` (the advertised address, see ``AGENT_ADVERTISE_HOST``), and
on change writes the include file, validates with ``nginx -t``, and reloads nginx.

The active ``/readyz`` probe is what makes this reliable on open-source nginx,
which has no built-in active health checks: the Manager registry + this probe are
the authority on which backends are in rotation.

Run it as a small sidecar next to nginx:

    python -m enterprise.control.lb_renderer --manager-url http://manager:9200 \\
        --out /etc/nginx/conf.d/fsp_upstream.conf \\
        --nginx-test-cmd "nginx -t" --reload-cmd "nginx -s reload"
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from typing import Callable

from observability.logging import get_logger

log = get_logger(__name__)

DEFAULT_UPSTREAM = "fsp_agents"


def render_upstream(backends: list[tuple[str, int]], name: str = DEFAULT_UPSTREAM) -> str:
    """Render an nginx ``upstream`` block from ``(host, port)`` backends.

    With no ready backends, emit a single ``down`` placeholder so the config stays
    valid (nginx requires at least one ``server``); requests then fail fast (502).
    """
    lines = [f"upstream {name} {{"]
    if backends:
        for host, port in backends:
            lines.append(f"    server {host}:{port} max_fails=2 fail_timeout=5s;")
    else:
        lines.append("    server 127.0.0.1:1 down;  # no ready agents")
    lines.append("}")
    return "\n".join(lines) + "\n"


def select_backends(payload: dict, is_ready: Callable[[str, int], bool]) -> list[tuple[str, int]]:
    """From a ``/agents`` payload keep alive + ready agents as sorted ``(host, port)``."""
    dead = set(payload.get("dead") or [])
    seen: set[tuple[str, int]] = set()
    for a in payload.get("agents") or []:
        if a.get("agent_id") in dead:
            continue
        host = str(a.get("host") or "").strip()
        try:
            port = int(a.get("port"))
        except (TypeError, ValueError):
            continue
        if not host or port <= 0:
            continue
        if not is_ready(host, port):
            continue
        seen.add((host, port))
    return sorted(seen)


def _http_get(url: str, timeout: float) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, method="GET"), timeout=timeout) as resp:
            return int(getattr(resp, "status", 200)), resp.read()
    except urllib.error.HTTPError as e:  # a real HTTP status (e.g. 503) is useful signal
        return int(e.code), b""
    except Exception:
        return 0, b""


def fetch_agents(manager_url: str, timeout: float = 3.0) -> dict:
    """GET the Manager's /agents payload; empty fleet on any error."""
    status, body = _http_get(manager_url.rstrip("/") + "/agents", timeout)
    if status != 200 or not body:
        return {"agents": [], "dead": []}
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return {"agents": [], "dead": []}


def make_ready_probe(timeout: float = 1.5, scheme: str = "http") -> Callable[[str, int], bool]:
    def _probe(host: str, port: int) -> bool:
        status, _ = _http_get(f"{scheme}://{host}:{port}/readyz", timeout)
        return status == 200
    return _probe


def write_if_changed(path: str, content: str) -> bool:
    """Atomically write ``content`` to ``path`` when it differs. True if written."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            if f.read() == content:
                return False
    except FileNotFoundError:
        pass
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return True


def _run(cmd: str) -> int:
    if not cmd:
        return 0
    try:
        return subprocess.run(shlex.split(cmd), check=False).returncode
    except Exception as exc:  # noqa: BLE001 - a failed reload must not crash the daemon
        log.warning("lb_renderer_cmd_failed", cmd=cmd, error=str(exc))
        return 1


def render_once(manager_url: str, out_path: str, upstream_name: str,
                ready_probe: Callable[[str, int], bool],
                nginx_test_cmd: str, reload_cmd: str) -> bool:
    """One poll -> render -> (validate -> reload) pass. True when nginx was reloaded."""
    payload = fetch_agents(manager_url)
    backends = select_backends(payload, ready_probe)
    conf = render_upstream(backends, upstream_name)

    try:
        with open(out_path, "r", encoding="utf-8") as f:
            prev = f.read()
    except FileNotFoundError:
        prev = None
    if conf == prev:
        return False

    write_if_changed(out_path, conf)
    if nginx_test_cmd and _run(nginx_test_cmd) != 0:
        log.error("lb_renderer_nginx_test_failed", out=out_path, backends=len(backends))
        if prev is not None:  # roll back so a later reload does not load a bad include
            write_if_changed(out_path, prev)
        return False
    if reload_cmd:
        _run(reload_cmd)
    log.info("lb_renderer_reloaded", backends=len(backends),
             servers=[f"{h}:{p}" for h, p in backends])
    return True


def run(args: argparse.Namespace) -> int:
    probe = make_ready_probe(args.readyz_timeout, args.scheme)
    if args.once:
        render_once(args.manager_url, args.out, args.upstream_name, probe,
                    args.nginx_test_cmd, args.reload_cmd)
        return 0
    log.info("lb_renderer_started", manager=args.manager_url, out=args.out, interval=args.interval)
    while True:
        try:
            render_once(args.manager_url, args.out, args.upstream_name, probe,
                        args.nginx_test_cmd, args.reload_cmd)
        except Exception as exc:  # noqa: BLE001 - keep the daemon alive across blips
            log.warning("lb_renderer_tick_error", error=str(exc))
        time.sleep(args.interval)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Tier 2 nginx backend renderer for the FSP agent fleet.")
    p.add_argument("--manager-url", required=True, help="Manager control URL (serves GET /agents)")
    p.add_argument("--out", required=True, help="nginx upstream include file to render")
    p.add_argument("--upstream-name", default=DEFAULT_UPSTREAM)
    p.add_argument("--interval", type=float, default=5.0, help="poll interval (s)")
    p.add_argument("--readyz-timeout", type=float, default=1.5, help="per-agent /readyz probe timeout (s)")
    p.add_argument("--scheme", default="http", choices=["http", "https"], help="agent readiness scheme")
    p.add_argument("--nginx-test-cmd", default="nginx -t")
    p.add_argument("--reload-cmd", default="nginx -s reload")
    p.add_argument("--once", action="store_true", help="render a single pass and exit")
    return run(p.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
