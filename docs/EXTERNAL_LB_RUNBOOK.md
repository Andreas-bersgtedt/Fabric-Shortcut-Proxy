# External Load Balancer Runbook (nginx, Tier 2 discovery)

How to run the Fabric Shortcut Proxy agent fleet behind an external nginx load
balancer, with the Manager as a control plane only. This is "Option A": the LB
serves the data plane directly (LB to agent), so Python stays out of the byte path
and the C++ agent's throughput is preserved. Backend discovery is "Tier 2": a small
renderer keeps the nginx `upstream` in sync with the live fleet.

See [architecture-distributed.excalidraw](architecture-distributed.excalidraw) for
the topology and [architecture-overview.excalidraw](architecture-overview.excalidraw)
for the component view.

## Topology

```
Fabric (S3 client) --> nginx LB (TLS) --> agent fleet (private subnet)
                          ^                    |
   enterprise/control/lb_renderer ---+   register/heartbeat/drain, GET /agents
        (polls GET /agents)         |
                                 Manager (control only, ENABLE_GATEWAY=0)

materializer worker --> shared object store --> agents serve from the store
```

## 1. Manager (control node)

Run the Manager control-only so nothing is served through the built-in gateway:

- `ENABLE_GATEWAY=0` (default). Start with `python -m enterprise.manager` (or `Manager.ps1` /
  `Manager.sh`).
- It exposes `GET /agents` (`{"agents": [...], "dead": [...]}`), which the renderer
  consumes. Keep the control port on a private/control network, not public.

## 2. Agents (the fleet)

Set per agent (env vars or `config.system.json`):

| Setting | Purpose |
|---|---|
| `HOST` | Bind to the private NIC, not a public `0.0.0.0` if the host is public. |
| `AGENT_ADVERTISE_HOST` | Routable IP or DNS the LB dials. Required for a multi-host fleet. |
| `MANAGER_URL` | e.g. `http://manager:9200` (register + heartbeat). |
| `AGENT_ID` | Stable id (else auto from host:port). |
| `AGENT_DRAIN_GRACE_SECONDS` | Default 15; keep it above the LB/renderer probe interval. |
| `FORWARDED_ALLOW_IPS` | The LB address/CIDR, so audit logs the real client IP. |
| `REQUIRE_SIGV4` | `1` on Python agents that must enforce request signatures. |

Both agent flavors register and heartbeat: the Python agent (`main.py`) and the
zero-dependency C++ agent (`agent-cpp/agent.exe`, same env vars). See the security
checklist for the C++ SigV4 caveat.

## 3. nginx

- Install [deploy/nginx/fsp.conf](../deploy/nginx/fsp.conf) into `/etc/nginx/conf.d/`.
- Put the TLS cert and key at the paths in the config.
- Seed the include file so `nginx -t` passes before the renderer runs:
  `cp deploy/nginx/upstream.conf.example /etc/nginx/conf.d/fsp_upstream.conf`.
- `nginx -t && systemctl reload nginx`.

## 4. Renderer (co-located with nginx)

Run [enterprise/control/lb_renderer.py](../enterprise/control/lb_renderer.py) as a service. It polls
`GET /agents`, keeps agents that are alive and answer `/readyz` 200, renders the
`upstream`, and reloads nginx on change (validating with `nginx -t` first).

```ini
# /etc/systemd/system/fsp-lb-renderer.service
[Unit]
Description=FSP nginx upstream renderer
After=network-online.target nginx.service

[Service]
ExecStart=/usr/bin/python3 -m enterprise.control.lb_renderer \
  --manager-url http://manager:9200 \
  --out /etc/nginx/conf.d/fsp_upstream.conf \
  --nginx-test-cmd "nginx -t" --reload-cmd "nginx -s reload" \
  --interval 5
WorkingDirectory=/opt/fabric-shortcut-proxy
Restart=always

[Install]
WantedBy=multi-user.target
```

Use `--scheme https` if agents terminate TLS on their own port.

## 5. Fabric

Point the OneLake shortcut at `https://<lb-host>`. Bucket and object keys are
unchanged from a single-agent deployment.

## Operations

### Add an agent (scale up)

Start a new agent with `MANAGER_URL` and a routable `AGENT_ADVERTISE_HOST`. It
registers, and the renderer adds it on the next poll once `/readyz` returns 200.
No LB edit is needed.

### Drain an agent (maintenance or scale down)

```
POST /_manager/api/agents/{agent_id}/drain      (admin token)
```

Sequence:
1. The agent flips `/readyz` to 503 (liveness `/healthz` stays 200).
2. The renderer's next probe drops it from the `upstream` and reloads nginx.
3. After `AGENT_DRAIN_GRACE_SECONDS` the agent exits, in-flight requests finished.

Verify: `curl http://<agent>:<port>/readyz` returns 503; the renderer log shows
`lb_renderer_reloaded` with the agent gone.

### Rolling upgrade

For each agent, one at a time:
1. Drain it (above) and wait until it is out of rotation (`/readyz` 503 or the
   renderer log).
2. Stop, upgrade, restart.
3. Wait for `/readyz` 200; the renderer re-adds it.

Readiness gates re-entry, so no request reaches a not-ready agent during the roll.

## Verification

- `curl -k https://<lb>/healthz` and `/readyz` through the LB reach an agent.
- `curl http://<manager>:9200/agents` lists agents with a routable `host`.
- Renderer log: `lb_renderer_reloaded backends=N`.
- A 502 with the `down` placeholder in `fsp_upstream.conf` means no agent is ready.

## Security checklist

- Agents on a private subnet; only the LB is public.
- TLS terminates at the LB; rotate the cert/key.
- `REQUIRE_SIGV4=1` on Python agents where request signatures must be enforced.
- `FORWARDED_ALLOW_IPS` set to the LB so audit logs the real client, not the LB.
- The LB blocks `/_manager`, `/_config`, `/_monitor`, `/control`, `/agents`.
- C++ agent caveat: `agent.cpp` does not verify SigV4, and nginx does not either.
  A C++-agent fleet relies on network isolation plus the LB for access control. Use
  Python agents (`REQUIRE_SIGV4=1`) where per-request signature enforcement is needed.
- Optional: nginx rate limiting (`limit_req`) and an LB to agent TLS hop (agent TLS
  plus `proxy_pass https://` plus renderer `--scheme https`).

## Troubleshooting

- Agent never enters rotation: confirm `/readyz` is 200, `AGENT_ADVERTISE_HOST` is
  routable from the LB host, the renderer can reach the agent, and the agent shows
  in `GET /agents` and not in `dead`.
- Everything returns 502: no ready agents (the `down` placeholder is active). Check
  the fleet `/readyz` and the Manager.
- Wrong client IP in audit logs: set `FORWARDED_ALLOW_IPS` to the LB address.
- nginx not reloading: check the renderer `--reload-cmd` / `--nginx-test-cmd`, and
  the log for `lb_renderer_nginx_test_failed` (a bad include is rolled back).
- Agents keep exiting with code 78 (`agent_config_error`) and the Manager does not
  restart them: this is a source-database failure at startup, not a crash. The
  supervisor holds the Agent on purpose (restarting would just loop). SQL Server
  `Login failed for user '<user>' (18456)` means the server rejected the
  credentials. Named sources keep their password in the encrypted credential store
  and it is hydrated as `DB_URL_<ID>` at Manager startup; if the store has no
  password (or a stale one) the Agent connects without one and is rejected. Fix:
  set the password in the config builder (`/_config`), or export the full
  password-bearing URL as `DB_URL_<ID>` (id uppercased, non-alphanumerics become
  `_`, e.g. `SalesLT` -> `DB_URL_SALESLT`), then restart the Manager so it
  re-hydrates and respawns the Agents. Also confirm the account is enabled and the
  database firewall allows the Agent host.
