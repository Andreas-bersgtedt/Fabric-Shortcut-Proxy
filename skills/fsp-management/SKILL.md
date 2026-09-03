---
name: fsp-management
description: "Operate and administer the Fabric Shortcut Proxy Manager and Agent fleet. Use for starting, stopping, restarting, draining, scaling, monitoring, health checks, gateway routing, retention GC, HA, and controlled rollouts."
argument-hint: "Describe the operational action, deployment mode, and affected Manager or Agent."
---

# Fabric Shortcut Proxy Management

## Use When

- Starting or stopping the Manager/Agent runtime.
- Checking health, readiness, metrics, traces, or fleet membership.
- Scaling Agents or changing gateway routing.
- Draining an Agent for maintenance.
- Managing HA, retention GC, or a rollout.

## Identify the Control Plane

The Manager control plane normally listens on `127.0.0.1:9200`; the Agent data plane normally listens on `0.0.0.0:9000`. With `-Gateway`, Fabric uses the Manager control port as the S3 gateway. Do not send Fabric traffic to the administrative endpoint unless gateway mode is intentionally enabled.

## Start and Stop

```powershell
.\Manager.ps1 -SkipInstall
```

```bash
bash ./Manager.sh --skip-install
```

Use `-AgentCount` or `--agent-count` for a supervised local fleet. Preserve the existing configuration directory and artifact store when restarting. The Manager supervises local Agents and restarts a crashed Agent; distinguish that from a readiness failure.

## Health and Fleet Checks

```powershell
Invoke-RestMethod http://127.0.0.1:9200/healthz
Invoke-RestMethod http://127.0.0.1:9200/readyz
Invoke-RestMethod http://127.0.0.1:9200/metrics
```

```bash
curl -s http://127.0.0.1:9200/healthz
curl -s http://127.0.0.1:9200/readyz
curl -s http://127.0.0.1:9200/metrics
```

Interpret the endpoints separately:

- `/healthz` means the process is alive.
- `/readyz` means the required snapshot/source state or fleet state is ready; a `503` is actionable, not proof that the process is down.
- `/_admin` and Manager UI routes expose operational state and may require `ADMIN_TOKEN` or Manager Basic Auth.

For AKS, inspect the whole control path:

```bash
kubectl -n fabric-shortcut-proxy get pods,svc,pvc
kubectl -n fabric-shortcut-proxy get endpointslices
kubectl -n fabric-shortcut-proxy logs deployment/fsp-manager --tail=200
kubectl -n fabric-shortcut-proxy describe pod <pod-name>
```

## Safe Maintenance

1. Confirm the target Agent and current readiness.
2. Drain it using the Manager control operation or the documented admin endpoint.
3. Wait until it is out of rotation and existing requests finish.
4. Restart, replace, or inspect it.
5. Confirm `/readyz` returns `200` and the gateway includes it again before declaring maintenance complete.

For HA, use the same shared artifact store and distinct control ports for the warm standby. Only the lease holder should supervise Agents and serve the gateway.

## Operational Guardrails

- Use `/healthz` for liveness probes and `/readyz` for readiness probes according to the deployment topology.
- Protect Manager UI and mutating admin operations; do not expose them directly to Fabric clients.
- Treat the artifact store as persistent state. Avoid cleanup during routine restarts.
- Capture commit/image, effective config source, logs, health responses, and object-read results during changes.

## References

- [Operations manual](../../docs/manual/08-operations.md)
- [External load balancer runbook](../../docs/EXTERNAL_LB_RUNBOOK.md)
- [Scale architecture plan](../../docs/SCALE_ARCHITECTURE_PLAN.md)
- [Enterprise deployment guide](../../docs/Enterprise_Deployment_guide.md)