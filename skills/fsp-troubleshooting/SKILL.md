---
name: fsp-troubleshooting
description: "Troubleshoot Fabric Shortcut Proxy startup, Manager/Agent readiness, Python environments, database drivers, private networking, TLS, SigV4 authentication, mounts, credentials, stale code, and S3 or Parquet data-path failures. Use when health checks fail or requests return errors."
argument-hint: "Provide the symptom, platform, endpoint, recent change, and relevant redacted log lines."
---

# Fabric Shortcut Proxy Troubleshooting

## Triage Order

1. Record platform, launch command, current Git commit/image, effective config location, and the exact failing endpoint/request.
2. Check process liveness before readiness:

```bash
curl -i http://127.0.0.1:9000/healthz
curl -i http://127.0.0.1:9000/readyz
```

3. Check the correct logs for the runtime. On Linux systemd:

```bash
systemctl status fabric-shortcut-proxy.service
sudo journalctl -u fabric-shortcut-proxy.service --no-pager -o cat -n 200
pgrep -af 'enterprise.manager|main.py'
git -C /opt/fabric-shortcut-proxy log --oneline -1
```

On Windows, inspect the Manager terminal/service log and confirm the `.venv` path. In AKS:

```bash
kubectl -n fabric-shortcut-proxy get pods,svc,endpointslices
kubectl -n fabric-shortcut-proxy logs deployment/fsp-manager --tail=200
```

4. Test one layer at a time: process, source DB, snapshot/artifact store, S3 auth, then Fabric/network path.

## Common Startup Failures

### Wrong or incomplete environment

Environment variables override JSON files. Inspect effective non-secret values from the running service, especially `DB_URL` presence, `TABLE_FORMAT`, `MATERIALIZE_MODE`, `AUTO_REFRESH`, ports, and `FSP_CONFIG_DIR`. Do not print passwords or tokens.

### `MATERIALIZE_MODE=virtual` with `AUTO_REFRESH=1`

This combination is rejected intentionally. Use `MATERIALIZE_MODE=eager` with refresh, or disable `AUTO_REFRESH` for virtual mode. Restart after changing the source that wins precedence.

### Missing Python dependency or wrong venv

```bash
pgrep -af enterprise.manager
/path/to/the/actual/.venv/bin/python -m pip show cryptography asyncpg aioodbc
```

Install extras into the interpreter the service actually runs. Recreating a different development venv does not repair a systemd or service-user venv.

### SQL Server ODBC errors

Confirm ODBC Driver 18 and unixODBC on Linux:

```bash
odbcinst -q -d
ldconfig -p | grep libodbc
```

On Windows, confirm the driver is installed and the connection string uses the driver name correctly. The Python `aioodbc` package alone is not sufficient.

### Credential-store encryption errors

Install `cryptography` in the active venv, keep `FSP_CRED_KEY` stable, and verify credential-store file permissions. A changed Fernet key makes existing encrypted entries unreadable.

## Network and Endpoint Diagnosis

- If `/healthz` fails, inspect process startup, bind host, port collision, and logs.
- If `/healthz` works but `/readyz` is `503`, inspect source DB reachability, schema reflection, snapshot/materialization errors, or missing ready Agents.
- If localhost works but a private endpoint fails, test DNS resolution, route, NSG/firewall, service endpoints, internal LoadBalancer health probes, and TLS termination.
- In AKS, check Service selectors and EndpointSlices before changing application code. Never point production DNS at a pod IP.
- Manager `/healthz` is process health; Manager `/readyz` can be fleet readiness and may be non-200 with zero registered Agents.

## S3, Auth, and Data-Path Failures

1. Reproduce with a single `HEAD` or `GET` and capture HTTP status, bucket, key, and redacted headers.
2. For `403`, verify SigV4 clock skew, access key selection, bucket/prefix ACL, mount auth enforcement, and whether the request reached the intended endpoint.
3. For `404`, verify bucket routing, canonical versus legacy object path layout, table registration, and snapshot metadata.
4. For range or large reads, check byte-range handling and upstream object-store permissions.
5. For database or Parquet errors, inspect SQL dialect, reflected schema, key column, split planning, and source-side permissions.

## Change Discipline

After each corrective change, rerun the smallest failing check, then `/healthz`, `/readyz`, and one representative object read. Record the result and the effective configuration source. Do not delete artifacts or rotate encryption keys as a first response.

## References

- [Linux troubleshooting guide](../../docs/LINUX_MANAGER_TROUBLESHOOTING.md)
- [Operations manual](../../docs/manual/08-operations.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Security](../../docs/SECURITY.md)
- [Enterprise deployment guide](../../docs/Enterprise_Deployment_guide.md)