---
name: fsp-rapid-poc
description: "Rapidly deploy a Fabric Shortcut Proxy proof of concept. Use for a quick Lite demo, local kind proof, or AKS validation deployment with SQLite, PostgreSQL, a real source database, S3-compatible reads, Delta/Iceberg output, and basic health verification."
argument-hint: "Describe the target: Lite host, local kind, or AKS; source type; and the POC success criterion."
---

# Fabric Shortcut Proxy Rapid POC

## Purpose

Turn a clean checkout into a working proof with the fewest moving parts. Choose the smallest
target that answers the question:

| Goal | Path |
| --- | --- |
| Prove the proxy and S3 read path on one machine | Lite |
| Prove the multi-worker Kubernetes image and artifact flow | Local `kind` |
| Prove the private AKS Service, source, and Fabric network path | AKS validation |

This skill is for evaluation and integration discovery. It does not establish production HA,
backup, TLS, capacity, public ingress, or a security review.

## Guardrails

1. Use disposable names, credentials, databases, and namespaces for a POC.
2. Keep passwords, connection strings, SAS tokens, private IPs, and tenant identifiers out of Git.
3. Prefer `TABLE_FORMAT=delta` when the POC target is Fabric reading `_delta_log`; use `iceberg`
   when validating Iceberg metadata compatibility.
4. Use read-only source credentials.
5. Do not expose Manager port `9200` or a POC Agent publicly.
6. For AKS, use a private DNS hostname backed by the internal Agent LoadBalancer, not a Pod IP.

## Path A: Fastest Lite POC

Use this when a single Windows, Linux, or macOS host can reach the source. SQLite is automatic
when `DB_URL` is omitted, and the application seeds a disposable demo table.

### Windows

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
python main.py
```

### Linux or macOS

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
python main.py
```

Verify in a second terminal:

```bash
curl -fsS http://127.0.0.1:9000/healthz
curl -fsS http://127.0.0.1:9000/readyz
```

For a real source, install the matching extra and set the table inputs before starting:

```powershell
python -m pip install -e ".[postgres]"
$env:DB_URL = "postgresql+asyncpg://<user>:<password>@<host>:5432/<database>"
$env:DB_SOURCE_TABLE = "public.<table>"
$env:KEY_COLUMN = "<key-column>"
python main.py
```

Use the equivalent `export` syntax on Linux/macOS. Never paste a real password into a committed
file or a shared terminal transcript.

## Path B: Local Kubernetes POC

Use the repository's `kind` overlay when Docker and `kind` are available and the POC needs the
multi-worker materializer, shared artifact volume, PostgreSQL source, and smoke client.

```powershell
docker build --build-arg FSP_ENTERPRISE=1 -f Dockerfile.python -t fabric-shortcut-proxy-python:dev .
docker build -f agent-cpp/Dockerfile -t fabric-shortcut-proxy-cpp:dev .
kind create cluster --config deploy/kubernetes/overlays/kind/cluster.yaml
kind load docker-image fabric-shortcut-proxy-python:dev fabric-shortcut-proxy-cpp:dev --name fsp-proof
kubectl apply -k deploy/kubernetes/overlays/kind
kubectl -n fabric-shortcut-proxy get pods,pvc,service,hpa
```

Wait for the materializers and serving replicas. Then run the checked-in smoke client:

```powershell
kubectl apply -f deploy/kubernetes/examples/smoke-client.yaml
kubectl -n fabric-shortcut-proxy exec fsp-smoke-client -- curl -fsS http://shortcut-proxy/healthz
kubectl -n fabric-shortcut-proxy exec fsp-smoke-client -- curl -fsS http://shortcut-proxy/readyz
```

Use `kubectl -n fabric-shortcut-proxy logs fsp-materializer-0` when readiness waits on snapshot
publication. This path uses disposable local storage and is not an AKS durability test.

## Path C: AKS Validation POC

Use this when the cluster and private network already exist. Complete the
[infrastructure prerequisites](../fsp-infrastructure-prerequisites/SKILL.md) first.

1. Confirm AKS is running and the source Secret, `fsp-common` ConfigMap, artifact PVC, Manager,
   and intended Agent workload are present.
2. Render the overlay before applying it:

```bash
kubectl kustomize deploy/kubernetes/overlays/aks-validation
```

3. Apply the validation overlay. It adds `fsp-materializer-internal`, an Azure internal
   LoadBalancer on port `9000`, while keeping the headless StatefulSet Service.

```bash
kubectl apply -k deploy/kubernetes/overlays/aks-validation
kubectl -n fabric-shortcut-proxy get pods,svc,pvc
kubectl -n fabric-shortcut-proxy get svc fsp-materializer-internal -o wide
kubectl -n fabric-shortcut-proxy get endpointslice \
  -l kubernetes.io/service-name=fsp-materializer-internal -o wide
```

4. Create or update the private DNS A record to the Service `EXTERNAL-IP`. Use a hostname such
   as `agent-poc.<private-zone>` for the Fabric shortcut or OPDG, with port `9000`.
5. Verify from the OPDG, jump box, or other client network, not only from the operator laptop:

```powershell
Resolve-DnsName agent-poc.<private-zone>
Test-NetConnection agent-poc.<private-zone> -Port 9000
```

6. Test `/healthz`, `/readyz`, and one authenticated `HEAD` or `GET` for a known object. A
   `200` health response does not prove the source snapshot or S3 authorization is correct.

## Success Criteria

Declare the POC successful only when the stated path passes its relevant checks:

- Process responds on `/healthz`.
- Source-backed mode responds on `/readyz`.
- Kubernetes mode has ready Agent endpoints and no repeated container restarts.
- The intended private hostname resolves to the current LoadBalancer frontend.
- A representative authenticated S3 object request succeeds.
- Logs show the expected source dialect, table, snapshot, and output format.

## Cleanup

Lite: stop the process and remove the disposable virtual environment/database if no longer needed.

Kind:

```powershell
kubectl delete -k deploy/kubernetes/overlays/kind
kind delete cluster --name fsp-proof
```

AKS: remove only the POC namespace/workloads and DNS record after recording results. Do not delete
shared production PVCs, artifact storage, Key Vault secrets, or the AKS cluster as POC cleanup.

## Next Step

Move a successful POC to the [deployment](../fsp-deployment/SKILL.md),
[configuration](../fsp-configuration/SKILL.md), and [management](../fsp-management/SKILL.md)
skills before sharing it with users or connecting production Fabric workloads.

## References

- [Installation manual](../../docs/manual/04-installation.md)
- [Kubernetes proof](../../deploy/kubernetes/README.md)
- [Connectivity setup](../../docs/CONNECTIVITY_SETUP.md)
- [Configuration manual](../../docs/CONFIGURATION.md)