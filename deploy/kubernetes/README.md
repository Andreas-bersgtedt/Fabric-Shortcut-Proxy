# Kubernetes hybrid agent proof

This deployment runs three Python materializers and two C++ serving replicas against one ReadWriteMany volume. The Python StatefulSet starts all ordinals in parallel so shard 0 cannot block shard 1 or shard 2 during generation coordination. The C++ Pods remain unready until the publisher creates a valid `CURRENT` generation.

## Requirements

- Kubernetes 1.25 or later
- A default StorageClass that supports `ReadWriteMany`
- Metrics Server for the C++ HorizontalPodAutoscaler
- Network access from the materializer Pods to the source database
- A source account with read-only access

The base installs the `postgres`, `azureblob`, `keyvault`, and `onelake` Python extras by default so Azure Identity, Key Vault, Azure Blob/ADLS, and OneLake-backed Open Mirroring work in the production image. Set the `FSP_EXTRAS` build argument for a leaner or different packaged driver set. SQL Server containers also require the Microsoft ODBC driver at the operating-system layer.

## Build images

Run from the repository root:

```powershell
docker build --build-arg FSP_ENTERPRISE=1 -f Dockerfile.python -t fabric-shortcut-proxy-python:dev .
docker build -f agent-cpp/Dockerfile -t fabric-shortcut-proxy-cpp:dev .
```

Push both images to a registry for a remote cluster, then replace the two `image` values in the base manifests. For kind or minikube, load the local images into the cluster.

## Configure the source

Copy `examples/source-secret.example.yaml` outside the repository, replace the
connection string, and apply it. Do not commit the populated Secret. When using
Manager integration, set `MANAGER_AUTH_USERNAME` and `MANAGER_AUTH_PASSWORD` in
this Secret, configure the Manager with the same values, and set `MANAGER_URL` on
each Agent workload.

```powershell
kubectl apply -f deploy/kubernetes/base/namespace.yaml
kubectl apply -f path/to/source-secret.yaml
```

The three workers must read one shared source database. An in-memory SQLite URL creates three unrelated databases and is not a distributed-materialization test.

## Deploy

Inspect the rendered manifests before applying them:

```powershell
kubectl kustomize deploy/kubernetes/base
kubectl apply -k deploy/kubernetes/base
kubectl -n fabric-shortcut-proxy get pods,pvc,service,hpa
```

If the cluster has no default RWX class, set `storageClassName` in `base/artifact-pvc.yaml` or add it through a Kustomize overlay.

## Manager TLS

The base overlay keeps Manager traffic cluster-private over HTTP. Apply
`overlays/tls` when Agent-to-Manager traffic must be encrypted. It starts TLS on
the Manager, configures Python Agents with CA verification, and gives C++ Agents
a loopback TLS proxy. See `overlays/tls/README.md` for the certificate Secret
contract and deployment commands.

## Local kind proof

The `overlays/kind` overlay adds a seeded PostgreSQL source, a single-node hostPath RWX volume, the smoke client, and smaller resource requests. It also removes the HPA because a default kind cluster has no Metrics Server.

```powershell
kind create cluster --config deploy/kubernetes/overlays/kind/cluster.yaml
kind load docker-image fabric-shortcut-proxy-python:dev fabric-shortcut-proxy-cpp:dev --name fsp-proof
kubectl apply -k deploy/kubernetes/overlays/kind
kubectl -n fabric-shortcut-proxy get pods -w
```

The checked-in kind Secret contains only a disposable local password. Do not copy it into a shared or remote cluster.

## Verify

Watch the materializers first. All three should start together, and each log should report a different shard index.

```powershell
kubectl -n fabric-shortcut-proxy logs -f fsp-materializer-0
kubectl -n fabric-shortcut-proxy get pods -w
```

The C++ Pods return 503 from `/readyz` until shard 0 writes `READY.json` and atomically activates `CURRENT`. After both serving Pods are ready, create the allowed smoke client:

```powershell
kubectl apply -f deploy/kubernetes/examples/smoke-client.yaml
kubectl -n fabric-shortcut-proxy exec fsp-smoke-client -- curl -fsS http://shortcut-proxy/healthz
kubectl -n fabric-shortcut-proxy exec fsp-smoke-client -- curl -fsS http://shortcut-proxy/readyz
```

A Pod without the `fabric-shortcut-proxy.io/data-plane-client: "true"` label is blocked by the base NetworkPolicy. Label the namespace and ingress-controller Pods with the access labels from `base/network-policy.yaml` before routing traffic from another namespace.

## Current limits

- The base Service is cluster-private and does not configure TLS or external ingress.
- The C++ data plane does not verify SigV4. Keep it behind private networking and an authenticated gateway.
- Source-wide snapshot consistency is not implemented. The supported setting is `best_effort`.
- The base assumes one fixed three-worker generation epoch. Do not change the StatefulSet replica count during publication.
