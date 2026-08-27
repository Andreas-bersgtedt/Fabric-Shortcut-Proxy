# Kubernetes hybrid agent fleet

Build `Dockerfile.python` and `agent-cpp/Dockerfile`, publish the images, then set
their names in `base/python-statefulset.yaml` and `base/cpp-deployment.yaml`.
Provision an RWX storage class for `shortcut-artifacts`, create `shortcut-source`
from `examples/source-secret.example.yaml`, and apply:

```sh
kubectl apply -k deploy/kubernetes/base
```

The Python StatefulSet has fixed three-way split ownership. Change its replica
count only between generations and update `AGENT_SHARD_COUNT` at the same time.
The C++ Deployment is the private Fabric-facing tier and may scale independently.
Its `/readyz` probe remains failed until `CURRENT` selects a complete generation.
