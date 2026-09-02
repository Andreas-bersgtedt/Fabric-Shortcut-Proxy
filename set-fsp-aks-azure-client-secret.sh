#!/usr/bin/env bash
# Securely inject an Azure service-principal client secret into the AKS FSP Manager.
# Run this on the private AKS jump host, where kubectl can resolve the cluster API.

set -euo pipefail

NAMESPACE="${FSP_NAMESPACE:-fabric-shortcut-proxy}"
SECRET_NAME="${FSP_AZURE_IDENTITY_SECRET:-fsp-azure-identity}"
DEPLOYMENT_NAME="${FSP_MANAGER_DEPLOYMENT:-fsp-manager}"

if ! command -v kubectl >/dev/null 2>&1; then
    echo "kubectl is required." >&2
    exit 1
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
    echo "kubectl cannot reach AKS. Run this script on the private AKS jump host." >&2
    exit 1
fi

read -r -s -p "Azure client secret: " AZURE_CLIENT_SECRET
echo

if [[ -z "$AZURE_CLIENT_SECRET" ]]; then
    echo "Azure client secret cannot be empty." >&2
    exit 1
fi

trap 'unset AZURE_CLIENT_SECRET' EXIT

kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
    --from-literal=AZURE_CLIENT_SECRET="$AZURE_CLIENT_SECRET" \
    --dry-run=client -o yaml | kubectl apply -f -

kubectl -n "$NAMESPACE" set env "deployment/$DEPLOYMENT_NAME" \
    "--from=secret/$SECRET_NAME"
kubectl -n "$NAMESPACE" rollout status "deployment/$DEPLOYMENT_NAME" --timeout=180s

echo "Azure client secret injected into $NAMESPACE/$DEPLOYMENT_NAME."