#!/usr/bin/env bash
# Creates a local kind cluster, applies the RBAC senro needs, and writes the
# three files senro reads the cluster from. Source scripts/cluster-env.sh after.
set -euo pipefail
cd "$(dirname "$0")/.."

NAME=${KIND_CLUSTER:-senro-ai}
mkdir -p .kind

if ! kind get clusters 2>/dev/null | grep -qx "$NAME"; then
  # A kubeconfig of its own: this never touches ~/.kube/config.
  kind create cluster --name "$NAME" --kubeconfig .kind/kubeconfig
fi
export KUBECONFIG=$PWD/.kind/kubeconfig

kubectl wait --for=condition=Ready node --all --timeout=120s
kubectl apply -f k8s/rbac.yaml

kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}' > .kind/server
kubectl config view --minify --raw -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d > .kind/ca.crt
kubectl -n support create token senro --duration=4h > .kind/token
chmod 600 .kind/token

echo "cluster $NAME ready. Now run:  source scripts/cluster-env.sh"
