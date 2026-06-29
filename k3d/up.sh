#!/usr/bin/env bash
# Bring up a local k3d cluster to validate RUNNER_MODE=k8s. Reuses the Compose Neo4j +
# MinIO (running Neo4j on k8s is the team's infra concern, not this tooling's code) —
# the backup pod reaches them via host.k3d.internal. Requires the Compose stack up
# (`just up`) with the backup port (6362) published.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
CLUSTER="${K3D_CLUSTER:-neo4j-backup}"
IMAGE="neo4j:${NEO4J_VERSION:-2026.05.0-enterprise}"

if k3d cluster list 2>/dev/null | grep -q "^$CLUSTER "; then
  echo ">> k3d cluster '$CLUSTER' already exists"
else
  echo ">> creating k3d cluster '$CLUSTER'"
  k3d cluster create "$CLUSTER" --wait
fi

echo ">> importing $IMAGE into the cluster (so the backup pod doesn't re-pull)"
k3d image import "$IMAGE" -c "$CLUSTER"

kubectl config use-context "k3d-$CLUSTER" >/dev/null
echo ">> nodes:"; kubectl get nodes
echo ">> ready. Run: just k3d-smoke"
