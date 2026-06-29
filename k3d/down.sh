#!/usr/bin/env bash
# Delete the k3d validation cluster.
set -euo pipefail
CLUSTER="${K3D_CLUSTER:-neo4j-backup}"
k3d cluster delete "$CLUSTER"
echo ">> deleted k3d cluster '$CLUSTER'"
