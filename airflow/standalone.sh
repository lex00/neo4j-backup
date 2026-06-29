#!/usr/bin/env bash
# Run Airflow standalone (scheduler + API server + UI on :8080) wired to the local Compose
# stack, so the DAGs are visible and triggerable in the UI. Same env the smokes set; the
# difference is a long-running server instead of in-process dag.test(). Local only.
#
# Prereqs: `just up` (+ `just bootstrap`) and `just airflow-install`.
#   ./airflow/standalone.sh
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

export AIRFLOW_HOME="$REPO/.airflow_home"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/airflow/dags"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True  # local: skip the login wall

# Wire the DAGs to the Compose Neo4j + MinIO + runner (override via env / .env as needed).
export NEO4J_BACKUP_POLICY="${NEO4J_BACKUP_POLICY:-$REPO/policies/demo.yaml}"
export NEO4J_BOLT_URI="${NEO4J_BOLT_URI:-neo4j://localhost:7687}"
export NEO4J_PASSWORD="${NEO4J_PASSWORD:-devpassword}"
export BACKUP_BUCKET="${BACKUP_BUCKET:-neo4j-backups}"
export AWS_ENDPOINT_URL_S3="${AWS_ENDPOINT_URL_S3:-http://localhost:9000}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"
export NEO4J_BACKUP_SOURCE="${NEO4J_BACKUP_SOURCE:-neo4j:6362}"
# neo4j-admin runs inside the Compose runner container (subprocess mode).
export RUNNER_EXEC_PREFIX="${RUNNER_EXEC_PREFIX:-[\"docker\",\"compose\",\"--env-file\",\".env\",\"-f\",\"docker/compose.yaml\",\"exec\",\"-T\",\"runner\"]}"

AF="$REPO/airflow/.venv/bin/airflow"
"$AF" db migrate
"$AF" pools set neo4j_full 1 "full backup lane" >/dev/null
"$AF" pools set neo4j_diff 6 "diff backup lane" >/dev/null

echo "Airflow UI -> http://localhost:8080  (DAGs wired to the Compose stack; Ctrl-C to stop)"
exec "$AF" standalone
