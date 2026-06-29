#!/usr/bin/env bash
# Install the Airflow adapter into its own venv (uv). Airflow needs its constraints file,
# and we keep it isolated from the Dagster adapter (--no-deps for the local package).
#
#   AIRFLOW_VERSION=3.2.2 PYTHON_VERSION=3.13 ./airflow/install.sh
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

PY="${PYTHON_VERSION:-3.13}"
AF="${AIRFLOW_VERSION:-3.2.2}"   # the version this project is validated against
EXTRAS="cncf.kubernetes,amazon,neo4j"

echo "==> uv venv airflow/.venv (python $PY)"
uv venv airflow/.venv --python "$PY"

echo "==> apache-airflow[$EXTRAS]==$AF (constraints for $PY)"
uv pip install --python airflow/.venv/bin/python \
  "apache-airflow[$EXTRAS]==$AF" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AF/constraints-$PY.txt"

echo "==> adapter (no deps) + neo4j driver + pytest"
uv pip install --python airflow/.venv/bin/python -e orchestrator --no-deps
uv pip install --python airflow/.venv/bin/python neo4j pytest

echo "done: airflow/.venv ready — try \`just airflow-smoke\` or \`just airflow-standalone\`"
