# Airflow adapter

Airflow 3.x DAGs over `neo4j_backup_core`, mirroring the Dagster adapter. Same `core/`,
same [policy](../POLICY.md) and [recovery](../RECOVERY.md) — pick Dagster *or* Airflow.
(Full operator guide lands with #12.)

- **DAGs:** `airflow/dags/`
- **Helpers** (env → core clients): `orchestrator/neo4j_backup_airflow/config.py`
- **Config:** same env vars as the Dagster adapter (orchestrator/README env table), e.g.
  `NEO4J_PASSWORD`, `NEO4J_BOLT_URI`, `BACKUP_BUCKET`, `NEO4J_BACKUP_POLICY`, `RUNNER_MODE`.

## Install (uv, Python 3.13)

Airflow needs its constraints file; install into its own venv:

```bash
uv venv airflow/.venv --python 3.13
AF=$(curl -s https://pypi.org/pypi/apache-airflow/json | python3 -c "import sys,json;print(json.load(sys.stdin)['info']['version'])")
uv pip install --python airflow/.venv/bin/python \
  "apache-airflow[cncf.kubernetes,amazon,neo4j]==$AF" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-$AF/constraints-3.13.txt"
uv pip install --python airflow/.venv/bin/python -e orchestrator --no-deps
uv pip install --python airflow/.venv/bin/python neo4j
```

## Validate DAGs parse

```bash
AIRFLOW_HOME=/tmp/af NEO4J_BACKUP_POLICY=policies/demo.yaml \
  airflow/.venv/bin/python -m pytest airflow/tests/
```
