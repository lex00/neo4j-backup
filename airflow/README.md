# Airflow adapter

Airflow 3.x DAGs over `neo4j_backup_core`, mirroring the Dagster adapter. Same `core/`,
same [policy](../POLICY.md) and [recovery](../RECOVERY.md) — pick Dagster *or* Airflow.
(Full operator guide lands with #12.)

- **DAGs:** `airflow/dags/`
- **Helpers** (env → core clients): `orchestrator/neo4j_backup_airflow/config.py`
- **Config:** same env vars as the Dagster adapter (orchestrator/README env table), e.g.
  `NEO4J_PASSWORD`, `NEO4J_BOLT_URI`, `BACKUP_BUCKET`, `NEO4J_BACKUP_POLICY`, `RUNNER_MODE`.

## Install (uv, Python 3.13)

Airflow needs its constraints file; it goes in its own venv, isolated from Dagster:

```bash
just airflow-install          # uv venv + pinned Airflow + adapter (no deps) + neo4j + pytest
```

Override the pins with `AIRFLOW_VERSION=… PYTHON_VERSION=… just airflow-install`. See
`airflow/install.sh` for the underlying steps.

## Run it locally

Against the Compose stack (`just up && just bootstrap`):

```bash
just airflow-smoke        # backup → verify → restore → prune (dag.test, in-process)
just airflow-pitr         # real full+diff chain + point-in-time restore (seedRestoreUntil)
just airflow-k8s-smoke    # KubernetesPodOperator mode against k3d (needs `just k3d-up`)
just airflow-standalone   # scheduler + UI on :8080, DAGs wired to the stack
```

`dag.test()` runs a DAG in-process against the live stack — the Airflow analogue of
Dagster's `execute_in_process`, and how the smokes validate without a running scheduler.

## Validate DAGs parse (no stack needed)

```bash
AIRFLOW_HOME=/tmp/af NEO4J_BACKUP_POLICY=policies/demo.yaml \
  airflow/.venv/bin/python -m pytest airflow/tests/
```

This is the same DagBag import-error check CI runs.
