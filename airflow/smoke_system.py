"""Validate the Airflow neo4j_system_backup DAG (#15) against the local stack via
dag.test(): a binary `system` backup lands under `_dbms/system/`. The restore side is
offline + node-local (path B) and is validated by `bootstrap/restore_system.sh` (which
restarts Neo4j), not here.

    airflow/.venv/bin/python airflow/smoke_system.py
"""

import json
import os
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T", "runner"]
os.environ.update({
    "AIRFLOW_HOME": os.path.join(REPO, ".airflow_home"),
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "AIRFLOW__CORE__DAGS_FOLDER": os.path.join(REPO, "airflow", "dags"),
    "NEO4J_BACKUP_POLICY": os.path.join(REPO, "policies", "demo.yaml"),
    "NEO4J_BOLT_URI": "neo4j://localhost:7687",
    "NEO4J_PASSWORD": "devpassword",
    "BACKUP_BUCKET": "neo4j-backups",
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "NEO4J_BACKUP_SOURCE": "neo4j:6362",
    "RUNNER_EXEC_PREFIX": json.dumps(EXEC),
})


def _ok(run) -> bool:
    return str(getattr(run, "state", run)).split(".")[-1].lower() == "success"


def main() -> None:
    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import paths
    import system_dag as sd

    store = config.store()
    before = len(store.list_artifacts(paths.system_prefix()))

    print("== BACKUP: neo4j_system_backup via dag.test() ==")
    run = sd.neo4j_system_backup_dag.test()
    assert _ok(run), f"system backup DAG state={getattr(run,'state',run)}"
    after = len(store.list_artifacts(paths.system_prefix()))
    assert after > before, f"no system artifact written ({before}->{after})"
    print(f"   system backup OK ({before}->{after} artifacts under {paths.system_prefix()})")
    print("PASS: Airflow system binary backup validated (restore: bootstrap/restore_system.sh)")


if __name__ == "__main__":
    main()
