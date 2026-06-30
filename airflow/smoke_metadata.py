"""Validate the Airflow metadata DAGs (#14) against the local stack via dag.test():
backup captures + stores a `_dbms/*.cypher` artifact; restore replays it over Bolt. The
replay is idempotent against the live DB (CREATE … IF NOT EXISTS + additive GRANTs), so it
runs safely without fixtures — the round-trip parity is covered by the core
orchestrator/smoke_metadata.py.

    airflow/.venv/bin/python airflow/smoke_metadata.py
"""

import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

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
})

AF = os.path.join(REPO, "airflow", ".venv", "bin", "airflow")


def _ok(run) -> bool:
    return str(getattr(run, "state", run)).split(".")[-1].lower() == "success"


def main() -> None:
    subprocess.run([AF, "db", "migrate"], check=True, env=os.environ,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import paths
    import metadata_dag as md

    store = config.store()

    print("== BACKUP: neo4j_metadata_backup via dag.test() ==")
    run = md.neo4j_metadata_backup_dag.test()
    assert _ok(run), f"metadata backup DAG state={getattr(run,'state',run)}"
    key = store.latest_text_key(paths.metadata_prefix())
    assert key, "no _dbms/ artifact written"
    body = store.get_text(key)
    assert "CREATE ROLE" in body or "GRANT" in body, "artifact has no metadata statements"
    print(f"   wrote {key} ({len(body)} bytes)")

    print("== RESTORE: neo4j_metadata_restore via dag.test() (replay latest) ==")
    run = md.neo4j_metadata_restore_dag.test()
    assert _ok(run), f"metadata restore DAG state={getattr(run,'state',run)}"
    print("   replay ran clean (idempotent against the live DB)")
    print("PASS: Airflow metadata backup + restore validated end to end (dag.test)")


if __name__ == "__main__":
    main()
