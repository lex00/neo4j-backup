"""Airflow rebuild lane on the local stack (#108): `neo4j_rebuild_<tier>` (dag.test,
in-process) rebuilds the tier's rebuild group and swaps its alias; with the source gone, the
next run fails and the previous load keeps serving.

Prereqs: `just fresh`, `just airflow-install`.   airflow/.venv/bin/python airflow/smoke_rebuild.py
"""

import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "orchestrator"))
import rebuild_smoke_common as C  # noqa: E402

POL = C.policy_file()
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
os.chdir(REPO)
os.environ.update({
    "AIRFLOW_HOME": os.path.join(REPO, ".airflow_home"),
    "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
    "AIRFLOW__CORE__DAGS_FOLDER": os.path.join(REPO, "airflow", "dags"),
    "NEO4J_BACKUP_POLICY": POL, "NEO4J_BOLT_URI": "neo4j://localhost:7687",
    "NEO4J_PASSWORD": "devpassword", "BACKUP_BUCKET": "neo4j-backups",
    "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
    "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
    "NEO4J_BACKUP_SOURCE": "neo4j:6362", "SCRATCH_PATH": "/scratch",
    "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "RUNNER_PAGECACHE": "512M",
    "RUNNER_HEAP_SIZE": "2G"})
AF = os.path.join(REPO, "airflow", ".venv", "bin", "airflow")


def _ok(run) -> bool:
    return str(getattr(run, "state", run)).split(".")[-1].lower() == "success"


def main() -> None:
    subprocess.run([AF, "db", "migrate"], check=True, env=os.environ,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    subprocess.run([AF, "pools", "set", "neo4j_full", "1", "full lane"], check=True,
                   env=os.environ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    import load_dag

    dag = getattr(load_dag, f"neo4j_rebuild_{C.TIER}")
    store, neo = config.store(), config.neo4j()
    C.put_source(store)
    before = C.alias_target(neo)
    assert _ok(dag.test()), "rebuild DAG run failed"
    first = C.alias_target(neo)
    assert first and first != before and neo.count_nodes(C.ALIAS) == 3
    print(f"== neo4j_rebuild_{C.TIER}: {C.ALIAS} {before} -> {first}, 3 nodes ==")
    C.drop_source(store)
    assert not _ok(dag.test()), "a rebuild with no source should fail"
    assert C.alias_target(neo) == first
    print(f"== source removed: the DAG run failed; {C.ALIAS} still serves {first} ==")
    print("PASS: Airflow rebuild lane (DAG run, failure keeps the previous load)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        os.unlink(POL)
