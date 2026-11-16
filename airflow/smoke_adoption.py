"""#244 on the local stack: the Airflow restore preview and one-group backup, the Dagster restore
preview, and the CLI waiting on (and refusing at) the group lock.

1. `neo4j_backup_gold_full` with group_id=demo: one full per member of demo; an unknown group
   fails.
2. `neo4j_restore` with dry_run: the plan task logs the plan, nothing after it runs, every
   alias still points where it did.
3. Dagster `restore_group` with dry_run: same, through Dagster.
4. CLI `backup demo` while another holder has the lock: fails with kind `locked`; after the
   lock is released it succeeds.

Prereqs: `just fresh`.   airflow/.venv/bin/python airflow/smoke_adoption.py
(Step 3 runs in the orchestrator venv as a subprocess.)
"""

import json
import os
import subprocess
import sys

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
ENV = {
    "NEO4J_BACKUP_POLICY": os.path.join(REPO, "policies", "demo.yaml"),
    "NEO4J_BOLT_URI": "neo4j://localhost:7687", "NEO4J_PASSWORD": "devpassword",
    "BACKUP_BUCKET": "neo4j-backups", "AWS_ENDPOINT_URL_S3": "http://localhost:9000",
    "AWS_REGION": "us-east-1", "AWS_ACCESS_KEY_ID": "minioadmin",
    "AWS_SECRET_ACCESS_KEY": "minioadmin", "NEO4J_BACKUP_SOURCE": "neo4j:6362",
    "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "RUNNER_PAGECACHE": "512M",
    "RUNNER_HEAP_SIZE": "2G",
}
os.environ.update({**ENV, "AIRFLOW_HOME": os.path.join(REPO, ".airflow_home"),
                   "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
                   "AIRFLOW__CORE__DAGS_FOLDER": os.path.join(REPO, "airflow", "dags")})
MEMBERS = ["acme-orders", "acme-graph", "acme-audit"]


def _state(run) -> str:
    return str(getattr(run, "state", run)).split(".")[-1].lower()


def _targets(neo):
    return {m: neo.alias_target(m) for m in MEMBERS}


def main() -> None:
    subprocess.run([os.path.join(REPO, "airflow", ".venv", "bin", "airflow"), "db", "migrate"],
                   check=True, capture_output=True)
    sys.path.insert(0, os.path.join(REPO, "airflow", "dags"))
    from neo4j_backup_airflow import config
    from neo4j_backup_core import locks, paths
    import backup_dag as bd
    import restore_dag as rd

    store, neo = config.store(), config.neo4j()
    before = _targets(neo)
    assert all(before.values()), f"run `just fresh` first: {before}"

    counts = {m: len(store.list_artifacts(paths.alias_prefix("demo", m))) for m in MEMBERS}
    run = bd.neo4j_backup_gold_full.test(run_conf={"group_id": "demo"})
    assert _state(run) == "success", _state(run)
    after = {m: len(store.list_artifacts(paths.alias_prefix("demo", m))) for m in MEMBERS}
    assert all(after[m] == counts[m] + 1 for m in MEMBERS), (counts, after)
    bad = _state(bd.neo4j_backup_gold_full.test(run_conf={"group_id": "nope"}))
    assert bad == "failed", f"an unknown group_id ran: {bad}"
    print(f"== Airflow backup with group_id=demo: one full per member ({counts} -> {after}); "
          f"group_id=nope fails ==")

    run = rd.neo4j_restore_dag.test(run_conf={"group_id": "demo", "dry_run": True})
    tis = {ti.task_id: _state(ti) for ti in run.get_task_instances()}
    assert tis.get("plan") == "skipped", tis
    assert not any(s == "success" for t, s in tis.items() if t != "plan"), tis
    assert _targets(neo) == before, "a dry run moved an alias"
    print(f"== Airflow neo4j_restore dry_run: plan logged, later tasks {sorted(set(tis.values()))}, "
          f"aliases unchanged ==")

    py = os.path.join(REPO, "orchestrator", ".venv", "bin", "python")
    code = (
        "import os, json, dagster as dg\n"
        "from neo4j_backup_dagster import definitions as D\n"
        "from neo4j_backup_dagster.resources import *\n"
        "r = D.restore_group.execute_in_process(run_config={'ops': {'restore_group_op': "
        "{'config': {'group_id': 'demo', 'dry_run': True}}}}, resources={"
        "'neo4j': Neo4jResource(uri='neo4j://localhost:7687', password='devpassword'),"
        "'store': ObjectStoreResource(bucket='neo4j-backups', endpoint_url='http://localhost:9000',"
        " region='us-east-1'), 'runner': RunnerResource(),"
        "'pipes_subprocess_client': dg.PipesSubprocessClient()})\n"
        "out = r.output_for_node('restore_group_op')\n"
        "print(json.dumps({'ok': r.success, 'dry_run': out['dry_run'],"
        " 'members': sorted(m['alias'] for m in out['plan'].get('members', []))}))\n")
    p = subprocess.run([py, "-c", code], env={**os.environ}, capture_output=True, text=True,
                       cwd=os.path.join(REPO, "orchestrator"))
    assert p.returncode == 0, p.stderr[-2000:]
    got = json.loads(p.stdout.strip().splitlines()[-1])
    assert got["ok"] and got["dry_run"], got
    assert _targets(neo) == before, "a Dagster dry run moved an alias"
    print(f"== Dagster restore_group dry_run: plan for {got['members']}, aliases unchanged ==")

    cli = [os.path.join(REPO, "orchestrator", ".venv", "bin", "neo4j-backup"), "--json"]
    held = locks.acquire(store, "demo", "smoke_adoption")
    try:
        p = subprocess.run([*cli, "backup", "demo", "--kind", "DIFF"],
                           env={**os.environ, "NEO4J_BACKUP_LOCK_WAIT": "3"},
                           capture_output=True, text=True)
        env = json.loads(p.stdout)
        assert p.returncode == 1 and env["error"]["kind"] == "locked", p.stdout
        print(f"== CLI backup with the lock held: exit 1, {env['error']['kind']}: "
              f"{env['error']['msg'][:80]} ==")
    finally:
        locks.release(store, "demo", held)
    p = subprocess.run([*cli, "backup", "demo", "--kind", "DIFF"], env={**os.environ},
                       capture_output=True, text=True)
    assert p.returncode == 0, p.stdout + p.stderr[-1500:]
    assert not store.list_text_keys("_locks/", ".lock"), "the CLI left its lock behind"
    print("== CLI backup after release: ok, lock released ==")
    print("PASS: previews, one-group backup and the CLI lock on the stack")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
