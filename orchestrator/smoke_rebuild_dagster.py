"""Dagster rebuild lane on the local stack (#108): the `<tier>_rebuild` schedule requests the
rebuild group; the run swaps the alias. Then, with the source gone, the next run fails as a
Dagster failure and the previous load keeps serving.

Prereqs: `just fresh`.   orchestrator/.venv/bin/python orchestrator/smoke_rebuild_dagster.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import rebuild_smoke_common as C  # noqa: E402

POL = C.policy_file()
EXEC = ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T",
        "runner"]
os.chdir(C.REPO)
os.environ.update({"NEO4J_BACKUP_POLICY": POL, "NEO4J_BOLT_URI": "neo4j://localhost:7687",
                   "NEO4J_PASSWORD": "devpassword", "BACKUP_BUCKET": "neo4j-backups",
                   "AWS_ENDPOINT_URL_S3": "http://localhost:9000", "AWS_REGION": "us-east-1",
                   "AWS_ACCESS_KEY_ID": "minioadmin", "AWS_SECRET_ACCESS_KEY": "minioadmin",
                   "NEO4J_BACKUP_SOURCE": "neo4j:6362", "SCRATCH_PATH": "/scratch",
                   "RUNNER_EXEC_PREFIX": json.dumps(EXEC), "RUNNER_PAGECACHE": "512M"})

import dagster as dg  # noqa: E402

from neo4j_backup_core import env  # noqa: E402
from neo4j_backup_dagster import definitions as D  # noqa: E402


def run(request):
    """The run the schedule requested, with local resources: neo4j-admin runs in the runner
    container (as in smoke_local.py; a deployment leaves exec_prefix empty)."""
    from neo4j_backup_dagster.resources import RunnerResource
    job = D.defs.get_job_def("load_job")
    return job.execute_in_process(
        run_config=request.run_config, tags=request.tags, raise_on_error=False,
        resources={"runner": RunnerResource(backup_source="neo4j:6362", scratch_path="/scratch",
                                            pagecache="512M", heap_size="2G",
                                            exec_prefix=EXEC)})


def main() -> None:
    store, neo = env.store(), env.neo4j()
    C.put_source(store)
    sched = D.defs.get_schedule_def(f"{C.TIER}_rebuild")
    with dg.build_schedule_context() as ctx:
        reqs = list(sched.evaluate_tick(ctx).run_requests or [])
    assert [r.run_config["ops"]["load_group_op"]["config"]["group_id"] for r in reqs] == \
        [C.GROUP], reqs
    assert reqs[0].tags.get("neo4j_group") == C.GROUP, reqs[0].tags
    res = run(reqs[0])
    assert res.success, "rebuild run failed"
    first = C.alias_target(neo)
    assert first and neo.count_nodes(C.ALIAS) == 3
    print(f"== {C.TIER}_rebuild schedule -> load_job: {C.ALIAS} -> {first}, 3 nodes ==")
    C.drop_source(store)
    res = run(reqs[0])
    assert not res.success, "a rebuild with no source should fail"
    assert C.alias_target(neo) == first
    print(f"== source removed: the run failed in Dagster; {C.ALIAS} still serves {first} ==")
    print("PASS: Dagster rebuild lane (schedule, run, failure keeps the previous load)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        os.unlink(POL)
