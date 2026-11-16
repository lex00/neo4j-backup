"""#229 on k3d: a RUNNER_MODE=k8s backup whose bucket credentials reach the pod only from a
Kubernetes Secret (RUNNER_SECRET_ENV), never as plain values in the pod spec.

Prereqs: `just fresh`, `just k3d-up`.   orchestrator/.venv/bin/python orchestrator/smoke_k8s_secret.py
"""

import json
import subprocess
import sys

import dagster as dg

import smoke_k8s as K
from neo4j_backup_dagster import definitions as D
from neo4j_backup_dagster.resources import RunnerResource

SECRET = "neo4j-backup-s3"


def main() -> None:
    subprocess.run(["kubectl", "delete", "secret", SECRET, "--ignore-not-found"],
                   capture_output=True)
    subprocess.run(["kubectl", "create", "secret", "generic", SECRET,
                    "--from-literal=AWS_ACCESS_KEY_ID=minioadmin",
                    "--from-literal=AWS_SECRET_ACCESS_KEY=minioadmin"], check=True,
                   capture_output=True)
    runner = RunnerResource(
        mode="k8s", image=K.IMAGE, backup_source=K.POD_SOURCE, scratch_path="/scratch",
        pagecache="256M", heap_size="512M", memory_limit="1Gi", scratch_storage="1Gi",
        extra_env_json=json.dumps({"AWS_REGION": "us-east-1", "AWS_ENDPOINT_URL_S3": K.POD_S3}),
        secret_env_json=json.dumps({"AWS_ACCESS_KEY_ID": f"{SECRET}:AWS_ACCESS_KEY_ID",
                                    "AWS_SECRET_ACCESS_KEY": f"{SECRET}:AWS_SECRET_ACCESS_KEY"}))
    spec = runner._core().k8s_pod_spec({})
    assert "minioadmin" not in json.dumps(spec), spec
    env = {e["name"]: e for e in spec["containers"][0]["env"]}
    assert env["AWS_SECRET_ACCESS_KEY"]["valueFrom"]["secretKeyRef"]["name"] == SECRET
    store = K.RES["store"]
    phys = K.RES["neo4j"].alias_target("acme-orders")
    prefix = D._layout.physical_prefix("demo", "acme-orders", phys)
    before = len(store.list_artifacts(prefix))
    with dg.instance_for_test() as inst:
        inst.add_dynamic_partitions(D.targets.name, [K.PARTITION])
        r = dg.materialize([D.backup], partition_key=K.PARTITION,
                           resources={**K.RES, "runner": runner}, instance=inst,
                           run_config={"ops": {"backup": {"config": {"kind": "FULL"}}}})
        assert r.success, "k8s backup with Secret-sourced credentials failed"
    after = len(store.list_artifacts(prefix))
    assert after == before + 1, (before, after)
    print(f"== k8s backup of {phys}: credentials from Secret {SECRET}, none in the pod spec; "
          f"artifacts {before} -> {after} ==")
    print("PASS: RUNNER_SECRET_ENV (credentials from a Kubernetes Secret)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
