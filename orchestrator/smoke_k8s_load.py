"""Loads with RUNNER_MODE=k8s (#107, #109): Dagster's `load_job` with the k8s runner in k3d.

1. A CSV `kind: import` group: the import and dump run in a neo4j pod, then the seed, check
   and swap.
2. A `kind: command` group with `image`: the build runs in its own pod (the aws-cli image),
   writes CSV to OUTPUT_URI in the bucket, and the built-in import runs from there.

Prereqs: `just fresh`, `just k3d-up`, and the aws-cli image in k3d:
`k3d image import amazon/aws-cli:2.27.50 -c neo4j-backup`.

    orchestrator/.venv/bin/python orchestrator/smoke_k8s_load.py
"""

import json
import os
import sys
import tempfile

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)
NODES = "id:ID,name,:LABEL\n1,a,Item\n2,b,Item\n3,c,Item\n"
BUILD = ("printf 'id:ID,name,:LABEL\\n1,a,Item\\n2,b,Item\\n3,c,Item\\n' > /tmp/nodes.csv && "
         "aws s3 cp /tmp/nodes.csv \"${OUTPUT_URI}nodes.csv\" && echo SMOKE109-image-built")


def policy() -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    check = [{"query": "MATCH (i:Item) RETURN count(i)", "expect": "== 3"}]
    raw["db_groups"] += [
        {"id": "k8simp", "tier": "gold", "aliases": ["k8s-imp"], "load": {
            "mode": "once", "writes": "read-only", "checks": check,
            "build": {"kind": "import", "input_type": "csv", "args": ["--nodes=nodes.csv"]}}},
        {"id": "k8simg", "tier": "gold", "aliases": ["k8s-img"], "load": {
            "mode": "once", "writes": "read-only", "checks": check,
            "build": {"kind": "command", "image": "amazon/aws-cli:2.27.50",
                      "command": ["sh", "-c", BUILD], "output": "import",
                      "import_args": ["--nodes=nodes.csv"], "timeout": "10m"}}},
    ]
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


POL = policy()
os.environ["NEO4J_BACKUP_POLICY"] = POL

import smoke_k8s as K  # noqa: E402  (k8s runner resources, host IP)
from neo4j_backup_dagster import definitions as D  # noqa: E402


def load(group: str):
    import dagster as dg
    with dg.instance_for_test() as inst:
        res = D.load_job.execute_in_process(
            run_config={"ops": {"load_group_op": {"config": {"group_id": group,
                                                               "baseline": False}}}},
            resources=K.RES, instance=inst, raise_on_error=False)
        return res, [e.user_message for e in inst.all_logs(res.run_id)]


def main() -> None:
    store, neo = K.RES["store"], K.RES["neo4j"]
    store._core().put_text("k8simp/_source/nodes.csv", NODES)
    for group, alias in (("k8simp", "k8s-imp"), ("k8simg", "k8s-img")):
        res, logs = load(group)
        assert res.success, f"k8s load of {group} failed"
        out = res.output_for_node("load_group_op")
        m = out["members"][0]
        assert m["import_counts"]["nodes"] == 3, m
        assert neo.alias_target(alias) == m["newdb"]
        if group == "k8simg":  # the image build's own output is in the run's log (#109)
            assert any("SMOKE109-image-built" in x for x in logs), "build log missing"
        print(f"== k8s load {group}: built in a pod, {alias} -> {m['newdb']}, 3 nodes ==")
    print("PASS: loads with RUNNER_MODE=k8s (import build, image build)")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        os.unlink(POL)
