"""Artifact sidecars against the local stack (#118).

After `just cli-smoke` (or any backup of the demo group): every demo artifact has a
`<artifact>.meta.json` written from `neo4j-admin backup inspect`; chains read from them start at a
full; a missing sidecar is reported as unknown, then backfilled by one inspect.

    orchestrator/.venv/bin/python orchestrator/smoke_sidecars.py
"""

import json
import os
import sys

os.environ["AWS_ENDPOINT_URL_S3"] = "http://localhost:9000"  # host side of the stack
os.environ.setdefault("RUNNER_EXEC_PREFIX", json.dumps(
    ["docker", "compose", "--env-file", ".env", "-f", "docker/compose.yaml", "exec", "-T", "runner"]))
os.environ.setdefault("NEO4J_BACKUP_POLICY", "policies/demo.yaml")

from neo4j_backup_core import artifacts, env, ops, paths  # noqa: E402
from neo4j_backup_core.policy import load_policy  # noqa: E402


def main() -> None:
    store, runner, layout = env.store(), env.runner(), paths.get_layout()
    run = env.subprocess_admin(runner, stdout=sys.stderr)
    group = load_policy(env.policy_path()).group("demo")
    for name in group.names:
        out = ops.artifact_chains(store, layout, group.id, name, run_admin=run, runner=runner)
        assert out["chains"], out
        for c in out["chains"]:
            assert c["artifacts"][0]["type"] == "full" or c.get("broken"), c
        print(f"== {name}: {len(out['chains'])} chain(s), newest {out['chains'][-1]['from']} -> "
              f"{out['chains'][-1]['to']} (tx {out['chains'][-1]['highest_tx']}) ==")

    name = group.names[0]
    prefix = layout.alias_prefix(group.id, name)
    key = store.latest_artifact_key(prefix)
    side = artifacts.sidecar_key(key)
    before = json.loads(store.get_text(side))
    store.delete_keys([side])
    assert key in ops.artifact_chains(store, layout, group.id, name)["unknown"]
    back = ops.artifact_chains(store, layout, group.id, name, run_admin=run, runner=runner)
    assert back["unknown"] == [], back
    after = json.loads(store.get_text(side))
    assert after == before, (before, after)
    print(f"== backfilled {side} ({after['type']}, tx {after['lowest_tx']}-{after['highest_tx']}, "
          f"neo4j-admin {after['neo4j_admin']}) ==")
    print("PASS: sidecars written, chains read, backfill works")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        sys.exit(1)
