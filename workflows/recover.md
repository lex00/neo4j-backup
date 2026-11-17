# Recover or move a group

An agent on the [MCP server](../MCP.md) runs this page with the `recover_cluster` prompt.

With both clusters in the policy's `clusters:`, `restore --to-cluster` restores into another cluster:

```bash
# one group; the source is the group's cluster in the policy
neo4j-backup --json restore orders --to-cluster west --dry-run
# every group of a lost cluster, in one plan
neo4j-backup --json restore --from-cluster east --to-cluster west --confirm --ticket INC-77
# the source still answers (a planned move): stop the groups' writes there
neo4j-backup --json restore orders --to-cluster west --fence-source --confirm
```

First it checks that the target answers and every member has a seed (`--until` works as usual).
The source must be down, or `--fence-source` given. Then, on the target:

1. It replays users, roles and DBMS privileges from the source, or from its newest metadata export.
    Exported users get a placeholder password and CHANGE REQUIRED. `--no-metadata` skips this step.

2. It seeds every member. The target's servers must read the group's store.
3. It grants privileges under the new names, creates or swaps the aliases, and takes baseline fulls.
4. It fences the groups on the source in `_state/<group>.json`, so two clusters never write one chain.

The policy isn't edited. For a permanent move, set `cluster: west` on the groups, which lifts the fence.
Dagster: `move_job`. Airflow: `neo4j_restore_to_cluster`. MCP: `run_restore_to_cluster`.
`--member`, `--from-load`, `--from-dump`, `--replace` and `--check` don't combine with `--to-cluster`.
See [RECOVERY.md](../RECOVERY.md) for rebuilding the cluster itself.
