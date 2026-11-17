# Recover or move a group

Restore groups from a lost cluster, or move a group to another cluster.
Both clusters must be in the policy's `clusters:`. The source must be down, or fenced off with `fence_source`.

=== "Agent (MCP)"

    Run the `recover_cluster` prompt with the lost and the target cluster
    (Claude Code: `/mcp__neo4j-backup__recover_cluster east west INC-77`). The agent:

    1. Names the groups on the lost cluster with `list_targets`, and flags stale ones from `coverage_report`.
    2. Shows every group's seeds and the metadata it replays with `run_restore_to_cluster(dry_run=true)`.
        If the source still answers, it stops: fencing is the operator's call.

    3. After the operator approves, calls it with `confirm=true` and shows the baseline per group.

    For one group, call `run_restore_to_cluster(to_cluster, group)` directly, dry_run first.

=== "CLI"

    ```bash
    # one group; the source is the group's cluster in the policy
    neo4j-backup --json restore orders --to-cluster west --dry-run
    # every group of a lost cluster, in one plan
    neo4j-backup --json restore --from-cluster east --to-cluster west --confirm --ticket INC-77
    # the source still answers (a planned move): stop the groups' writes there
    neo4j-backup --json restore orders --to-cluster west --fence-source --confirm
    ```

=== "Dagster"

    The `move_job`, with `to_cluster` and `group_id` or `from_cluster`.

=== "Airflow"

    The `neo4j_restore_to_cluster` DAG, with Params `to_cluster` and `group_id` or `from_cluster`.

First it checks that the target answers and every member has a seed (`until` works as usual). Then, on the target:

1. It replays users, roles and DBMS privileges from the source, or from its newest metadata export.
    Exported users get a placeholder password and CHANGE REQUIRED. `skip_metadata` (`--no-metadata`) skips this step.

2. It seeds every member. The target's servers must read the group's store.
3. It grants privileges under the new names, creates or swaps the aliases, and takes baseline fulls.
4. It fences the groups on the source in `_state/<group>.json`, so each chain has one writing cluster.

A lost source needs a recent metadata export. The `<cluster>_dbms_metadata` schedule takes one.
By hand: `neo4j-backup metadata export` or `run_metadata_export`.

The policy stays unchanged. For a permanent move, set `cluster: west` on the groups, which lifts the fence.
`--member`, `--from-load`, `--from-dump`, `--replace` and `--check` conflict with `--to-cluster`.
[RECOVERY.md](../RECOVERY.md) covers rebuilding the cluster.
