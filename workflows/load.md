# Load data in bulk

A bulk load fills a group from files: a nightly rebuild, or a one-time import.
The tool builds new databases from the files, checks them, and moves the [aliases](../GLOSSARY.md).

Give the group a `load:` block ([POLICY.md](../POLICY.md)) with `mode` (`once` or `rebuild`) and `writes`.
Its `build` of `kind: import` names the input files in `args`.
Put the files in `load.source` (default `<group>/_source/`, a `<slug>/` folder per member). Then:

=== "Agent (MCP)"

    Run the `bulk_load` prompt with the group and a ticket (Claude Code: `/mcp__neo4j-backup__bulk_load shop CHG-1234`).
    The agent shows the `load` settings with `policy_show` and the plan with `run_load(dry_run=true)`.
    After the operator approves, it calls `run_load` with `confirm=true`.

=== "CLI"

    ```bash
    neo4j-backup --json load <group> --dry-run        # source objects, build argv, target, what is lost
    neo4j-backup --json load <group> --confirm --ticket CHG-1234
    ```

=== "Dagster"

    `load_job` on demand, with `group_id`. Rebuilds run on the `<tier>_rebuild` schedule (`rebuild_cron`).
   

=== "Airflow"

    `neo4j_load` on demand, with Param `group_id`. Rebuilds run in `neo4j_rebuild_<tier>`.
   

The [runner](../GLOSSARY.md) imports and dumps each member to `_seed/`, and the cluster [seeds](../GLOSSARY.md) new databases.
Once every member passes its checks (online, counts, `load.checks`), privileges copy and aliases move together.
A failure drops the new databases (`keep_failed` keeps them). By-name groups need `replace`.
A rebuild and the group's backups take turns on one lock. After a failed rebuild, the previous load keeps serving.

A switch to `writes: read-only` applies at the next load. For `mode: once`, apply it now:
`run_load(reapply_access=true)` or `neo4j-backup load <group> --reapply-access --confirm`.

`run_export` or `neo4j-backup export <group>` writes a portable copy of a group's data.
For builds outside the tool, see [IMPORT.md](../IMPORT.md).

## Custom builds

For formats outside `neo4j-admin database import` (RDF, say), run your own converter:

```yaml
load:
  mode: rebuild
  writes: read-only
  build:
    kind: command
    command: [python3, /opt/rdf2csv.py]      # in the runner (subprocess); or image: on k8s
    args: [--in, "$SOURCE_URI", --out, .]
    env: { API_TOKEN: "secret:aws-sm:rdf-token" }
    output: import                           # the command writes nodes.csv, rels.csv
    import_args: [--nodes=nodes.csv, --relationships=rels.csv]
    timeout: 2h
```

It runs in a fresh `WORK_DIR` with `SOURCE_URI`, `OUTPUT_URI`, `NEO4J_BACKUP_GROUP` and `NEO4J_BACKUP_MEMBER` set.
An `image` runs as its own pod and must write to `OUTPUT_URI`.
A non-zero exit, a timeout or missing output fails the load before seeding starts.
