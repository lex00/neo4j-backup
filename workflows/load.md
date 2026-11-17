# Load data in bulk

Give the group a `load:` block ([POLICY.md](../POLICY.md)) with `mode` (`once` or `rebuild`) and `writes`.
Its `build` of `kind: import` names the input files in `args`.
Put the files in `load.source` (default `<group>/_source/`, a `<slug>/` folder per member). Then:

```bash
neo4j-backup --json load <group> --dry-run        # source objects, build argv, target, what is lost
neo4j-backup --json load <group> --confirm --ticket CHG-1234
```

The runner imports and dumps each member to `_seed/`, and the cluster seeds new databases.
Once every member passes its checks (online, counts, `load.checks`), privileges copy and aliases move together.
A failure drops the new databases (`--keep-failed` keeps them). By-name groups need `--replace`.

A switch to `writes: read-only` applies at the next load. For `mode: once`, run
`neo4j-backup load <group> --reapply-access --confirm`.

Scheduled rebuilds run on the tier's `rebuild_cron` (Dagster `<tier>_rebuild`, Airflow `neo4j_rebuild_<tier>`).
On demand: Dagster's `load_job`, Airflow's `neo4j_load`. A rebuild never overlaps the group's backups.
A failed rebuild leaves the previous load serving.

For builds the tool doesn't run, see [IMPORT.md](../IMPORT.md).

## Custom builds

When `neo4j-admin database import` can't read the source (RDF, say), run your own converter:

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
A non-zero exit, a timeout or missing output fails the load before anything is seeded.
