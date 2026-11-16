# Operator guide

For the platform team that runs Neo4j for the rest of the organization and uses neo4j-backup to
back up and restore its customers' databases. Your customers (application teams, data teams) use
the databases; they may have credentials on the cluster but not on Dagster or Airflow, so when they
need a restore they ask you.

The idea is that you edit one policy file and it drives every job: what is backed up, how often,
and how long it is kept. Restores are started by you when a customer asks. Approvals stay with
your team's own process; the tool's guard is `--dry-run` to preview and `--confirm` to apply.

Each section below gives the steps that work today. Steps marked **Planned** link to the issue
that will add them; until then, do them by hand as described.

If you haven't run the tool yet, start with [Getting started](GETTING-STARTED.md).

## How it fits together

- The **policy** ([POLICY.md](POLICY.md)) lists groups of databases. A group is the unit that
  restores to one instant; usually one customer, or one customer's related databases. Each group
  has a tier (when full and differential backups run) and a retention.
- A **scheduler** runs the jobs: Dagster schedules, Airflow DAGs, or cron/CI calling the CLI.
- A **runner** with `neo4j-admin` pulls each backup over the backup port and writes it to the
  bucket under `<group>/<slug>/<database>/`, where the slug is the alias (or a cleaned-up form of
  it when the alias has characters a path can't use). Nothing runs on the database servers.
- A **restore** creates a new database from a backup (`CREATE DATABASE … seedURI`, pulled by the
  Neo4j servers from the bucket), then moves the customer's alias to it. The old database stays
  until you drop it.

Applications must connect through the alias. Anything that uses the database name directly breaks
at every restore, because the name changes.

## Onboard a customer

1. Pick a tier. The example policy has `gold` (daily full, hourly differential), `silver`
   (weekly full, differential every 6 hours) and `bronze` (weekly full, daily differential). The
   differential interval is how much a customer can lose in a restore.
2. Make sure the database is behind an alias. For a new database, create it and an alias
   (`CREATE DATABASE`, then `CREATE ALIAS <alias> FOR DATABASE <database>`). For an existing one,
   create an alias with a different name and move the application to it; reusing the same name
   needs a migration ([`bootstrap/adopt.sh`](bootstrap/adopt.sh) shows the steps).
3. Grant the customer's role on the database. Grants made through the alias are stored
   against the database behind it; see [the restore runbook](#restore-on-a-customers-request) for
   what that means after a restore.
4. Add a group to the policy. Usually that's a profile and the alias:

   ```yaml
   - id: shop
     profile: app-live          # tier gold, 14 days; or data-interval, data-scratch, data-then-live
     aliases: [shop]
   ```

   Set anything that differs from the profile on the group (`retention_days: 30`, or one
   `load:` key). Check what the group ends up with, and where each value comes from, with
   `neo4j-backup policy show shop`, and check the file before it ships with
   `neo4j-backup policy validate <file>` (exit 0 valid, 2 invalid; no cluster or store needed,
   so it suits a pull-request check on the policy repo). Then deliver the policy
   (a file the workers read, or `s3://…` with `NEO4J_BACKUP_POLICY`). Dagster's
   `reconcile_registry` sensor adds the new targets within five minutes; Airflow picks them up at
   the next DAG parse.
5. If the tier is new, switch on its schedules: Dagster creates them stopped, unless the policy
   says `schedules: running`.
6. Run the first backup now rather than waiting for the schedule, then verify it:
   `neo4j-backup backup <group> --kind FULL` and `neo4j-backup verify <group>`.
7. Tell the customer their tier, how far back they can restore (`retention_days`), and how to
   ask for a restore. The [Customer guide](CUSTOMERS.md) is written for them.

Give a customer's groups a `tenant:` label (or put it in their profile) to act on all of them at
once: `neo4j-backup backup --tenant acme`, `verify --tenant acme`, `targets --tenant acme`. It's a
label for selecting groups, not access control.

**Planned:** an `adopt` command ([#92](https://github.com/lex00/neo4j-backup/issues/92)).

## Check that customers are covered

- Runs: failed backups show as failed runs in Dagster or Airflow, or as a non-zero exit from
  the CLI in CI or cron.
- Freshness: the MCP tool `backup_status` lists every target's newest backup and its age in
  hours ([MCP.md](MCP.md)).
- Restorability: `neo4j-backup verify <group>` copies the latest backups aside, combines them
  into a full backup and runs a consistency check, without touching the cluster. It runs on each
  tier's `verify_cron` (default daily 06:00): Dagster's `<tier>_verify` schedule, Airflow's
  `neo4j_verify_<tier>`. Aggregate and prune run the same way (`aggregate_cron`, `prune_cron`), and
  the metadata export and `system` backup on the policy's `dbms:` crons (#104).

- Restore points: `neo4j-backup restore-points <group>` (or `--tenant <t>`) shows what each member
  can be restored to, and any gaps.

**Planned:** freshness checked against each group's `rpo_minutes`
([#123](https://github.com/lex00/neo4j-backup/issues/123)).

## Restore on a customer's request

### 1. Get the request straight

Ask the customer for the group or alias, the point in time in UTC (or "latest"), and your ticket
reference. Check that they understand a restore covers every database in the group: they all go
back to the same instant.

### 2. See what can be restored

```bash
neo4j-backup --json restore-points <group>      # or --tenant <t>
```

Per member: the database the alias serves now, each backup chain with the window it covers
(`from` to `to`, per physical, older physicals included), the load artifacts, and gaps (diffs
missing in a window, or no backups yet of the database the alias serves since the last swap),
each with the restore command that reaches it. It only reads the bucket (the artifact metadata,
#118) and asks Neo4j which database each alias serves; it never runs neo4j-admin, so read-only
credentials are enough. The MCP tool is `restore_points`.

A point in time is reachable when it falls inside a chain's window, including a chain of a
database the alias pointed at before an earlier restore: with `--until`, the restore picks, per
member, the chain whose window covers the time ([#114](https://github.com/lex00/neo4j-backup/issues/114)).
A member with no change after the time (only a full, or diffs with no transactions) is seeded
whole from its last backup. If no chain covers the time for some member, the restore stops
before changing anything and lists the nearest windows.

### 3. Tell the customer what will happen

- Writes after the chosen time are gone.
- Transactions running at the moment of the switch are aborted and rolled back; applications must
  retry. On a cluster, members may briefly disagree about where the alias points.
- Their users keep access: the restore copies every privilege on the old database to the new one
  before the switch.

### 4. Preview

```bash
neo4j-backup restore <group> --dry-run                                   # latest
neo4j-backup restore <group> --until 2026-11-15T09:00:00Z --dry-run      # point in time
```

The plan lists the backup each database will come from, any database that would be dropped
(only in by-name groups with `--replace`), and per member the privileges that will be carried over
(`privileges`: the grants on the database the alias points at now).

### 5. Restore

```bash
neo4j-backup --json restore <group> --until 2026-11-15T09:00:00Z --confirm \
  --ticket CHG-1234 --reason "customer request: bad import at 09:05" > restore.json
```

`--ticket` and `--reason` ([#119](https://github.com/lex00/neo4j-backup/issues/119)) are written
to the log and returned in the result; every mutating command takes them (`aggregate`, `prune`,
`metadata restore` too), as do the MCP write tools (`ticket`, `reason`), the Dagster restore job
(config; also set as the run tags `neo4j_backup/ticket` and `neo4j_backup/reason`) and the Airflow
restore DAG (params). The tool records them; approval stays with your own process. Set
`NEO4J_BACKUP_REQUIRE_TICKET=true` to refuse any applied change without a ticket (exit 3,
`ticket_required`); dry runs never need one.

The same restore from the orchestrators:

- Dagster: launch the `restore_group` job with config
  `{ops: {restore_group_op: {config: {group_id: <group>, restore_until: "2026-11-15T09:00:00Z", ticket: CHG-1234, reason: "…"}}}}`.
- Airflow: `airflow dags trigger neo4j_restore -c '{"group_id": "<group>", "restore_until": "2026-11-15T09:00:00Z", "ticket": "CHG-1234", "reason": "…"}'`.

The result lists, per member, the old database (`old`), the new one (`newdb`) and the privileges
granted on the new one (`privileges`). Keep it for the report.

`--member <alias>` (repeatable) restores only those members (#121); the dry-run warns that they
will no longer be aligned in time with the rest of the group. Privileges, the baseline and the
`--until` chain choice apply per member as usual.

`--check` runs the group's `load.checks` (#111) on each new database before the swap; a failed
check drops the new databases (keep them with `--keep-failed`) and moves no alias. The result has
each member's check results under `checks`.

To seed from a load artifact instead of the backups (#144, #159): `--from-load` (every member's
newest dump in its `_seed/` folder), or `--from-dump <member>=<key-or-uri>` per member (a key under
`_seed/`, or the URI of a dump a customer exported, in a bucket the servers can read). A dump
restores whole, so neither goes with `--until`; `--until` itself never picks a dump. A member with
no backups but a load is seeded from its newest load automatically.

Neo4j stores privileges against the database, not the alias, so the restore re-grants every
privilege on the old database onto the new one before it moves any alias. Grants on `*` and on
`HOME` don't name a database and are left as they are. If a grant fails, the restore stops before
the switch and every alias still points at its old database. In a by-name group with `--replace`,
the privileges are read before the drop and granted again after the create (DROP DATABASE removes
them).

### 6. Check the baseline backup

The restored databases start with no backups of their own, and a differential needs a full of the
same database. So the restore ends by taking a full backup of each database it created, the
baseline ([#97](https://github.com/lex00/neo4j-backup/issues/97)); the result lists them under
`baseline`. Point-in-time coverage of the new databases starts there. In Dagster it is the
`baseline_op` step of `restore_group` (the job runs with the `backup_kind: full` tag, so it queues
behind a running full); in Airflow it is the `baseline` task in the `neo4j_full` pool. Skip it with
`--no-baseline` (CLI), `baseline: false` (Dagster config, Airflow param, MCP).

If the baseline fails, the restore is still applied: the CLI exits 1 with error kind
`baseline_failed` and the restore result, and you rerun `neo4j-backup backup <group> --kind FULL`.
Until a full exists, the diff schedule sends those members to the full lane and
`backup --kind DIFF` lists them under `needs_full`
([#74](https://github.com/lex00/neo4j-backup/issues/74)). The same holds after a bulk load, an
adopt, and a by-name restore with `--replace`.

### 7. Report back

Send the customer the restore point, the time of the switch, and the old and new database names.

### Undo a restore

The old database is still there until cleanup. Point the aliases back:

```bash
neo4j-backup --json rollback <group> --dry-run          # [--member <alias>]
neo4j-backup --json rollback <group> --confirm --ticket CHG-1234
```

Every restore, load and rebuild records each alias's previous database in
`_state/<group>.json` in the group's store (until the records layer exists, #100), and rollback
uses the most recent swap. It refuses, before changing anything, if the previous database no
longer exists. Privileges granted on the current database since the swap are copied back first,
and no baseline is taken (the old chain is still there). A rollback is itself recorded, so
running it again goes forward again. In Dagster it's the `rollback_job`, in Airflow
`neo4j_rollback`, in MCP `run_rollback`.

### Clean up later

After a soak period, drop the superseded databases:

```bash
neo4j-backup --json cleanup <group> --dry-run                 # the exact DROP set
neo4j-backup --json cleanup <group> --keep 1 --older-than 7 --confirm --ticket CHG-1234
```

Cleanup only touches databases the naming scheme says the tool created for a member (`owns`,
#89) and that no alias targets. It always keeps the current target and the `--keep` newest
before it (default 1, for `rollback`). Databases you created yourself are never candidates.
Privileges copied to a database (#117) are revoked before it's dropped. It can also run on the
tier's `cleanup_cron` (Dagster `<tier>_cleanup`, Airflow `neo4j_cleanup_<tier>`); MCP has
`run_cleanup`. by-name groups have nothing to clean up.

### Limits today



## Load a customer's data in bulk

Give the group a `load:` block (POLICY.md): `mode` (`once` or `rebuild`), `writes`, and a `build`
of `kind: import` whose `args` name the input files relative to the source folder
(`load.source`, default `<group>/_source/` in the group's store; one `<slug>/` folder per member
when the group has several). Put the files there, then:

```bash
neo4j-backup --json load <group> --dry-run        # source objects, build argv, target, what is lost
neo4j-backup --json load <group> --confirm --ticket CHG-1234
```

For each member the runner imports the files (neo4j-admin reads them from the store), dumps the
offline store into the member's `_seed/` folder and removes it; the cluster then seeds a new
database from the dump. The new databases are set read-only for `writes: read-only`, checked
(online, counts equal to the import's, and the group's `load.checks`, #111), and only when every
member passed are the privileges copied and the aliases moved, all together. A failure before the
swap drops the new databases (keep them with `--keep-failed`) and names the failed step. The
baseline backup follows if the group backs up its writes. by-name groups need `--replace` to drop
and recreate an existing database.

A group switched from read-write to read-only takes effect at its next load; for `mode: once`
there is none, so run `neo4j-backup load <group> --reapply-access --confirm`.

The manual route in [IMPORT.md](IMPORT.md) still works for builds the tool doesn't run.

Scheduled rebuilds come from the tier's `rebuild_cron` (#108): Dagster's `<tier>_rebuild`
schedule (one `load_job` run per rebuild group) and Airflow's `neo4j_rebuild_<tier>` DAG; on demand,
Dagster's `load_job` and Airflow's `neo4j_load`. A group's rebuild never overlaps its own backups:
in Dagster through the `neo4j_group` tag limit (sample `dagster.yaml`), in Airflow through a lock
object (`_locks/<group>.lock`) that backups and loads both take, waiting up to
`NEO4J_BACKUP_LOCK_WAIT` seconds (default 3600). A failed rebuild is a failed run; the previous
load keeps serving. The first load of a new group creates its alias.

### Custom builds (#109)

When `neo4j-admin database import` can't read the source (RDF is the usual case), run your own
converter as `build.kind: command`. The tool doesn't ship converters; this is an example:

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

The command runs in a fresh work directory (its current directory, also `WORK_DIR`) with
`SOURCE_URI` (the group's source folder in the store), `OUTPUT_URI`, `NEO4J_BACKUP_GROUP` and
`NEO4J_BACKUP_MEMBER` set. In the runner the command, the import, the dump and the cleanup run
as one step. An `image` (k8s runner modes) runs as its own pod and must write its CSV or Parquet
to `OUTPUT_URI`, which the import then reads. A non-zero exit, the timeout, or a missing output
fails the load before anything is seeded; the command's output is in the run logs (CLI stderr,
Dagster and Airflow task logs).

## Change a customer's tier

Change the group's `tier` in the policy. The group moves to that tier's schedules. If you move to a
tier whose full backups are further apart than the group's `retention_days`, read
[#73](https://github.com/lex00/neo4j-backup/issues/73) first.

## Offboard a customer

Remove the group from the policy; its schedules stop. Its backups stay in the bucket and nothing
deletes them: remove `s3://<bucket>/<group>/` yourself once your retention obligations allow, and
drop the customer's databases and aliases on the cluster.

**Planned:** a `retired` state with a final retention period
([#124](https://github.com/lex00/neo4j-backup/issues/124)).

## Recover after losing a cluster, or move a group

With both clusters in the policy's `clusters:` (#106), `restore --to-cluster` restores a group
into another cluster (#115):

```bash
# one group; the source is the group's cluster in the policy
neo4j-backup --json restore orders --to-cluster west --dry-run
# every group of a lost cluster, in one plan
neo4j-backup --json restore --from-cluster east --to-cluster west --confirm --ticket INC-77
# the source still answers (a planned move): stop the groups' writes there
neo4j-backup --json restore orders --to-cluster west --fence-source --confirm
```

Before anything changes, it checks that the target is in the policy and answers, that every
member has a seed (`--until` works as for a restore), and that the source either doesn't answer
or `--fence-source` was given. Then, on the target:

1. It replays the source's users, roles and DBMS-wide privileges, live from the source if it
   answers, else from its newest metadata export (`--no-metadata` skips this when the target
   has them already). Native passwords aren't in an export, so users get a placeholder and
   CHANGE REQUIRED.
2. It seeds every member. The target's servers must be able to read the group's store; if they
   can't, the seed fails here and no alias has moved.
3. It grants the members' privileges, rewritten from the source's database names to the new
   ones (#117), then creates each alias (or swaps it, if the target has it) and takes the
   baseline fulls on the target.
4. It fences the groups on the source in `_state/<group>.json`: backups, restores and loads that
   the policy still sends to the source refuse, so two clusters never write one chain.

The policy isn't edited. If the move is permanent, set `cluster: west` on the groups; that also
lifts the fence. In Dagster it's the `move_job`, in Airflow `neo4j_restore_to_cluster`, in MCP
`run_restore_to_cluster`. `--member`, `--from-load`, `--from-dump`, `--replace` and `--check`
don't combine with `--to-cluster`.

## Notice changes customers make directly

Customers with cluster credentials can change things the tool depends on. The most important: if
an alias is moved to another database, backups follow the alias to a database with no full backup,
and differential backups fail there until the next full one. Check `SHOW ALIASES` against your last
restore results when backups for a group start failing.

## Settings

Everything below is optional and defaults to the tested behaviour. The full list is the
[environment table](orchestrator/README.md#environment-variables).

- Credentials: `SECRET_PROVIDER` reads the Neo4j password from AWS Secrets Manager (or your own
  provider) at each connection, so rotation needs no redeploy.
- Bolt retries: transient cluster errors (leader changes, dropped sessions, expired tokens)
  are retried a bounded number of times, chosen by Neo4j status code.
- Seed topology: set `topology` in the policy so a restore comes back with the intended number
  of primaries and secondaries instead of the cluster default.
- Cypher version: `SEED_CYPHER_VERSION=5` on a Cypher 5 cluster (adds the `existingData`
  option it requires).
- Cutover: the default moves the alias; `CUTOVER_STRATEGY=external` calls your hook to repoint
  a router or proxy instead.
- Storage layout: `PATH_LAYOUT` brings your own bucket key scheme.
- Database names: `NAMING=module.Class` brings your own scheme (#89). For example, two fixed
  names per alias that a restore flips between:

  ```python
  class BlueGreen:
      def slug(self, alias): return alias
      def physical(self, alias, ts, current=None):
          return f"{alias}-green" if current == f"{alias}-blue" else f"{alias}-blue"
      def owns(self, alias, name): return name in (f"{alias}-blue", f"{alias}-green")
  ```

  Every name it returns is checked against Neo4j's rules. A restore refuses a name that is
  already in use, so with blue/green, clean up (or drop) the idle one first. The shell scripts
  under `bootstrap/` use the default scheme only.
- Policy source: `NEO4J_BACKUP_POLICY=s3://…` lets you change the policy without a redeploy,
  with a cache and last-known-good fallback (`POLICY_CACHE_TTL`); `POLICY_LOADER` for an
  authenticated source such as Vault. If the policy can't be loaded when Dagster or Airflow
  starts, the Dagster code location fails to load and the Airflow backup DAG file shows an import
  error, both with the policy error; a fetch error is retried a few times (1, 2, 4 seconds) first.
  With no policy configured at all (`NEO4J_BACKUP_POLICY` unset and no `policies/demo.yaml`), no
  schedules are built.
- Encryption on every write: `S3_SSE` adds the SSE-KMS header to the tool's own writes, and
  `BACKUP_UPLOAD=pipeline` sends neo4j-admin's backups through the same path, for buckets that
  reject writes without the header.

## Commands

| Command | Does | Changes anything? |
|---|---|---|
| `targets` | lists the policy's groups and members | no |
| `policy show <group>` / `policy validate <file>` | a group's effective settings and their source / check a policy file | no |
| `export <group> [--until <iso>]` | a portable `.dump` of each member at a point in time (or the latest backup), into its `_seed/` folder; the cluster is never touched | writes dumps |
| `policy check` | can the tool list and write every store the policy uses; which stores the servers must read | writes and deletes one probe object per store |
| `backup <group> [--kind AUTO\|FULL\|DIFF]` | backs up every database in a group | adds backups |
| `verify <group>` | checks the latest backups restore and are consistent | no |
| `aggregate <group>` | combines each chain into one full backup, in place | yes |
| `restore <group> [--until <time>] [--replace]` | restores a group | yes |
| `prune` | deletes backups older than each group's retention, whole chains only; the preview reports `kept_for_chains` (past retention but needed) and `unknown_kept` (no metadata yet) | yes |
| `metadata export` / `metadata restore [--key <k>]` | users, roles, privileges and aliases as Cypher | restore changes the cluster |
| `system-backup` | full backup of the `system` database | adds a backup |
| `import [--to-dump <group>/<member>] <database> -- <args>` | bulk import on a loader; with `--to-dump`, dump it into the member's `_seed/` folder ([IMPORT.md](IMPORT.md)) | yes |

Every command follows the [CLI contract](CLI-CONTRACT.md): `--json` prints one result object, exit
codes are stable for scripts, and commands that change things need `--confirm` (preview with
`--dry-run`). [AGENTS.md](AGENTS.md) covers driving the CLI from an agent; [MCP.md](MCP.md) the
operator MCP server.
