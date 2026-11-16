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

**Planned:** freshness checked against each group's `rpo_minutes`
([#123](https://github.com/lex00/neo4j-backup/issues/123)), a `restore-points` command
([#113](https://github.com/lex00/neo4j-backup/issues/113)).

## Restore on a customer's request

### 1. Get the request straight

Ask the customer for the group or alias, the point in time in UTC (or "latest"), and your ticket
reference. Check that they understand a restore covers every database in the group: they all go
back to the same instant.

### 2. See what can be restored

List the group's backups in the bucket:

```bash
aws s3 ls --recursive s3://$BACKUP_BUCKET/<group>/
```

Each backup has a small `<artifact>.meta.json` next to it, written from
`neo4j-admin backup inspect` after every backup and aggregate: full or differential, the
transaction range, the time the backup was taken, and the store. The MCP tool
`show_chain(group, member)` reads them and lists each chain (a full and the differentials after
it, per database folder) with the window it covers (`from`, `to`). A point in time is reachable
when it falls inside a chain's window, including a chain of a database the alias pointed at
before an earlier restore: with `--until`, the restore picks, per member, the chain whose window
covers the time ([#114](https://github.com/lex00/neo4j-backup/issues/114)), and the dry-run shows
it under each member's `seed` (`chain`, and `until` or `whole`). A member with no change after the
time (only a full, or diffs with no transactions) is seeded whole from its last backup. If no chain
covers the time for some member, the restore stops before changing anything and lists the nearest
windows. Backups taken before this was added are listed under `unknown` until the next backup or
aggregate of that database fills them in; `--until` doesn't consider them.

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

The old database is still there. Point the alias back:

```cypher
ALTER ALIAS `<alias>` SET DATABASE TARGET `<old database>`
```

Its privileges were never removed, so access works again straight away.

### Clean up later

After a soak period, drop the old databases (`DROP DATABASE <old>`). Dropping a database also
removes the privileges granted on it.

### Limits today

- A restore always covers the whole group; one database alone isn't possible
  ([#121](https://github.com/lex00/neo4j-backup/issues/121)).

**Planned:** `rollback` ([#122](https://github.com/lex00/neo4j-backup/issues/122)) and `cleanup`
([#93](https://github.com/lex00/neo4j-backup/issues/93)).

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

**Planned:** custom build steps such as RDF conversion
([#109](https://github.com/lex00/neo4j-backup/issues/109)).

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

## Recover after losing a cluster

Not yet tested end to end. The pieces exist:

1. Bring up the replacement cluster and give its servers read access to the bucket.
2. Restore users, roles and privileges: `neo4j-backup metadata restore --confirm` replays the
   latest export (passwords aren't in it), or restore the `system` database offline for exact
   copies ([RECOVERY.md](RECOVERY.md#the-metadata-layer-users-roles-privileges-aliases)).
3. Point the tool at the new cluster (`NEO4J_BOLT_URI`, `NEO4J_BACKUP_SOURCE`) and restore each
   group. The grants replayed in step 2 name the old cluster's physical databases, so the restore
   has nothing to copy from. Check that each customer role reads through its alias, and if not,
   copy the grants by hand from the old physical name (see the privileges note in
   [RECOVERY.md](RECOVERY.md)). Grants on `*` carry over as they are.

**Planned:** several clusters in one policy ([#106](https://github.com/lex00/neo4j-backup/issues/106))
and restoring a group into another cluster ([#115](https://github.com/lex00/neo4j-backup/issues/115)).

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
