# Scheduling backups from CI

A CI job or cron can run the [`neo4j-backup` CLI](CLI-CONTRACT.md) on a schedule in place of Dagster or Airflow.
Templates: [GitHub Actions](examples/ci/github-actions.yml), [GitLab CI](examples/ci/gitlab-ci.yml) and
[Forgejo Actions](examples/ci/forgejo-actions.yml).
This suits a handful of databases. With dozens across tiers, use Dagster or Airflow.

## The runner

The CI runner takes the backups, so use a self-hosted runner near the database. It needs:

- `neo4j-admin` (Neo4j Enterprise tooling) on `PATH`.
- Network access to the backup port (`6362`), Bolt (`7687`) and your object store.
- A scratch volume sized for the largest full backup (`SCRATCH_PATH`).

GitHub-hosted runners (about 14 GB disk) suit demo databases. Production needs a runner with `neo4j-admin` installed.

## Environment

Keep credentials in CI secrets. All variables are in [ENVIRONMENT.md](ENVIRONMENT.md).

| Variable | Where | Notes |
|---|---|---|
| `NEO4J_PASSWORD` | secret | or `NEO4J_PASSWORD_REF` with `SECRET_PROVIDER=aws-sm` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | secret | or the Azure or GCP credentials for `CLOUD` |
| `NEO4J_BOLT_URI` | var | `neo4j://db-host:7687` |
| `NEO4J_BACKUP_SOURCE` | var | `db-host:6362` (the backup port) |
| `BACKUP_BUCKET`, `AWS_REGION`, `CLOUD` | var | `CLOUD` is `aws` (default), `azure` or `gcp` |
| `NEO4J_BACKUP_POLICY` | var | a repo path, or `s3://…` (cached with a TTL) |
| `RUNNER_PAGECACHE` | var | neo4j-admin's page cache (default `512M`). Raise it for large databases. |
| `SCRATCH_PATH` | var | the sized scratch volume |

## Install

Install from a pinned git tag (the package lives on GitHub only):

```
pip install "neo4j-backup-dagster @ git+https://github.com/lex00/neo4j-backup@v0.7.0#subdirectory=orchestrator"
```

For Azure or GCS, add the extra to the package name: `"neo4j-backup-dagster[azure] @ git+…"` (or `[gcp]`).

## Policy crons to commands

| Policy cron | Command |
|---|---|
| `tiers.<t>.full_cron` | `neo4j-backup backup <group> --kind FULL` for each group on tier `<t>` |
| `tiers.<t>.diff_cron` | `neo4j-backup backup <group> --kind DIFF`, then `--kind FULL` for any in `result.needs_full` |
| `tiers.<t>.verify_cron` | `neo4j-backup verify <group>` |
| `tiers.<t>.aggregate_cron` | `neo4j-backup aggregate <group> --confirm` |
| `tiers.<t>.prune_cron` | `neo4j-backup prune --confirm` (covers all groups and the DBMS sweep, so one job is enough) |
| `tiers.<t>.rebuild_cron` | `neo4j-backup load <group> --confirm` for each `load.mode: rebuild` group on tier `<t>` |
| `dbms.metadata_cron` | `neo4j-backup metadata export` |
| `dbms.system_backup_cron` | `neo4j-backup system-backup` |

Run a group's rebuild and backups at separate times: one job per group, or a lock in your CI.
`neo4j-backup targets --tenant <t>` (or `targets <group>`) lists what a job covers.

## Gating and lanes

- The exit code gates the job: `1` failure, `2` bad arguments, `3` a refused guard. Add `--json` for parseable logs.
- Jobs on one group wait for its lock (up to `NEO4J_BACKUP_LOCK_WAIT`). Queue them in CI as well.
  Use GitLab's `resource_group` or a GitHub or Forgejo `concurrency` group.
  Forgejo runners older than v11.1 need `capacity: 1` instead.
- Run fulls and diffs from separate schedules, for example with a `LANE` variable.
- `prune`, `aggregate` and `restore --replace` need `--confirm`. In a scheduled job the schedule is the approval.
  Run `--dry-run` by hand first to see what it would delete.

## Limits

- A multi-TB full can exceed a job timeout.
- Scheduled crons on GitHub and Forgejo drift and sometimes skip. Leave slack in the RPO.
- Fan-out, retries and run history are orchestrator features. Watch job outcomes, and check freshness with `neo4j-backup status --stale` ([workflows/coverage.md](workflows/coverage.md)).
