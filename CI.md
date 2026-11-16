# Scheduling backups from CI

Many teams have CI (often self-hosted GitLab or Forgejo) or plain cron, but no Dagster/Airflow.
The [`neo4j-backup` CLI](CLI-CONTRACT.md) runs the same policy-driven backup/restore over the shared
core, so a CI job is "run the CLI on a schedule." Ready-to-copy templates live in
[`examples/ci/`](examples/ci/): [GitHub Actions](examples/ci/github-actions.yml),
[GitLab CI](examples/ci/gitlab-ci.yml), [Forgejo Actions](examples/ci/forgejo-actions.yml).

## Who this is for

Small fleets and non-orchestrator teams. CI is weak at the orchestration this project's value rests
on — concurrency lanes, dynamic policy fan-out, multi-TB scratch, and observability. If you run
dozens of databases across tiers with tight RTOs, use the Dagster or Airflow adapter. If you run a
handful and already have CI, the CLI path is a real option with the limits below stated plainly.

## Execution model: the CI runner *is* the backup runner

The job must run somewhere that can actually take a backup — DESIGN.md's central runner. That host
(a **self-hosted** CI runner) needs:

- `neo4j-admin` on `PATH` (the Neo4j Enterprise tooling);
- network to the database **backup port** (`6362`) and to Bolt (`7687`);
- egress to your object store;
- a **scratch disk sized for the largest full backup** (`SCRATCH_PATH`), on its own volume.

GitHub-hosted runners (≈14 GB disk, 6 h cap, no `neo4j-admin`) suit only tiny/demo databases. A
Forgejo/Gitea Actions runner is itself a natural backup runner — ephemeral compute with secrets,
sited next to the database.

## Secrets → environment

The CLI reads the same environment as the adapters (see the orchestrator README env table). Put
credentials in your CI's secret store and the rest in plain variables:

| Variable | Where | Notes |
|---|---|---|
| `NEO4J_PASSWORD` | secret | or `NEO4J_PASSWORD_REF` with `SECRET_PROVIDER=aws-sm` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | secret | or the azure/gcp credentials for `CLOUD` |
| `NEO4J_BOLT_URI` | var | `neo4j://db-host:7687` |
| `NEO4J_BACKUP_SOURCE` | var | `db-host:6362` (backup port) |
| `BACKUP_BUCKET`, `AWS_REGION`, `CLOUD` | var | `CLOUD` = `aws` (default) / `azure` / `gcp` |
| `NEO4J_BACKUP_POLICY` | var | a repo path, or `s3://…` (loaded with TTL cache) |
| `RUNNER_PAGECACHE` | var | set explicitly, or `neo4j-admin` inherits the server pagecache and OOMs |
| `SCRATCH_PATH` | var | a sized volume, not the runner's install disk |

## Install

Not on PyPI — pin a git tag (or vendor the source and `pip install ./orchestrator`):

```
pip install "neo4j-backup-dagster @ git+https://github.com/lex00/neo4j-backup@v0.5.0#subdirectory=orchestrator"
```

Add a cloud extra if you use Azure/GCS — put it on the package name:
`"neo4j-backup-dagster[azure] @ git+…#subdirectory=orchestrator"` (or `[gcp]`).

## Policy crons → CLI commands

The orchestrators read every schedule from the policy (#104). A cron or CI job runs the matching
command on the same cron:

| Policy cron | Command |
|---|---|
| `tiers.<t>.full_cron` | `neo4j-backup backup <group> --kind FULL` for each group on tier `<t>` |
| `tiers.<t>.diff_cron` | `neo4j-backup backup <group> --kind DIFF` (then `--kind FULL` if `result.needs_full` lists any, #74) |
| `tiers.<t>.verify_cron` | `neo4j-backup verify <group>` |
| `tiers.<t>.aggregate_cron` | `neo4j-backup aggregate <group> --confirm` |
| `tiers.<t>.prune_cron` | `neo4j-backup prune --confirm` (all groups and the DBMS sweep; one job is enough) |
| `tiers.<t>.rebuild_cron` | `neo4j-backup load <group> --confirm` for each `load.mode: rebuild` group on tier `<t>` (never at the same time as that group's backups: one job per group, or a lock in your CI) |
| `dbms.metadata_cron` | `neo4j-backup metadata export` |
| `dbms.system_backup_cron` | `neo4j-backup system-backup` |

`neo4j-backup targets --tenant <t>` (or `targets <group>`) lists what a job covers.

## Gating and lanes

- **Exit codes gate the job.** The CLI returns non-zero on failure (`1`), bad args (`2`), or a
  refused guard (`3`); a scheduled job fails loudly on any of these. Use `--json` for
  machine-readable logs.
- **Serialize a lane.** Never overlap two backups of one group. GitLab's `resource_group` is the
  cleanest fit; GitHub/Forgejo use a `concurrency` group. Drive full vs differential from separate
  schedules (a `LANE`/`kind` variable), the CI analogue of the orchestrators' full/diff pools.
- **Destructive commands in automation.** `prune` (and `restore --replace`) require `--confirm`; in
  a scheduled job you pass it because the schedule is the operator's standing approval. Run
  `--dry-run` by hand first to see the blast radius.

## Caveats (read before relying on this)

- **Scratch and time limits.** Hosted runners cannot stage a multi-TB full; use self-hosted with a
  sized volume. A full backup can exceed a hosted runner's job timeout.
- **Cron is best-effort.** GitHub/Forgejo scheduled crons drift and occasionally skip under load —
  fine for backups with slack in the RPO, not for tight windows.
- **No orchestration.** You get scheduling and serialized lanes, not dynamic partition fan-out,
  retries with backoff across a fleet, or the run-level observability the Dagster/Airflow adapters
  give. Verify (`verify`) and prune on their own cadence; watch job outcomes yourself.
