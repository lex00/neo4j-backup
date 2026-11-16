# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: a minor bump for features or breaking changes,
a patch for fixes). See [RELEASING.md](RELEASING.md).

## [Unreleased]

**Upgrading (Airflow).** The fixed `neo4j_verify`, `neo4j_aggregate`, `neo4j_prune`,
`neo4j_metadata_backup` and `neo4j_system_backup` DAGs are replaced by policy-generated ones
(`neo4j_verify_<tier>`, `neo4j_aggregate_<tier>`, `neo4j_prune_<tier>`,
`neo4j_dbms_metadata_default`, `neo4j_dbms_system_default`). Their run history isn't carried
over; unpause the new DAGs.

### Added
- **Restore some members of a group** (#121): `restore --member <alias>` (repeatable; MCP
  `members`, Dagster config `members`, Airflow param `members`). The dry-run warns that they will
  no longer be aligned in time with the rest; privileges, baseline and chain choice apply per
  member.
- **`restore-points <group>`** (#113; MCP `restore_points`): per member, the current database,
  each chain's point-in-time window (older physicals included), the load artifacts, gaps (diffs
  missing in a window, no backups since the last swap), and the restore command for each.
  Read-only; never runs neo4j-admin. Takes `--tenant`.
- **Notice before a load or rebuild** (#126): `load.notice: {hook, before, required}` calls the
  hook (HTTP POST JSON or a command with `NOTICE_*` env vars, the same mechanism as
  `CUTOVER_HOOK`) with the group, tenant, members, swap time and what will be lost, then waits
  `before`. A failed hook is logged and doesn't stop the load unless `required: true`.
- **Custom build steps** (#109): `build.kind: command` runs a customer command in the runner
  (one step with the import and dump, under `timeout`) or an `image` as its own pod in the k8s
  runner modes, with `SOURCE_URI`, `WORK_DIR`, `OUTPUT_URI` and `env` (values `secret:<ref>`
  resolved). `output: import` hands CSV/Parquet to the built-in import; `output: dump` a dump.
  `run_admin` takes an `image` in every adapter.
- **Scheduled rebuilds and orchestrated loads** (#107, #108): Dagster `load_job` and
  `<tier>_rebuild` schedules (from `rebuild_cron`, only `load.mode: rebuild` groups); Airflow
  `neo4j_load` and `neo4j_rebuild_<tier>`. Runs carry a `neo4j_group` tag (the sample
  `dagster.yaml` limits it to 1 per group); Airflow backups and loads take a per-group lock object
  created with the store's conditional write. The first load of a group creates its alias.
  `just load-smoke` covers CSV and Parquet, read-only and read-write. The `dev` extra adds
  `pyarrow` for it.
- **`load <group>`** (#107, core, CLI and MCP): builds each member in the runner from the policy's
  `load.build` (`kind: import`; relative input paths resolve against `load.source`, read by
  neo4j-admin straight from the store), dumps it, seeds a new database from the dump, sets it
  read-only for `writes: read-only`, runs the checks (with the import's counts), copies
  privileges and swaps every alias together, then takes the baseline. A failure before the swap
  drops the new databases and names the step. `--reapply-access` applies `writes` without a load.
- **`writes` and `protect_writes` enforced** (#112). A `writes: read-only` group's new databases
  are set read-only before the swap (restores included). Backups follow the live access mode and
  `protect_writes`: a read-only database, or a group whose writes are disposable, is skipped with
  `by_design` in the result (CLI, MCP, Dagster metadata, Airflow); a database that is still
  read-write keeps its backups until a load makes it read-only.
- **Checks before a swap** (#111): `load.checks` in the policy (Cypher returning one number, with
  `== n`, `>= n`, `<= n` or `between a b`), plus default checks (online, non-zero counts, counts
  equal to the import's). `restore --check` runs them before the swap; a failure drops the new
  databases (unless `--keep-failed`) and moves no alias. Loads (#107) run them every time.
- `just dump-smoke` (#161): import -> dump -> seed -> baseline -> diff -> point-in-time restore
  -> export, end to end on the local stack.
- **`export <group> [--until T]`** (#160, part of #98): a portable `.dump` of each member at a
  point in time (or the latest backup), written to its `_seed/` folder. The runner restores the
  covering chain into a throwaway local store, dumps it and removes it in one step; the cluster is
  never touched. Guarded (`--dry-run` shows the chain per member), with `--ticket` / `--reason`.
- **Prune keeps `keep_loads` load artifacts per member** (#157, part of #98): the newest
  `load.keep_loads` dumps in each member's `_seed/` folder stay; older ones go, whatever their
  age. The preview lists them under `loads` (CLI, MCP).
- **Restore from load artifacts** (#144, #159, part of #98): `restore --from-load` (every
  member's newest dump) and `--from-dump MEMBER=KEY_OR_URI` (a key in the member's `_seed/`, or an
  external dump URI the servers can read); also in MCP, the Dagster restore config and the Airflow
  restore params. `--until` never picks a dump and is refused with one; a member with no backups
  seeds from its newest load. Privileges and the baseline apply as for any restore.
- **`import --to-dump <group>/<member>`** (#158, part of #98): import on the loader, dump the
  offline store (no server start), and put the dump in the member's `_seed/<ts>/` folder;
  `BACKUP_UPLOAD=pipeline` uploads it through the tool. IMPORT.md makes it the default route.
- **Load artifacts** (#156, part of #98). A `.dump` (or a `.backup` kept as a load) lives in
  `<group>/<slug>/_seed/`; `PathLayout.load_prefix` names it and `list_load_artifacts` lists it
  on every backend, while `list_artifacts` never returns anything under `_seed/`, so a load
  never becomes a chain head. MCP `show_chain` returns `loads`; `backup_status` shows the latest.
- **DBMS jobs and concurrency per cluster** (#106). The metadata export, `system` backup,
  metadata restore and DBMS sweep run per cluster (CLI `--cluster`; Dagster
  `<cluster>_dbms_*` schedules; Airflow `neo4j_dbms_*_<cluster>` DAGs). The default cluster keeps
  `_dbms/`; a named cluster writes under `_dbms/<cluster>/`; metadata listings now use the exact
  prefix `_dbms/metadata-`. Dagster runs carry a `neo4j_cluster` tag (the sample `dagster.yaml`
  limits it per cluster); Airflow backup DAGs use per-cluster pools `neo4j_<cluster>_<lane>`
  (default cluster unchanged). Cluster names `system` and `metadata…` are reserved.
- **Several clusters in one policy** (#106, first part). `clusters:` names each Neo4j DBMS
  (`bolt_uri`, `user`, `password_ref`, `backup_source` (one or a list), `storage`, `topology`,
  `cypher_version`, `cutover`, `backup_upload`); a group sets `cluster:` (else `default`, from the
  environment as before). The Neo4j client, runner and store are resolved per group in every op;
  `targets` and the MCP status show each group's cluster. New validation: group ids must be
  unique (`Policy.group` used to return the first match), and an alias or database belongs to
  one group per cluster.
- **Per-store settings for neo4j-admin, secret references, `policy check`** (#105). A named S3
  store's `credentials_ref` (a key pair) and `region` / `runner_endpoint` reach both the tool and
  neo4j-admin (`run_admin` takes an `env`; with a `docker … exec` prefix the names are passed as
  bare `-e NAME`, so values stay out of argv). One secret-reference syntax, `<provider>:<ref>`.
  `neo4j-backup policy check` probes every store and names those the servers must read.
- **Named stores** (#105, first part). A `storage:` map names object stores (`url`, `region`,
  `endpoint`, `sse`, `sse_kms_key_id`, `write_args`, `credentials_ref`); a group picks one with
  `storage:`, else `default`, which is the environment's store unless the policy defines it. Every
  op resolves the store from the group (backup, verify, aggregate, prune, restore, MCP status).
  Unknown store names fail policy load. `s3_prefix` is optional and deprecated (a warning; never
  read).
- **Every schedule comes from the policy** (#104). Tiers gain `verify_cron`, `aggregate_cron`,
  `prune_cron` (defaulting to the old fixed crons on tiers that take backups) and `cleanup_cron`
  (for #93); a top-level `dbms:` block sets `metadata_cron`, `system_backup_cron`,
  `keep_metadata` and `keep_system`; `schedules: running` makes the generated Dagster schedules
  start running. Dagster gains `<tier>_verify`, `<tier>_aggregate`, `<tier>_prune`,
  `default_dbms_metadata` and `default_dbms_system`; Airflow's DAGs are generated the same way.
  Prune runs per tier over that tier's groups; the DBMS sweep runs after each metadata export and
  system backup. CI.md maps each cron to its CLI command.
- **`tenant` label** (#102). Groups (and profiles) take `tenant:`; `owner` is still read as the
  tenant, with a deprecation warning. `backup`, `verify`, `aggregate` and `targets` take
  `--tenant <t>` instead of a group (group and `--tenant` together, or an unknown name, is exit 2);
  `targets` and the MCP status show each group's tenant; MCP `list_targets`, `backup_status`,
  `run_backup`, `run_verify` and `run_aggregate` take `tenant`. A label for selecting groups, not
  access control; `restore` stays per group.
- **Profiles** (#101). `profiles:` names a customer type's settings once; a group sets
  `profile:` and overrides what differs (`load:` merges per key). Applied before validation, in
  one place, so every front-end sees the resolved groups. `neo4j-backup policy show <group>`
  prints a group's effective settings and where each came from; `neo4j-backup policy validate
  <file>` checks a file without a cluster or store (exit 2 if invalid). The demo policy ships
  `app-live`, `data-interval`, `data-scratch` and `data-then-live`, and a `nightly` tier with
  only `rebuild_cron`.
- **`load:` block in the policy** (#103): `mode` (live, once, rebuild), `writes`, `protect_writes`,
  `keep_loads`, `baseline`, `source` and `build`, with validation of the combinations (errors name
  the group). Groups without it behave as before. Tiers gain `rebuild_cron`; `full_cron` and
  `diff_cron` become optional, so a tier for load-only groups can have only `rebuild_cron` (no
  backup schedules are built for a missing cron). Schema only: loads and rebuilds come with #107
  and #108.
- **`--ticket` and `--reason`** (#119) on every mutating CLI command (`restore`, `aggregate`,
  `prune`, `metadata restore`), the MCP write tools, the Dagster restore job (config, plus run tags
  `neo4j_backup/ticket` / `neo4j_backup/reason`) and the Airflow restore DAG (params). Logged and
  returned with the result. `NEO4J_BACKUP_REQUIRE_TICKET=true` refuses an applied change without a
  ticket (`ticket_required`, exit 3).

## [0.5.0] — 2026-11-15

Phase 1 of #100 (correctness): restores keep customer privileges and take a baseline full,
every backup carries its own metadata, prune and `--until` read real chains, the diff lane
survives a seed, and the Dagster tier schedules work with more than one member.

**Upgrading.** Restore now takes a full backup of each restored database by default
(`--no-baseline` to skip), and the Dagster `restore_group` job carries the `backup_kind: full`
tag and needs the `runner` and `pipes_subprocess_client` resources, as does the `prune` asset.
A policy that fails to load now fails the Dagster code location and the Airflow DAG file instead
of leaving no schedules. `keep_system` / `keep_metadata` of 0 are refused. Backups taken before
this release have no metadata sidecar; the next backup or prune of each database fills it in.

### Added
- **Baseline full backup after every restore** (#97), on by default. The restore takes a FULL of
  each database it created as its last step, so the next diff has a base and point-in-time
  coverage starts straight away. CLI `--no-baseline` / `baseline: false` (Dagster config, Airflow
  param, MCP) skips it. Dagster runs it as `baseline_op` after the cutover and tags the
  `restore_group` job `backup_kind: full`; Airflow runs a mapped `baseline` task in the
  `neo4j_full` pool. A failed baseline leaves the restore applied and reports `baseline_failed`.
- **Artifact metadata** (#118). After every backup and aggregate the tool runs
  `neo4j-admin backup inspect` on the physical's folder and writes `<artifact>.meta.json` beside
  each artifact: full or diff, transaction range, backup time, store id hash, database, and the
  neo4j-admin version. Aggregate rewrites them, prune deletes them with their artifacts, and older
  artifacts without one are filled in on the next backup. `ops.artifact_chains` reads them into
  chains with time windows; the MCP `show_chain` tool returns them. A failed inspect is logged and
  doesn't fail the backup.
- `run_admin` returns neo4j-admin's output in every adapter (#118): the CLI and MCP subprocess,
  Dagster Pipes (subprocess and `PipesK8sClient`), Airflow subprocess and KubernetesPodOperator.
- Policy load warns when a tier's full backups are further apart than a group's `retention_days`
  (#73); `neo4j-backup targets` returns the warnings. Crons the check can't read (`@weekly`, day
  names) are skipped, not rejected.
- Chains are split by store (`databaseId`) as well as folder (#120). A by-name restore with
  `--replace` recreates the database under the same name, and the next AUTO backup writes a new
  full into the same folder; the two stores' backups are now read as separate chains.

### Documentation
- Docs reorganised by reader (#116): **Getting started** (the local loop with real output, then a
  production checklist), an **Operator guide** (onboarding, coverage checks, the restore runbook with
  a tested workaround for #117, bulk loads, offboarding, lost cluster, settings, commands) and a
  **Customer guide** (what backups give you, how to ask for a restore, what happens during one).
  README trimmed to who it's for and a map; the docs site nav follows the readers.
- IMPORT.md corrected (#96): Parquet imports directly, dumps can be seeded, and a seeded database
  needs its own full backup. RECOVERY.md warns about #117. DESIGN.md §9 no longer says several
  clusters work unchanged.

### Fixed
- `import --to-dump` runs the import, the dump and the removal of the local store as one runner
  step (#167): two separate steps broke when each command is its own pod (k8s modes), and the
  leftover store made a second load under the same name fail.
- **`--until` picks the chain that covers the time** (#114). Restore always seeded from the
  member's newest backup, so a time before an earlier restore (in the previous database's chain)
  failed. Now, per member, it picks the chain whose window covers the time, across every physical
  and store, using the sidecars; a member with nothing after the time is seeded whole (Neo4j
  refuses `seedRestoreUntil` on a full or on a diff with no transactions). Every member's choice is
  made before any change, so an uncovered time fails cleanly, listing the nearest windows. The
  dry-run shows the chain per member. Also `preview_restore(until=…)` in MCP.
- `keep_system` and `keep_metadata` mean the same thing (#78): the newest N are kept, N >= 1.
  0 used to delete no system fulls but every metadata export; it is now refused.
- **Point-in-time values are validated** (#77). `--until` (CLI), `until` (MCP) and `restore_until`
  (Dagster config, Airflow param) are parsed as ISO-8601 at the boundary; no offset means UTC; the
  value is normalised (`2026-07-11T14:32:00+00:00`) and parsed again before it is written into
  the seed statement, so raw input never reaches Cypher. A bad value is a usage error (CLI exit 2,
  MCP `usage` error) before any Neo4j call.
- **Names are quoted safely in every DDL statement** (#77). Database and alias names went into
  backticks unescaped, though aliases may contain a backtick. One helper (`cypher.quote`) now
  doubles embedded backticks for CREATE/DROP/STOP DATABASE, CREATE/ALTER ALIAS, the privilege
  copy and the metadata export. The seed URI is checked before it is written into the statement:
  an s3/gs/azb/file URI with no quote or backslash, inside the backup store.
- **S3 deletes in batches of 1000** (#76). `delete_keys` sent every key in one `DeleteObjects`
  call, which S3 rejects above 1000 keys (a large first prune), and ignored per-key errors, counting
  them as deleted. It now batches, tries every batch, returns the number actually deleted, and
  raises `DeleteError` listing the keys that failed.
- **A policy that fails to load is an error, not an empty orchestrator** (#75). Dagster's
  schedules and Airflow's backup DAGs used to vanish silently when the policy didn't parse or
  validate, or an `s3://` source failed at a cold start. Now the Dagster code location fails to
  load and the Airflow DAG file shows an import error, with the policy error. Fetch errors are
  retried with backoff (1, 2, 4 seconds) before failing. Only "no policy configured"
  (`NEO4J_BACKUP_POLICY` unset and the default file absent) builds nothing, and says so in the log.
- **Diff lane after a seed** (#74). A differential needs a full of the same database, so after an
  alias-swap restore, a bulk load, an adopt or a by-name `--replace` every diff failed until the
  next full. Now a member whose current database has no full taken since it was created goes to
  the full lane: the Dagster diff schedule requests a FULL with the `backup_kind: full` tag, the
  Airflow diff DAG runs it in the `neo4j_full` pool, and `backup --kind DIFF` skips it (no
  failure) and lists it under `needs_full`; the CI examples then run `--kind FULL`. Every backup
  result reports `taken`, the type neo4j-admin actually wrote.
- **Dagster tier schedules failed on every tick** when a tier had more than one member: Dagster
  rejects several run requests without a `run_key`. Each request now has one.
- **Prune keeps whole chains** (#73). It used to keep only the newest backup per member, so with
  weekly fulls and `retention_days: 7` (the shipped `silver` and `bronze` tiers) it deleted the
  full that the remaining diffs needed, and the newest backup could no longer be restored. Prune now
  reads the chains from the sidecars (#118) and, per member, keeps the newest chain, every chain
  with a backup inside the window, and the chain covering the start of the window; older chains go
  whole, sidecars with them. Backups with no sidecar are inspected first (CLI, Dagster, Airflow,
  MCP `run_prune`), or kept and counted. The preview reports `kept_for_chains` and `unknown_kept`.
- `smoke_k8s.py` and `smoke_phase6.py` called a removed helper (`_physical_prefix`) and failed at
  start; they use the path layout now.
- **Restores keep customer privileges** (#117). Neo4j stores a privilege against the physical
  database even when it was granted through an alias, so an alias-swap restore locked out every
  customer user without DBMS-wide grants. The restore now re-grants every privilege on each old
  physical onto its new one before any alias moves; a failed grant stops the restore with every
  alias where it was. by-name restores with `--replace` read the privileges before the DROP (which
  removes them) and grant them again after the CREATE. The dry-run plan lists the privileges per
  member, and the result shows what was granted. `just restore` does the same copy.
- **Local stack** (#127): MinIO's images can no longer be pulled, so `just fresh` failed at its
  first step. The stack now uses the floci S3/KMS emulator (`floci/floci:1.5.34`) and the AWS CLI
  (`amazon/aws-cli`) for setup and listing. Host port 9000 still serves S3. **Recreate your `.env`**
  from `.env.example`: `AWS_ENDPOINT_URL_S3` is now `http://s3:4566` and `MINIO_KMS_SECRET_KEY` is
  gone. floci records the bucket's default SSE-KMS but doesn't apply it; see STACK.md.

## [0.4.0] — 2026-07-11

Bulk import off-cluster, plus a docs-coherency pass across the front-end story.

### Added
- **Bulk import** (#16) — `neo4j-backup import <database> -- <neo4j-admin import args>` builds an
  offline store from raw CSV/Parquet on ephemeral hardware (a passthrough over `neo4j-admin database
  import full`, database-first so multi-value `--nodes` can't swallow it). It's the first step of the
  off-cluster tail: import into a **fresh loader's default database** → adopt on first start →
  online-backup to a native `.backup` → seed the real cluster (all existing commands). `IMPORT.md`
  documents the pattern, including the key finding that importing into an *already-registered*
  database **quarantines** the store (validated on Neo4j 2026.05 — use a fresh instance's default
  db). `just import-smoke` validates the whole tail end to end with a throwaway loader container.

### Changed
- **Docs coherency** — `DESIGN.md` (§6.10), `RECOVERY.md`, and `STACK.md` now reference the CLI and
  MCP front-ends over the shared core, not just Dagster/Airflow; the mkdocs site stages the CI
  example recipes and `CHANGELOG`/`RELEASING` so their links resolve on the published site.

## [0.3.0] — 2026-07-11

Scheduler-agnostic reach: a `neo4j-backup` CLI, CI recipes, an agent guide, and an optional
operator MCP server — all thin over one shared engine (epic #58).

### Added
- **Operator MCP server** (#58 P5) — `neo4j-backup-mcp` exposes the operations as
  [MCP](https://modelcontextprotocol.io) tools so an operator drives DR/status through an agent
  (schedulers own the cadence; MCP owns the exceptions). **Read-only by default** (`list_targets`,
  `latest_artifact`, `show_chain`, `backup_status`, `preview_restore`, `preview_prune`);
  `NEO4J_BACKUP_MCP_MODE=read-write` adds the mutating tools (`run_backup`/`run_verify`/
  `run_aggregate`/`run_restore`/`run_prune`), each of which still needs `confirm=true`, previews via
  `dry_run`, and runs **verify-before-drop** for a destructive `run_restore(replace=true)`. stdio
  transport, every call audit-logged. The tool logic (`neo4j_backup_mcp.tools`) is dependency-free
  and tested; the FastMCP wiring is behind the optional `[mcp]` extra with a `neo4j-backup-mcp`
  entry point. Validated live (`just mcp-smoke`).
- **CI recipes** (#58 P3) — copy-and-adapt templates that schedule the `neo4j-backup` CLI without an
  orchestrator: [GitHub Actions](examples/ci/github-actions.yml),
  [GitLab CI](examples/ci/gitlab-ci.yml) (`resource_group` lanes), and
  [Forgejo/Gitea Actions](examples/ci/forgejo-actions.yml), plus a `CI.md` write-up (the
  runner-is-the-backup-runner model, secrets→env, full/diff lanes, exit-code gating, and honest
  caveats on scratch/cron/observability). The doc-drift test now also keeps the recipes' commands in
  sync with the CLI and checks the example YAML parses.
- **Agent guide (`AGENTS.md`) + skill** (#59) — the no-MCP way to point any coding/ops agent at the
  `neo4j-backup` CLI: safety posture (read-only default, `--confirm` to mutate, `--dry-run` a
  destructive op first), the command surface, and worked operator prompts → exact commands. A thin
  Claude skill (`.claude/skills/neo4j-backup/`) and `llms.txt` reference it (no second copy of the
  contract). A doc-drift test asserts every command shown in the docs is a real CLI subcommand.
- **Agent-drivable CLI contract** (#60) — `neo4j_backup_core.cli_contract` (the JSON result
  envelope, `Exit` code classes, `validate_envelope`) with a reusable pytest conformance harness,
  plus the `CLI-CONTRACT.md` spec. This is the machine-readable, no-MCP interface the forthcoming
  `neo4j-backup` CLI (#58) and the optional MCP server build to; shipped before any CLI code so the
  subcommands are written against a fixed contract.

- **`neo4j-backup` CLI** (#58 P1) — a scheduler-agnostic command-line adapter over the core, for
  teams on CI/cron with no orchestrator: `backup` / `verify` / `aggregate` / `restore` / `prune` /
  `metadata export|restore` / `system-backup` / `targets`. Subprocess execution (neo4j-admin local,
  or execed on a runner via `RUNNER_EXEC_PREFIX`); honours every existing env/policy seam. Every
  subcommand meets the #60 contract — `--json` envelope, documented exit codes, and
  `--dry-run` + blast-radius + `--confirm` on the mutating commands. Installs with the base package
  (`neo4j-backup = neo4j_backup_cli.__main__:main`); validated end-to-end on the compose stack
  (`just cli-smoke`). The shared env→client builder now lives in `neo4j_backup_core.env` (Airflow's
  `config` re-exports it).

### Changed
- **README positioning + CLI reference** (#58 P4) — "Who this is for" now includes the CI/cron path;
  a "Front-ends — pick by what you already run" table frames Dagster/Airflow vs the CLI+CI option;
  a "Command-line interface" section documents install + every command; and the documentation map
  lists `CLI-CONTRACT.md`, `CI.md`, and `AGENTS.md`.
- **Shared op bodies factored into `neo4j_backup_core.ops`** (#58 P1) — the backup / aggregate /
  verify / prune / restore (alias-swap + by-name) / metadata / system-backup logic, plus the
  `BACKUP_UPLOAD=pipeline` routing, lived in near-duplicate form in both the Dagster and Airflow
  adapters. It now lives once in core, parameterized by a `run_admin` callable + client handles, so
  both adapters (and the forthcoming CLI) call one implementation. Behaviour unchanged.

## [0.2.0] — 2026-07-11

### Added
- **Multi-cloud object store** (#52) — Azure Blob (`CLOUD=azure`) and GCS (`CLOUD=gcp`) backends
  alongside S3, behind one `ObjectStore` protocol with an `object_store()` factory and shared
  cloud-agnostic composites (`_BaseObjectStore`); per-cloud primitives per backend. Each is
  validated against its local emulator — MinIO / **Azurite** / **fake-gcs-server** (compose
  profiles `azure` / `gcp`). Install the matching extra: `pip install 'neo4j-backup-dagster[azure]'`
  / `[gcp]`.
- **Cloud-agnostic restore validation** — `file://` seed (FileSeedProvider) restores from a
  local/mounted artifact, so the restore drive is validated for every cloud without Neo4j's
  per-cloud `gs://`/`azb://` fetch (`just file-restore-smoke`).

### Changed
- `ObjectStore.s3_uri` → `uri` (cloud-neutral: returns `s3://` / `azb://` / `gs://`).

## [0.1.0] — 2026-07-10

First tagged release. Validated end to end against a real Neo4j Enterprise + object-store stack
(shell, Dagster, Airflow, PITR) and the `RUNNER_MODE=k8s` path on k3d.

### Added
- **Core loop** — policy-driven `neo4j-admin` backup to an S3-compatible store, seed-from-URI
  restore with alias-swap cutover, non-destructive `verify`, retention `prune`, chain
  `aggregate`, and PITR (`seedRestoreUntil`), over a shared `neo4j_backup_core` engine with
  interchangeable **Dagster** and **Airflow** adapters.
- **Metadata & system** — agentless DBMS metadata export/replay (users/roles/privileges/aliases)
  and a binary `system`-database backup + offline restore runbook.
- **Seed topology** — per-group `TOPOLOGY n PRIMARIES m SECONDARIES` so restores keep their
  redundancy (#20).
- **Bolt resilience** — bounded transient retry classified by Neo4j status code, over one shared
  client path (`Neo4jClient.run_on`) (#19, #24, #25).
- **Pluggable seams** — secret provider (env / AWS Secrets Manager, #18), cutover strategy
  (alias-swap / external router, #17), object-store path layout (`PATH_LAYOUT`, #21), policy
  source (`s3://` with TTL cache + last-known-good, #43) and loader override (`POLICY_LOADER`
  for authenticated endpoints, #46).
- **Encryption on every write** — `S3_SSE`/`S3_WRITE_ARGS` on the pipeline's boto3 PUT/COPY, and
  `BACKUP_UPLOAD=pipeline` to route neo4j-admin's S3 writes (backup/system/aggregate/verify)
  through boto3 for buckets that require an SSE header (#39).
- **By-name mode** — per-group `restore_mode: by-name` to back up/restore databases by their own
  name (create-if-absent, or gated destructive replace); backup accepts an alias or a physical
  name (`resolve_physical`) (#48).
- **Seed Cypher version** — `SEED_CYPHER_VERSION` couples `existingData` (required in Cypher 5,
  deprecated in Cypher 25).
- **Runner** — subprocess (EC2/VM) and `RUNNER_MODE=k8s` execution; `RUNNER_NEO4J_ADMIN`.

### Fixed
- PITR bracketing now polls the server clock instead of fixed sleeps (#26).
- Dagster `prune` `list_text_keys` delegation; `RUNNER_NEO4J_ADMIN` wiring.
- Pinned `grpcio-health-checking<1.82` (protobuf gencode/runtime drift).

[Unreleased]: https://github.com/lex00/neo4j-backup/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/lex00/neo4j-backup/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/lex00/neo4j-backup/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lex00/neo4j-backup/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lex00/neo4j-backup/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lex00/neo4j-backup/releases/tag/v0.1.0
