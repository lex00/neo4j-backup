# Changelog

All notable changes to this project are documented here, in the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format. Versions follow
[Semantic Versioning](https://semver.org/): in 0.x, a minor bump for features or breaking changes
and a patch for fixes. See [RELEASING.md](RELEASING.md).

## [Unreleased]

### Site copy (#282)
- Every page rewritten for a new operator: short statements, positive framing, terms linked to the glossary.
- New local textlint rule `plain-positive`, built on `sentence-splitter`. It flags sentences that open with "What" or "That", and negations in prose.
- `just docs-lint` now covers every built page, CHANGELOG, RELEASING and `examples/ci` included.
- Home page headline: "Neo4j PITR for operations teams with many tenants". `just try` and `just try-mcp` print tool calls and results.

### Docs centered on the operator workflows (#276)
- Each workflow page has a tab per path: Agent (MCP), CLI, Dagster and Airflow (#277).
- A Workflows tab in the site nav. Dagster, Airflow and CI setup moved under Start (#278).
  The workflows overview has a table of the prompt, CLI command, Dagster job and Airflow DAG for each.
- MCP.md is now "Connect an agent", with an example run. AGENTS.md left the nav; it stays as the
  instruction file for agents that drive the CLI (#279).
- `just try-mcp`: the same restore request as `just try`, run through the MCP server. The home
  page shows the restore both ways, and the adoption path has a "Connect an agent" step (#280).

### MCP built around the operator workflows (#271)
- A prompt per workflow (#272): `onboard` / `check_coverage` / `restore_request` / `bulk_load` /
  `offboard` / `recover_cluster`. Each previews before it changes anything and waits for the
  operator's yes. In read-only scope it hands over the CLI commands instead.
- New tools for steps the CLI had and MCP lacked: `validate_policy`, `policy_show`,
  `preview_adopt`, `run_export`, `run_metadata_export`, and `backup_status(group)` (#273).
- `coverage_report` (members by state, each stale one with a reason and the fix) and
  `restore_plan` (restore points, preview and a message for the customer) (#274).
- MCP.md is organized by workflow.

### Docs site (#260)
- `just try`: the local loop in one command, as a restore request with a summary per step (#269).
  The home page leads with that restore, then a five-step adoption path (#268).
- A site-only landing page with three entry points, and a new look with light and dark modes.
  Top tabs: Start / Run it / Reference / Project (#261, #262).
- The operator guide is split into six workflow pages under `workflows/` (#263). The policy field
  reference moved to POLICY-FIELDS.md (#264).
- Every page cut to about half: the site went from 31.9k to 15.8k words (#265). DESIGN.md is
  now a short decision record.
- `just docs-lint`: textlint with sentences of at most 120 characters and 3 commas, run in CI
  (#266).

## [0.7.0] — 2026-11-16

Fixes from the novice adoption audits (#244), then from the v0.6.0 operator docs audit (#216).

### Adoption (#244)

**Upgrading.**
- A tier can set `rpo_minutes`, the default for its groups (#253). The demo and example
  policies set it per tier (gold 90, silver 420, bronze 1500). Before, every group defaulted to
  60, so slower tiers showed as stale between backups.
- The `default` metadata, `system` backup and DBMS sweep jobs exist only when a group runs on the
  `default` cluster (#251). Before, they pointed at `neo4j://localhost:7687`.
- The CLI's `backup`, `verify`, `aggregate` and `load` take the group lock, like the orchestrators
  (#252). A held lock fails with error kind `locked` after `NEO4J_BACKUP_LOCK_WAIT`.
- Airflow: `neo4j_verify_<tier>` and `neo4j_aggregate_<tier>` run in the cluster's full pool
  (#250). A tier spanning named clusters gets one task per cluster (`verify_<cluster>`).

**Added.**
- Failure alerts (#246): `NEO4J_BACKUP_ALERT_WEBHOOK` gets a JSON post for every failed run.
  Dagster's `alert_on_failure` sensor and every Airflow DAG's failure callback send it.
- Restore previews in the orchestrators (#249): `dry_run` on Airflow's `neo4j_restore` and
  Dagster's `restore_group`. The Airflow backup DAGs take `group_id` to back up one group.
- Deploy kits: the Dagster image includes `dagster-postgres` and takes `DAGSTER_VERSION`.
  `orchestrator/deploy/helm-values.yaml` is for the Dagster chart (#247). `airflow/deploy/` has
  a worker Dockerfile, RBAC and a pod template (#248).
- Dagster reads `RUNNER_EXEC_PREFIX`; `just dagster-dev` runs the UI on the local stack (#254).
- `policies/example.yaml`, a production starting point; GLOSSARY.md.

**Docs.** Prepare your Neo4j servers (#245). Choosing a runner mode, and permissions per
component (#255). Alerts, a test group and the CLI on Kubernetes. Choosing tiers, retention and
rpo. RECOVERY's users-and-roles section rewritten (#257).

### Operator docs audit (#216)

**Upgrading.**
- `aggregate_cron` is opt-in (#219): a tier aggregates only if it sets one. Before, every
  backed-up tier aggregated weekly, which removed point-in-time restore inside the week. Set
  `aggregate_cron` on a tier to keep the old schedule.
- Policy load refuses unknown keys (#222). Run `neo4j-backup policy validate` on your policy
  before upgrading. Before, a misspelled key fell back silently to its default.
- `BACKUP_UPLOAD=pipeline` with `RUNNER_MODE=k8s` is refused at startup (#218). It wrote zero
  backups to the bucket.
- With `RUNNER_MODE=k8s`, pod credentials come from workload identity or from Kubernetes
  Secrets. Map those in `RUNNER_SECRET_ENV` (#229).
- Dagster: the sample `dagster.yaml` drops `max_concurrent_runs` and the pool `default_limit`.
  Set the `neo4j` pool with `dagster instance concurrency set neo4j <n>` (#224). Restores and
  moves drop the `backup_kind: full` tag; only their baseline step waits for the pool. Minimum
  Dagster is 1.10.
- Airflow: `neo4j_restore` and `neo4j_rollback` require an explicit `group_id`.
- The demo policy's rebuild-only tier is `rebuild_nightly` (was `nightly`).

### Fixed
- Dagster's backup asset and restore job honour the fence left by `restore --to-cluster`
  (#217).
- A cutover that fails part-way leaves a record, so `rollback` goes back one generation (#220).
  Each alias move is recorded as it happens, and the error (`cutover_partial`) lists what moved.
  Same for `adopt`.
- A seed slower than 300 s is swapped in only once it is online (#221). The seed waits up to
  the group's `rto_minutes` (else `NEO4J_SEED_WAIT_SECONDS`) and checks every server's result.
  Every new database must be online before any alias moves. Dagster and Airflow restores take
  `check` and `keep_failed`.
- A "latest" group restore goes to the newest instant every member covers (#225). Before, it
  went to each member's own newest backup, possibly hours apart.
- After a store-id change (an incremental import), the next scheduled tick takes a full (#226).
  Before, scheduled diffs skipped until the next full schedule. Skips show as a warning (Dagster
  asset check `backup_taken`) or a skipped task (Airflow).
- `adopt --migrate` makes the original read-only before its backup, so the backup holds every
  write (#227).
- Dagster: aggregate and verify runs carry `neo4j_group` and the full lane's tag (#223). Every
  member op takes the per-group lock Airflow uses, so manual and backfill runs wait for a
  group's other work. Airflow's verify and aggregate DAGs take the lock too.
- Dagster: with `schedules: running` the `reconcile_registry` sensor starts too. A tick with
  zero partitions says so (#228).
- One Bolt driver per client instead of one per statement (#230).
- Scheduled cleanup keeps a soak period: tier `cleanup_keep` (1) and `cleanup_older_than_days`
  (7) (#231).
- Airflow rebuilds of a named cluster's groups run in its pool (#232).
- A backup that writes zero artifacts fails; before, it reported success (#218).
- A seed now waits until every server hosting the new database reports it online. On a
  three-server 2026.05 cluster, `CREATE DATABASE … WAIT` under Cypher 25 returned
  before the secondaries were up. It returns zero rows, so the swap could come early.
- `verify` copies only the newest database's backups when a folder holds backups of two
  same-name databases. This follows a by-name restore with `--replace`, or a same-name adopt. neo4j-admin refused to aggregate them together. Found on the stack by
  `adopt --migrate`, which stopped safely before its drop.
- `just down` also removes the optional profiles' containers: Azurite, the second cluster and the
  three-server cluster. They kept pointing at removed networks.

### Added
- A three-server cluster for the local stack (`docker/compose.cluster.yaml`,
  `just cluster-smoke`). Smokes for the guards (`just guards-smoke`) and Kubernetes Secrets
  (`just k3d-secret-smoke`) (#234).
- Freshness alerting on `dbms.status_cron` (#233): Dagster `status_job` and `status_check`
  schedule; Airflow `neo4j_status` DAG.
- k8s runner settings: `RUNNER_NAMESPACE` (Dagster too), `RUNNER_CPU_REQUEST`,
  `RUNNER_MEMORY_REQUEST`, `RUNNER_TOLERATIONS`, `RUNNER_SECRET_ENV` (#229).
- Docs: ENVIRONMENT.md with every variable, and a Dagster deployment page (#236). It has an
  image recipe (`orchestrator/deploy/Dockerfile`), Helm/Dagster+ examples and a worked
  concurrency example. An Airflow production section (#235) and the README's
  validation matrix (#234). Corrections (#237, #238, #239) in POLICY.md and OPERATOR.md, plus
  RECOVERY.md and the reference pages. DESIGN.md marked as design notes and history (#240).
  Product description and issue numbers cut from the reference pages (#241).

## [0.6.0] — 2026-11-16

**Upgrading (Airflow).** Policy-generated DAGs replace the fixed ones. `neo4j_verify`,
`neo4j_aggregate` and `neo4j_prune` gain a `_<tier>` suffix. `neo4j_metadata_backup` and
`neo4j_system_backup` become `neo4j_dbms_metadata_default` and `neo4j_dbms_system_default`. Run
history stays behind; unpause the new DAGs.

### Added
- Readable slugs, opt-in (#91): `naming.slug_style: readable` lowercases an alias before
  hashing (`Orders` -> `orders`). The default stays `legacy`. Members that would share a
  slug fail policy load. Migration: pin existing members with `overrides.prefix` first.
- Per-member `prefix` and `physical_base` (#90): `overrides.<alias>.prefix` pins a member's
  storage folder. Renaming an alias keeps its history. `physical_base` sets the base of its
  database names. Policy load refuses duplicate, nested and reserved prefixes.
- Policy `naming:` block (#88): `physical_template` (`{alias}`, `{slug}`, `{group}`, `{ts}`) and
  `ts_format`, at the top level, in a profile or per group. Defaults reproduce today's names,
  and policy load checks every member's name. Used by restore, load, adopt and cleanup, restore
  into another cluster and the Airflow restore DAG.
- Offboarding with retired groups (#124): `state: retired` with `retired_on` and
  `retire_after_days` stops a group's schedules. Its artifacts stay restorable until the period
  ends, then prune deletes them. `status` lists retired groups with `deletes_on`. Store folders
  owned by zero groups show under `unmanaged` and stay forever. New store method
  `list_prefixes`.
- Freshness against `rpo_minutes` (#123): CLI `status [group|--tenant t] [--stale]` and MCP
  `backup_status`. Each target gets its newest restorable point (backup or load) and a state:
  `ok`, `stale` or `by_design` (#112). `--stale` exits 1 when any target is stale. Policy load
  warns when a tier's cadence is longer than a group's `rpo_minutes`.
- `adopt <group>` (#92; MCP `run_adopt`) puts existing databases, named in
  `overrides.<alias>.adopt_from`, behind the group's aliases. Each member is a create, skip or
  conflict, and any conflict stops it. `--migrate --replace` handles a database with the
  alias's own name. It backs up, verifies, seeds a new physical, checks, copies privileges, drops
  the original and creates the alias. Adopted targets are recorded for rollback
  and cleanup.
- Restore into another cluster (#115): `restore <group> --to-cluster <c>`, or
  `restore --from-cluster <lost> --to-cluster <c>` for every group of a cluster. Also MCP
  `run_restore_to_cluster`, Dagster `move_job` and Airflow `neo4j_restore_to_cluster`. It replays
  the source's users and roles, seeds on the target and rewrites the members' privileges. It
  creates or swaps the aliases and takes the baselines. It fences the groups on the source
  until the policy names the target. A live source blocks it; `--fence-source` overrides.
- `cleanup <group>` (#93; MCP `run_cleanup`, tier `cleanup_cron` in Dagster and Airflow). It
  drops superseded databases the tool created that zero aliases target. It keeps the current
  target and `--keep` before it, and revokes their copied privileges first. Customer databases
  always stay.
- Naming hook (#89): `NAMING=module.Class` (`slug`, `physical(alias, ts, current)`, `owns`).
  It replaces the default scheme everywhere: layout, restore and load. Its output is validated,
  and a physical name already in use is refused (blue/green safe).
- `rollback <group>` (#122; MCP `run_rollback`, Dagster `rollback_job`, Airflow
  `neo4j_rollback`) points each alias back at its database before the most recent swap. It
  reads `_state/<group>.json`, which every restore, load and rebuild now writes. It refuses if
  the previous database is gone, copies privileges granted since the swap and skips the
  baseline.
- Restore some members of a group (#121): `restore --member <alias>`, repeatable. MCP, the
  Dagster config and the Airflow params take `members`. The dry-run warns that they lose time
  alignment with the rest. Privileges, baseline and chain choice apply per member.
- `restore-points <group>` (#113; MCP `restore_points`) lists per member the current database
  and load artifacts. It gives each chain's point-in-time window (older physicals included)
  and the gaps, each with its restore command. A gap is a diff missing in a
  window, or zero backups since the last swap. Read-only; neo4j-admin stays idle. Takes
  `--tenant`.
- Notice before a load or rebuild (#126): `load.notice: {hook, before, required}` calls the hook,
  then waits `before`. The hook is an HTTP POST (JSON) or a command with `NOTICE_*` env vars,
  like `CUTOVER_HOOK`. It gets the group, tenant, members, swap time and what will be lost. A
  failed hook is logged and the load goes ahead; `required: true` stops it.
- Custom build steps (#109): `build.kind: command` runs a customer command in the runner. It
  shares one step with the import and dump, under `timeout`. The k8s runner modes can run an `image` as
  its own pod instead. Both get `SOURCE_URI`, `WORK_DIR`, `OUTPUT_URI` and `env` (values
  `secret:<ref>` resolved). `output: import` hands CSV/Parquet to the built-in import;
  `output: dump` a dump. `run_admin` takes an `image` in every adapter.
- Scheduled rebuilds and orchestrated loads (#107, #108): Dagster `load_job` and
  `<tier>_rebuild` schedules (from `rebuild_cron`, only `load.mode: rebuild` groups); Airflow
  `neo4j_load` and `neo4j_rebuild_<tier>`. Runs carry a `neo4j_group` tag, and the sample
  `dagster.yaml` limits it to 1 per group. Airflow backups and loads take a per-group lock
  object, created with the store's conditional write. The first load of a group creates its
  alias. `just load-smoke` covers CSV and Parquet, read-only and read-write. The `dev` extra
  adds `pyarrow` for it.
- `load <group>` (#107; core, CLI and MCP) builds each member in the runner from `load.build`
  (`kind: import`). Relative input paths resolve against `load.source`, and neo4j-admin reads
  them straight from the store. Each member is dumped and seeded into a new database, read-only
  for `writes: read-only`. It runs the checks (with the import's
  counts), copies privileges, swaps every alias together and takes the baseline. A
  failure before the swap drops the new databases and names the step. `--reapply-access`
  applies `writes` and skips the load.
- `writes` and `protect_writes` enforced (#112). A `writes: read-only` group's new databases are
  set read-only before the swap, restores included. Backups follow the live access mode and
  `protect_writes`. They skip a read-only database and a group whose writes are disposable,
  with `by_design` in the result (CLI, MCP, Dagster metadata, Airflow). A database that is still
  read-write keeps its backups until a load makes it read-only.
- Checks before a swap (#111): `load.checks` in the policy. Each is Cypher returning one
  number, with `== n`, `>= n`, `<= n` or `between a b`. Default checks cover online state,
  non-zero counts and counts equal to the import's. `restore --check` runs them before the
  swap. A failure drops the new databases (`--keep-failed` keeps them) and moves zero aliases.
  Loads (#107) run them every time.
- `just dump-smoke` (#161) runs end to end on the local stack. It imports, dumps and seeds,
  takes a baseline and a diff, then does a point-in-time restore and an export.
- `export <group> [--until T]` (#160, part of #98) writes a portable `.dump` of each member to
  its `_seed/` folder. The dump is at a point in time (or the latest backup). The
  runner restores the covering chain into a throwaway local store, dumps it and removes it in
  one step. The cluster stays untouched. Guarded (`--dry-run` shows the chain per member), with
  `--ticket` / `--reason`.
- Prune keeps `keep_loads` load artifacts per member (#157, part of #98). The newest
  `load.keep_loads` dumps in each member's `_seed/` folder stay; older ones go, whatever their
  age. The preview lists them under `loads` (CLI, MCP).
- Restore from load artifacts (#144, #159, part of #98): `restore --from-load` uses every
  member's newest dump. `--from-dump MEMBER=KEY_OR_URI` takes a key in the member's `_seed/`, or
  an external dump URI the servers can read. Also in MCP, the Dagster restore config and the
  Airflow restore params. `--until` picks backups only and is refused with a dump. A
  member with zero backups seeds from its newest load. Privileges and the baseline apply as for
  any restore.
- `import --to-dump <group>/<member>` (#158, part of #98): import on the loader, then dump the
  offline store. The server stays stopped. The dump goes in the member's `_seed/<ts>/` folder;
  `BACKUP_UPLOAD=pipeline` uploads it through the tool. IMPORT.md makes it the default route.
- Load artifacts (#156, part of #98). A `.dump` (or a `.backup` kept as a load) lives in
  `<group>/<slug>/_seed/`. `PathLayout.load_prefix` names it and `list_load_artifacts` lists it
  on every backend. `list_artifacts` skips `_seed/`, so chain heads are always backups. MCP
  `show_chain` returns `loads`; `backup_status` shows the latest.
- DBMS jobs and concurrency per cluster (#106). The metadata export, `system` backup, metadata
  restore and DBMS sweep run per cluster (CLI `--cluster`). Dagster has `<cluster>_dbms_*`
  schedules; Airflow has `neo4j_dbms_*_<cluster>` DAGs. The default cluster keeps `_dbms/`; a
  named cluster writes under `_dbms/<cluster>/`. Metadata listings now use the exact prefix
  `_dbms/metadata-`. Dagster runs carry a `neo4j_cluster` tag, limited per cluster in the sample
  `dagster.yaml`. Airflow backup DAGs use per-cluster pools `neo4j_<cluster>_<lane>` (default
  cluster unchanged). Cluster names `system` and `metadata…` are reserved.
- Several clusters in one policy (#106, first part). `clusters:` names each Neo4j DBMS:
  `bolt_uri`, `user`, `password_ref`, `backup_source` (one or a list), `storage`, `topology`,
  `cypher_version`, `cutover`, `backup_upload`. A group sets `cluster:` (else `default`, from
  the environment as before). Every op resolves the Neo4j client, runner and store per group.
  `targets` and the MCP status show each group's cluster. New validation: group ids must be
  unique (`Policy.group` used to return the first match). An alias or database belongs to one
  group per cluster.
- Per-store settings for neo4j-admin, secret references and `policy check` (#105). A named S3
  store's `credentials_ref` (a key pair) and `region` / `runner_endpoint` reach both the tool
  and neo4j-admin. `run_admin` takes an `env`. With a `docker … exec` prefix the names pass as
  bare `-e NAME`, so values stay out of argv. One secret-reference syntax: `<provider>:<ref>`.
  `neo4j-backup policy check` probes every store and names those the servers must read.
- Named stores (#105, first part). A `storage:` map names object stores (`url`, `region`,
  `endpoint`, `sse`, `sse_kms_key_id`, `write_args`, `credentials_ref`). A group picks one with
  `storage:`, else `default`. `default` is the environment's store, or the policy's own entry
  if it defines one. Every op (backup, verify, aggregate, prune, restore, MCP status)
  resolves the store from the group. Unknown store names fail policy load. `s3_prefix` is optional
  and deprecated (a warning; its value is ignored).
- Every schedule comes from the policy (#104). Tiers gain `verify_cron`, `aggregate_cron`,
  `prune_cron` and `cleanup_cron` (for #93). The first three default to the old fixed crons on
  backed-up tiers. A top-level `dbms:` block sets `metadata_cron`, `system_backup_cron`,
  `keep_metadata` and `keep_system`. `schedules: running` starts the generated Dagster
  schedules. Dagster gains `<tier>_verify`, `<tier>_aggregate`, `<tier>_prune`,
  `default_dbms_metadata` and `default_dbms_system`. Airflow's DAGs are generated the same way.
  Prune runs per tier over that tier's groups. The DBMS sweep runs after each metadata export
  and system backup. CI.md maps each cron to its CLI command.
- `tenant` label (#102). Groups (and profiles) take `tenant:`. `owner` is still read as the
  tenant, with a deprecation warning. `backup`, `verify`, `aggregate` and `targets` take
  `--tenant <t>` in place of a group. A group and `--tenant` together, or an unknown name, is
  exit 2. `targets` and the MCP status show each group's tenant. MCP `list_targets`,
  `backup_status`, `run_backup`, `run_verify` and `run_aggregate` take `tenant`. It selects
  groups; access control is separate, and `restore` stays per group.
- Profiles (#101). `profiles:` names a customer type's settings once. A group sets `profile:`
  and overrides what differs (`load:` merges per key). Applied before validation, in one place,
  so every front-end sees the resolved groups. `neo4j-backup policy show <group>` prints a
  group's effective settings and the source of each. `neo4j-backup policy validate <file>`
  checks the file alone and exits 2 if invalid. The demo policy ships `app-live`,
  `data-interval`, `data-scratch`, `data-then-live` and a `rebuild_cron`-only `nightly` tier.
- `load:` block in the policy (#103). Keys: `mode` (live / once / rebuild), `writes`,
  `protect_writes`, `keep_loads`, `baseline`, `source` and `build`. Validation covers the
  combinations, and errors name the group. Groups that omit it behave as before. Tiers gain
  `rebuild_cron`, and `full_cron` and `diff_cron` become optional. A tier for load-only groups
  can have `rebuild_cron` alone; a missing cron skips its backup schedule. Schema only: loads
  and rebuilds come with #107 and #108.
- `--ticket` and `--reason` (#119) on every mutating CLI command (`restore`, `aggregate`,
  `prune`, `metadata restore`), the MCP write tools and the Airflow restore DAG (params). The
  Dagster restore job takes them as config and run tags `neo4j_backup/ticket` /
  `neo4j_backup/reason`. Logged and returned with the result.
  `NEO4J_BACKUP_REQUIRE_TICKET=true` requires a ticket for an applied change
  (`ticket_required`, exit 3).

## [0.5.0] — 2026-11-15

Phase 1 of #100 (correctness). Restores keep customer privileges and take a baseline full.
Every backup carries its own metadata, and prune and `--until` read real chains. The diff lane
survives a seed, and the Dagster tier schedules work with more than one member.

**Upgrading.** Restore now takes a full backup of each restored database by default
(`--no-baseline` to skip). The Dagster `restore_group` job carries the `backup_kind: full` tag.
It needs the `runner` and `pipes_subprocess_client` resources, as does the `prune` asset. A
policy that fails to load now fails the Dagster code location and the Airflow DAG file. `keep_system` / `keep_metadata` must be 1 or more. Backups
from earlier releases lack a metadata sidecar; the next backup or prune of each database fills
it in.

### Added
- Baseline full backup after every restore (#97), on by default. The restore's last step takes
  a FULL of each database it created. The next diff then has a base, and point-in-time coverage
  starts straight away. CLI `--no-baseline` / `baseline: false` (Dagster config, Airflow param,
  MCP) skips it. Dagster runs it as `baseline_op` after the cutover and tags the `restore_group`
  job `backup_kind: full`. Airflow runs a mapped `baseline` task in the `neo4j_full` pool. A
  failed baseline leaves the restore applied and reports `baseline_failed`.
- Artifact metadata (#118). After every backup and aggregate the tool runs
  `neo4j-admin backup inspect` on the physical's folder. A `<artifact>.meta.json` sidecar beside
  each artifact records full or diff and the transaction range, the backup time and
  store id hash, and the database and neo4j-admin version. Aggregate rewrites them, and prune
  deletes them with their artifacts. Older artifacts get one on the next backup.
  `ops.artifact_chains` reads them into chains with time windows; the MCP `show_chain` tool
  returns them. A failed inspect is logged and the backup still succeeds.
- `run_admin` returns neo4j-admin's output in every adapter (#118). This covers the CLI and MCP
  subprocess, Dagster Pipes (subprocess, `PipesK8sClient`) and Airflow (subprocess, KPO).
- Policy load warns when a tier's full backups are further apart than a group's
  `retention_days` (#73). `neo4j-backup targets` returns the warnings. Crons in other formats
  (`@weekly`, day names) are accepted and left unchecked.
- Chains are split by store (`databaseId`) as well as folder (#120). A by-name restore with
  `--replace` recreates the database under the same name. The next AUTO backup writes a new full
  into the same folder, and the two stores' backups now read as separate chains.

### Documentation
- Docs reorganised by reader (#116). Getting started has the local loop with real output, then a
  production checklist. The Operator guide has onboarding, coverage checks and the restore
  runbook, with a tested workaround for #117. It also covers bulk loads, offboarding and a lost
  cluster, plus settings and commands. The Customer guide covers the protection backups give,
  how to ask for a restore and the steps during one. README trimmed to its audience and a map;
  the docs site nav follows the readers.
- IMPORT.md corrected (#96). Parquet imports directly and dumps can be seeded. A seeded database
  needs its own full backup. RECOVERY.md warns about #117. DESIGN.md §9 drops the claim that
  several clusters work unchanged.

### Fixed
- A DIFF of a store whose id changed (after `import incremental`, #99) is skipped with
  `needs_full` instead of failing. IMPORT.md records the experiment on 2026.05 and the operator
  procedure. Dagster's subprocess runs keep neo4j-admin's output on a failure.
- An `image` build's output (k8s runner) goes to the run's log, as a `command` build's did
  (#109; found on k3d).
- Azure downloads read 4 MiB at a time, one request at a time (#79). With the SDK defaults a
  64 MiB blob held ~288 MiB; the peak now stays ~40 MiB at any size (measured on Azurite).
- Mixed-case aliases work (#206). Neo4j stores alias names lowercase, so `alias_target` and
  `database_exists` now compare case-insensitively. Before, a group with an alias like `Orders`
  failed to back up, and a restore left the alias on the old database.
- The `mcp` extra pins `mcp<2` (#203); mcp 2.x renamed `FastMCP` and the server failed to start.
  CI now installs the extra, so the MCP server tests run there.
- `kind: command` builds: relative `import_args` files reach neo4j-admin as absolute paths in
  the work dir. neo4j-admin needs that (#109; found by the stack smoke).
- Docs (#116): README names its audience, a platform team serving other teams from one policy.
  POLICY.md splits settings between the policy and the environment, and points at the right
  schema file. RECOVERY.md has the customer-request runbook in short.
- GCS applies `write_args` to every object the tool writes (#81). Accepted keys:
  `kms_key_name` / `content_type` / `metadata` / `cache_control` / `storage_class`. It refuses
  other keys instead of dropping them.
- Azure: a server-side copy waits until Azure reports it finished (#80). Verify thus aggregates
  only a complete scratch copy. `failed`, `aborted` or `AZURE_COPY_TIMEOUT` (default 3600 s)
  raise. Copies keep the store's `encryption_scope` (and tier, metadata, tags).
- Azure: downloads stream to disk instead of reading the whole blob into memory (#79). A
  multi-GB backup in `BACKUP_UPLOAD=pipeline` mode fits in the worker's memory.
- `just backup` writes under the physical prefix like the CLI and orchestrators (#131).
  `just restore` seeds from the full key of the newest backup, so it works after a CLI or
  orchestrator backup.
- The CLI keeps its contract on any error (#140): one JSON envelope, exit 1, and error kind
  `neo4j_error` for driver errors or `internal_error`. The traceback appears only with
  `--debug` or `NEO4J_BACKUP_DEBUG=1`. A group restore that fails while seeding drops and lists the members already
  seeded (`seed_failed`, `result.dropped`). Zero aliases pointed at them.
- `NAMING_TS_FMT` is honoured by `naming.py` as by `naming.sh` (#86). Both sides refuse a format
  containing `-` (the join character).
- Database-name validation follows the Neo4j docs (#85): `[a-z0-9.-]` and 3-63 characters. A
  name starts and ends with a letter or digit, and the `_` and `system` prefixes are refused.
  by-name groups lowercase their database names, as Neo4j does, instead of rejecting uppercase.
- Long aliases: `physical()` in `naming.py` and `naming.sh` keeps the 8-character alias hash and
  cuts the readable part (#87). `naming.owns(alias, physical)`
  recognises the tool's own physicals (for cleanup, #93).
- `import --to-dump` runs the import, the dump and the removal of the local store as one runner
  step (#167). As two steps it broke when each command is its own pod (k8s modes). The leftover
  store also made a second load under the same name fail.
- `--until` picks the chain that covers the time (#114). Restore used to seed from the member's
  newest backup. A time before an earlier restore (in the previous database's chain) failed.
  Now it picks, per member, the chain covering the time across every physical and store (using
  the sidecars). A member with zero changes after the time is seeded whole.
  Neo4j refuses `seedRestoreUntil` on a full or a transaction-free diff. Every member's choice
  is made before any change, so an uncovered time fails cleanly,
  listing the nearest windows. The dry-run shows the chain per member. Also `preview_restore(until=…)` in MCP.
- `keep_system` and `keep_metadata` mean the same thing (#78): the newest N are kept, and N is
  at least 1. Before, 0 kept every system full and deleted every metadata export. 0 is now
  refused.
- Point-in-time values are validated (#77). `--until` (CLI), `until` (MCP) and `restore_until`
  (Dagster config, Airflow param) are parsed as ISO-8601 at the boundary. A value missing an
  offset is UTC. The value is normalised (`2026-07-11T14:32:00+00:00`) and parsed again before
  the seed statement. Raw input stays out of Cypher. A bad value is a
  usage error (CLI exit 2, MCP `usage` error) before any Neo4j call.
- Names are quoted safely in every DDL statement (#77). Database and alias names used to go
  into backticks unescaped, and aliases may contain a backtick. One helper (`cypher.quote`) now
  doubles embedded backticks. It covers CREATE/DROP/STOP DATABASE, CREATE/ALTER ALIAS, the
  privilege copy and the metadata export. The seed URI must be an s3/gs/azb/file URI inside the backup store, free of quotes and
  backslashes.
- S3 deletes in batches of 1000 (#76). `delete_keys` used to send every key in one
  `DeleteObjects` call, which S3 rejects above 1000 keys (a large first prune). It also counted
  per-key errors as deleted. It now batches, tries every batch, returns the number actually
  deleted and raises `DeleteError` listing failed keys.
- A policy that fails to load is an error in the orchestrators (#75). Before, Dagster's
  schedules and Airflow's backup DAGs vanished silently when the policy failed to parse or
  validate. Same for a failed `s3://` source at a cold start. Now the Dagster
  code location fails to load and the Airflow DAG file shows an import error, with the policy
  error. Fetch errors are retried with backoff (1, 2, 4 seconds) before failing. With
  `NEO4J_BACKUP_POLICY` unset and the default file absent, the orchestrators build zero jobs
  and say so in the log.
- Diff lane after a seed (#74). A differential needs a full of the same database. Every diff
  used to fail until the next full after a seed. (alias-swap restore, bulk load, adopt or by-name
  `--replace`). Now a member whose current database lacks a full since its
  creation goes to the full lane. The Dagster diff schedule requests a FULL tagged
  `backup_kind: full`; the Airflow diff DAG runs it in the `neo4j_full` pool.
  `backup --kind DIFF` skips it cleanly and lists it under `needs_full`; the CI examples then
  run `--kind FULL`. Every backup result reports `taken`, the type neo4j-admin actually wrote.
- Dagster tier schedules work on every tick when a tier has more than one member. Dagster
  rejects several run requests that lack a `run_key`, so each request now has one.
- Prune keeps whole chains (#73). It used to keep only the newest backup per member. With weekly
  fulls and `retention_days: 7` (shipped `silver` and `bronze`) it deleted the full the diffs
  needed. The newest backup became unrestorable. Prune now
  reads the chains from the sidecars (#118). Per member it keeps the newest chain, each chain
  with a backup in the window and the chain at the window's start. Older chains go whole, sidecars with them. Backups that lack a sidecar are inspected
  first in the CLI, Dagster, Airflow and MCP `run_prune`, or kept and counted. The preview reports
  `kept_for_chains` and `unknown_kept`.
- `smoke_k8s.py` and `smoke_phase6.py` use the path layout now. They called a removed helper
  (`_physical_prefix`) and failed at start.
- Restores keep customer privileges (#117). Neo4j stores a privilege against the physical
  database even when it was granted through an alias. An alias-swap restore therefore locked
  out every customer user that lacked DBMS-wide grants. The restore now re-grants every
  privilege on each old physical onto its new one before any alias moves. A failed grant stops
  the restore with every alias where it was. by-name restores with `--replace` read the
  privileges before the DROP (which removes them) and re-grant after CREATE. The
  dry-run plan lists the privileges per member, and the result shows the grants. `just restore`
  does the same copy.
- Local stack (#127): MinIO's images stopped being available, so `just fresh` failed at its
  first step. The stack now uses the floci S3/KMS emulator (`floci/floci:1.5.34`) and the AWS
  CLI (`amazon/aws-cli`) for setup/listing. Host port 9000 still serves S3. **Recreate your
  `.env`** from `.env.example`: `AWS_ENDPOINT_URL_S3` is now `http://s3:4566` and
  `MINIO_KMS_SECRET_KEY` is gone. floci records the bucket's default SSE-KMS and leaves it
  unapplied; see STACK.md.

## [0.4.0] — 2026-07-11

Bulk import off-cluster, plus a docs-coherency pass across the front-end story.

### Added
- Bulk import (#16): `neo4j-backup import <database> -- <neo4j-admin import args>`. It builds an
  offline store from raw CSV/Parquet on ephemeral hardware. It passes through to
  `neo4j-admin database import full`, database first, so multi-value `--nodes` leaves the
  database argument alone. It starts the off-cluster tail: import into a fresh loader's
  default database and adopt it on first start. Then back up online to a native `.backup` and
  seed the real cluster with existing commands. `IMPORT.md` documents the
  pattern. Its key finding: importing into an already-registered database quarantines the
  store (validated on Neo4j 2026.05), so use a fresh instance's default db. `just import-smoke`
  validates the tail end to end with a throwaway loader container.

### Changed
- Docs coherency: `DESIGN.md` (§6.10), `RECOVERY.md` and `STACK.md` now cover the CLI and MCP
  front-ends too. The mkdocs site stages the CI example recipes and `CHANGELOG`/`RELEASING`, so
  their links resolve on the published site.

## [0.3.0] — 2026-07-11

Scheduler-agnostic reach: a `neo4j-backup` CLI, CI recipes, an agent guide and an optional
operator MCP server. All are thin layers over one shared engine (epic #58).

### Added
- Operator MCP server (#58 P5): `neo4j-backup-mcp` exposes the operations as
  [MCP](https://modelcontextprotocol.io) tools, so an operator drives DR/status through an
  agent. Schedulers own the cadence; MCP owns the exceptions. Read-only by default:
  `list_targets`, `latest_artifact`, `show_chain`, `backup_status`, `preview_restore`,
  `preview_prune`. `NEO4J_BACKUP_MCP_MODE=read-write` adds the mutating tools
  (`run_backup`/`run_verify`/`run_aggregate`/`run_restore`/`run_prune`). Each needs `confirm=true` and previews via `dry_run`. A destructive `run_restore(replace=true)` runs
  verify-before-drop. stdio transport, every call audit-logged. The tool logic
  (`neo4j_backup_mcp.tools`) is dependency-free and tested. FastMCP wiring is in the optional
  `[mcp]` extra, with a `neo4j-backup-mcp` entry point. Validated live
  (`just mcp-smoke`).
- CI recipes (#58 P3): copy-and-adapt templates that schedule the `neo4j-backup` CLI from CI.
  [GitHub Actions](examples/ci/github-actions.yml), [GitLab CI](examples/ci/gitlab-ci.yml)
  (`resource_group` lanes) and [Forgejo/Gitea Actions](examples/ci/forgejo-actions.yml). A
  `CI.md` write-up covers the runner-is-the-backup-runner model, secrets to env, full/diff lanes
  and exit-code gating. It has honest caveats on scratch, cron and observability. The
  doc-drift test now keeps the recipes' commands in sync with the CLI and parses the YAML.
- Agent guide (`AGENTS.md`) and skill (#59): the MCP-free way to point any coding or ops agent
  at the `neo4j-backup` CLI. It covers the command surface and the safety posture (read-only
  default, `--confirm` to mutate, `--dry-run` first). Worked operator prompts
  map to exact commands. A thin Claude skill (`.claude/skills/neo4j-backup/`) and `llms.txt`
  reference it, so the contract has one copy. A doc-drift test asserts every command shown in
  the docs is a real CLI subcommand.
- Agent-drivable CLI contract (#60): `neo4j_backup_core.cli_contract` and the `CLI-CONTRACT.md`
  spec. Contents: the JSON result envelope, `Exit` code classes, `validate_envelope` and a
  reusable pytest conformance harness. It is the machine-readable interface for MCP-free agents. The forthcoming `neo4j-backup` CLI (#58) and the optional MCP server build to it.
  It shipped before any CLI code, so the subcommands are written against a fixed contract.

- `neo4j-backup` CLI (#58 P1): a scheduler-agnostic command-line adapter over the core. It
  serves teams on CI/cron instead of an orchestrator. Commands: `backup` / `verify` /
  `aggregate` / `restore` / `prune` / `metadata export|restore` / `system-backup` / `targets`.
  Subprocess execution: neo4j-admin runs locally, or execed on a runner via
  `RUNNER_EXEC_PREFIX`. It honours every existing env/policy seam. Every subcommand meets the
  #60 contract: `--json` envelope, documented exit codes, and `--dry-run` + blast-radius +
  `--confirm` on the mutating commands. Installs with the base package
  (`neo4j-backup = neo4j_backup_cli.__main__:main`). Validated end-to-end on the compose stack
  (`just cli-smoke`). The shared env-to-client builder now lives in `neo4j_backup_core.env`,
  and Airflow's `config` re-exports it.

### Changed
- README positioning and CLI reference (#58 P4). "Who this is for" now includes the CI/cron
  path. A front-ends table sets Dagster/Airflow beside the CLI+CI option,
  by what you already run. A "Command-line interface" section documents install and every command. The
  documentation map lists `CLI-CONTRACT.md`, `CI.md` and `AGENTS.md`.
- Shared op bodies factored into `neo4j_backup_core.ops` (#58 P1). The Dagster and Airflow
  adapters had near-duplicate op logic and `BACKUP_UPLOAD=pipeline` routing. The ops: backup / aggregate /
  verify / prune / restore (alias-swap and by-name) / metadata / system-backup. They now live
  once in core, parameterized by a `run_admin` callable and client handles. Both adapters (and
  the forthcoming CLI) call one implementation. Behaviour unchanged.

## [0.2.0] — 2026-07-11

### Added
- Multi-cloud object store (#52): Azure Blob (`CLOUD=azure`) and GCS (`CLOUD=gcp`) backends
  alongside S3. They sit behind one `ObjectStore` protocol with an `object_store()` factory.
  Shared cloud-agnostic composites (`_BaseObjectStore`) sit above per-cloud primitives. Each is validated against its local emulator: MinIO, Azurite or fake-gcs-server
  (compose profiles `azure` / `gcp`). Install the matching extra:
  `pip install 'neo4j-backup-dagster[azure]'` / `[gcp]`.
- Cloud-agnostic restore validation: a `file://` seed (FileSeedProvider) restores from a
  local/mounted artifact. This validates restores on every cloud
  (`just file-restore-smoke`), bypassing Neo4j's per-cloud `gs://`/`azb://` fetch.

### Changed
- `ObjectStore.s3_uri` is now `uri` (cloud-neutral: returns `s3://` / `azb://` / `gs://`).

## [0.1.0] — 2026-07-10

First tagged release. Validated end to end against a real Neo4j Enterprise and object-store
stack (shell, Dagster, Airflow, PITR). Also the `RUNNER_MODE=k8s` path on k3d.

### Added
- Core loop: policy-driven `neo4j-admin` backup to an S3-compatible store. Seed-from-URI
  restore with alias-swap cutover. Non-destructive `verify`, retention `prune`, chain
  `aggregate` and PITR (`seedRestoreUntil`). All run over a shared `neo4j_backup_core` engine
  with interchangeable Dagster and Airflow adapters.
- Metadata and system: agentless DBMS metadata export/replay (users/roles/privileges/aliases).
  A binary `system`-database backup and offline restore runbook.
- Seed topology: per-group `TOPOLOGY n PRIMARIES m SECONDARIES`, so restores keep their
  redundancy (#20).
- Bolt resilience: bounded transient retry classified by Neo4j status code (#19, #24, #25). It
  runs over one shared client path (`Neo4jClient.run_on`).
- Pluggable seams: secret provider (env / AWS Secrets Manager, #18). Cutover strategy
  (alias-swap / external router, #17). Object-store path layout (`PATH_LAYOUT`, #21) and policy
  source (`s3://` with TTL cache and last-known-good, #43). Loader override (`POLICY_LOADER`)
  for authenticated endpoints (#46).
- Encryption on every write: `S3_SSE`/`S3_WRITE_ARGS` on the pipeline's boto3 PUT/COPY.
  `BACKUP_UPLOAD=pipeline` routes neo4j-admin's S3 writes (backup/system/aggregate/verify)
  through boto3, for buckets that require an SSE header (#39).
- By-name mode: per-group `restore_mode: by-name` backs up and restores databases by their own
  name. It does create-if-absent, or a gated destructive replace. Backup accepts an alias or a
  physical name (`resolve_physical`) (#48).
- Seed Cypher version: `SEED_CYPHER_VERSION` couples `existingData` (required in Cypher 5,
  deprecated in Cypher 25).
- Runner: subprocess (EC2/VM) and `RUNNER_MODE=k8s` execution; `RUNNER_NEO4J_ADMIN`.

### Fixed
- PITR bracketing now polls the server clock instead of fixed sleeps (#26).
- Dagster `prune` `list_text_keys` delegation; `RUNNER_NEO4J_ADMIN` wiring.
- Pinned `grpcio-health-checking<1.82` (protobuf gencode/runtime drift).

[Unreleased]: https://github.com/lex00/neo4j-backup/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/lex00/neo4j-backup/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/lex00/neo4j-backup/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/lex00/neo4j-backup/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/lex00/neo4j-backup/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/lex00/neo4j-backup/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lex00/neo4j-backup/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lex00/neo4j-backup/releases/tag/v0.1.0
