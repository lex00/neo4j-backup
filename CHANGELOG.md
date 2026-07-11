# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: a minor bump for features or breaking changes,
a patch for fixes). See [RELEASING.md](RELEASING.md).

## [Unreleased]

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

[Unreleased]: https://github.com/lex00/neo4j-backup/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/lex00/neo4j-backup/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/lex00/neo4j-backup/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lex00/neo4j-backup/releases/tag/v0.1.0
