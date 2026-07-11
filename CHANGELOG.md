# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/) (0.x: a minor bump for features or breaking changes,
a patch for fixes). See [RELEASING.md](RELEASING.md).

## [Unreleased]

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

[Unreleased]: https://github.com/lex00/neo4j-backup/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/lex00/neo4j-backup/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/lex00/neo4j-backup/releases/tag/v0.1.0
