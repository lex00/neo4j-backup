# Roadmap

Forward-looking index for the Neo4j backup/restore project. Status as of 2026-06-28.
Entry point [`README.md`](README.md); architecture [`DESIGN.md`](DESIGN.md); local stack
[`STACK.md`](STACK.md); configuration walkthrough
[`orchestrator/README.md`](orchestrator/README.md); diagrams in
[`diagrams/`](diagrams/README.md) (`just diagrams`).

Scope note: this project is design + local validation. Cloud provisioning is out of
scope — teams adapt the runner/placement to their environment (see
[`orchestrator/deploy/DEPLOY.md`](orchestrator/deploy/DEPLOY.md)).

Legend: [x] done · [~] in progress / scaffolded-not-validated · [ ] not started

## Phase 0 — Grounding (done)

- [x] Design grounded in the current Neo4j operations manual + Dagster docs: seed-from-URI
      + PITR, alias semantics, MinIO/HTTP, runner memory/scratch, and Dagster
      code-location / placement / concurrency.

## Phase 1 — Architecture (done)

- [x] db-group as the policy + PITR-alignment unit; customer = optional tag
- [x] Execution surface: instances agentless; restore pure Cypher; backup = CLI on a
      runner (no Cypher backup API exists)
- [x] Restore strategy: seed-from-URI into unique names, then alias-swap cutover
- [x] Naming authority: alias (full set, preserved) / slug / physical
- [x] Encryption: SSE-KMS, cloud KMS, per-group bucket/key (keeps agentless restore)
- [x] Runner resource model: configurable scratch volume, HEAP_SIZE + --pagecache,
      consistency check out-of-band, full/diff lanes
- [x] Dagster design: dedicated code location, k8s placement, concurrency lanes + pool,
      Pipes for backup / Bolt-Cypher for restore (`DESIGN.md` §6)

## Phase 2 — Local stack scaffold (done, not yet validated)

- [x] `justfile`, single-node `docker/compose.yaml` (neo4j + MinIO SSE-KMS + runner +
      scratch volume), `policies/demo.yaml`, demo data
- [x] `bootstrap/naming.sh` (naming authority) with passing self-test
- [x] `bootstrap/{lib,bootstrap,backup,restore}.sh` — runner-as-Bolt-client, alias-swap
      restore with group-aligned PITR
- [x] Validated end-to-end 2026-06-29 (see Phase 3)

## Phase 3 — Validate the local loop (done 2026-06-29)

- [x] Boot + bootstrap from scratch (naming authority, aliases, runner-as-Bolt-client)
- [x] `just backup demo` → SSE-KMS artifacts in MinIO (`Encryption: SSE-KMS` confirmed)
- [x] `just restore demo` → seed-from-URI over plain-HTTP MinIO reads the encrypted
      artifact; alias-swap cutover; data verified through alias routing
- [x] All four flagged risks cleared: MinIO plain-HTTP seed, path-style addressing,
      SSE-KMS read via seed-from-URI, `eval` license
- [x] PITR demonstrated (`just demo-pitr`): full → change → differential chain, then
      `seedRestoreUntil=T0` restored the pre-change state (2), HEAD restored 3.
      Confirms `seedRestoreUntil` needs a differential **chain** (a lone full errors).

### Bugs found + fixed during validation
- `latest_artifact` ran awk/grep inside the minimal `mc` image → switched to `mc find`
  + host-side parsing.
- CloudSeedProvider rejects `seedConfig` → removed it (region/endpoint via server env).

## Phase 4 — Dagster code location (built + validated 2026-06-29)

Package at `orchestrator/` (`neo4j_backup_dagster`); `dagster definitions validate`
passes; `pytest` green.

- [x] `neo4j_backup_dagster` package: one `Definitions` (validates clean)
- [x] `naming.py` port of `naming.sh` — parity test passes across 9 inputs
- [x] `policy.py` pydantic models + loader (validates `demo.yaml`)
- [x] Resources: `Neo4jResource` (Bolt), `ObjectStoreResource`, `RunnerResource`
      (HEAP_SIZE/--pagecache/--temp-path), `PipesSubprocessClient`
- [x] Backup asset via Pipes; `backup_kind` full/diff lane tag; `pool="neo4j"`
- [x] Restore job via Bolt Cypher (seed-from-URI + `ALTER ALIAS`, group-aligned)
- [x] Dynamic partitions `(group, alias)` + tier schedules + reconciliation sensor
- [x] Backup + restore driven THROUGH Dagster against the live stack
      (`orchestrator/smoke_local.py`): backup asset → Pipes → `neo4j-admin` in the
      runner; restore job → Bolt seed-from-URI + `ALTER ALIAS`; verified via alias.
- [ ] Swap `PipesSubprocessClient` → `PipesK8sClient` for the multi-TB prod runner

## Phase 5 — Close the loop: verify + retain (done 2026-06-29)

Driven through Dagster against the live stack (`orchestrator/smoke_verify.py`).

- [x] Verification simplified: `database check --from-path=s3://…` reads the artifact
      directly (no ephemeral instance / seed). `verify` job — validated, exit-0 = pass.
- [x] `aggregate_chain` job (`neo4j-admin backup aggregate`) collapses a chain into a
      recovered full in place — the RTO/retention lever and what makes a chain checkable.
- [x] `prune` asset: age-based retention (keeps the chain head), keyed off
      `retention_days`. Runs clean.
- [x] Backup run metadata (artifact key, bytes) on the backup asset's MaterializeResult.
- [ ] Wire `aggregate`/`verify` to partitions + a verification schedule (after the
      per-store layout below).

### Finding (validated): chains are per physical store
A backup chain is per-store, but backups are keyed by alias-slug — after a restore the
alias points at a new physical, so the slug prefix mixes stores. Refinement: key backup
prefixes by **physical db name**, resolve the live chain via alias → current-physical.
Tracked in Phase 6; `aggregate`/`verify` take an explicit `(database, prefix)` for now.

## Phase 6 — Production hardening

- [x] Per-store backup layout `<group>/<slug>/<physical>/` — implemented + validated
      (`orchestrator/smoke_phase6.py`): real differential chains, non-destructive verify,
      `aggregate`/`verify` partitioned by `(group, alias)`.
- [x] Deployment artifacts authored (`orchestrator/deploy/`): `dagster.yaml` lane limits
      + source pool; `DEPLOY.md` covering VM/EC2 (default) and k8s (optional).
- [x] VM/EC2 execution model is the validated `PipesSubprocessClient` path — no k8s
      required (set `exec_prefix=[]` with neo4j-admin on the worker).

The in-scope work — the **validated Dagster pipelines** — is complete. Out of scope
(teams adapt, DEPLOY.md): the **object-store / bucket / IAM / KMS structure**, the
scratch volume, credentials, network, and the optional `PipesK8sClient` branch are all
environment-specific. The pipeline is object-store-agnostic (a configurable bucket +
`group/slug/physical` prefixes); the policy carries `s3_prefix` + `kms_key_ref` per
group for teams that want per-group buckets/keys.

## Drop-in readiness (2026-06-29, from an ops-person audit)

- [x] Real-S3 wiring: `AWS_ENDPOINT_URL_S3` is now optional (unset on AWS); only
      `NEO4J_PASSWORD` is required, the rest default sensibly.
- [x] `RUNNER_MODE=subprocess|k8s` — the k8s branch (`PipesK8sClient` + ephemeral PVC)
      is built in behind a flag, not a doc diff. Subprocess remains the validated path.
- [x] Alias prerequisite documented (apps connect via aliases) + `bootstrap/adopt.sh`
      to adopt existing databases.
- [x] Go-live checklist + env-var reference in `orchestrator/README.md`.
- [x] Target audience + "no lock-in" stated in `README.md`.
- [x] `RUNNER_MODE=k8s` validated on **k3d** (`just k3d-up` + `just k3d-smoke`):
      `PipesK8sClient` launched a pod that ran `neo4j-admin` with a fresh ephemeral
      scratch PVC and wrote a backup. Surfaced + fixed: pod S3 creds via `extra_env`,
      `imagePullPolicy`, host IP vs `host.k3d.internal`, publishing 6362.

## Phase 7 — Deferred / optional

- [ ] Cluster topologies: `docker/compose.cluster.yaml` (verified config parked in
      `STACK.md`); back up from a secondary/follower
- [ ] k3d path to test true k8s placement/affinity
- [ ] NOM (monitoring only) if/when `backup-manager` ships — not for backup today

## Decisions locked

| Decision | Choice |
|---|---|
| Start scope | Single node; cluster deferred |
| Local env | Docker Compose first; k3d later |
| Restore | Pure Cypher seed-from-URI + alias-swap (path A) |
| Encryption | SSE-KMS, cloud KMS, per-group bucket/key |
| Tenancy unit | db-group primary; customer optional tag |
| Naming | alias (full set, preserved) / slug / physical, one authority |
| Orchestration | Dedicated Dagster code location |
| Runner memory | HEAP_SIZE + `--pagecache` explicit; check out-of-band |
| Scratch | Configurable volume, sized to largest full (multi-TB) |

## Open risks / findings

- [resolved 2026-06-29] MinIO plain-HTTP seed, path-style addressing, and SSE-KMS read
  via seed-from-URI all validated working — no TLS proxy or `MINIO_DOMAIN` needed.
- PITR requires a differential **chain**; a lone full cannot be point-in-time restored.
- Cross-group transactional consistency is not native (bounded skew only).
- Dagster cross-version skew (location vs host) is not a documented guarantee.
