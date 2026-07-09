# Neo4j Policy-Based Backup / Restore

### 📖 Documentation site: **<https://lex00.github.io/neo4j-backup/>**

**Validated, policy-driven Dagster pipelines** for backing up and restoring self-hosted
Neo4j Enterprise — exercised end to end against a real Neo4j Enterprise + object-store
stack. The database instances stay **agentless**: restore is pure Cypher over Bolt
(seed-from-URI + alias swap), and backup is `neo4j-admin` run from a separate runner.

The deliverable is the orchestration — a [Dagster](orchestrator/README.md) code location
and an equivalent [Airflow](airflow/README.md) DAG set over one shared engine
(`neo4j_backup_core`), pick whichever you run — and its validation. The pipeline is
**object-store-agnostic** — it takes a
configurable bucket and writes `<group>/<slug>/<physical>/` prefixes; teams bring their
own bucket / IAM / KMS structure and adapt the runner placement to their environment
(see [Adapting](orchestrator/deploy/DEPLOY.md)). Cloud provisioning is out of scope.

## Configuration: every config, and exactly where it lives

There are **four** configs — no more. Two are files in *this* repo you copy/edit; two
live in *your* Dagster deployment. That's the complete list:

| Config | Exact location | Whose file | What it controls |
|---|---|---|---|
| **1. [Backup policy](POLICY.md)** | `policies/demo.yaml` → copy to `policies/<you>.yaml` | this repo (you edit) | which databases/groups to back up, their aliases, schedule tiers, retention |
| **2. Environment variables** | your code location's environment — locally `.env` (from `.env.example`); in prod, your Dagster env/secrets | you set them | point at your Neo4j (`NEO4J_BOLT_URI`, `NEO4J_PASSWORD`), your bucket (`BACKUP_BUCKET`, `AWS_REGION`), the backup source, and `NEO4J_BACKUP_POLICY` = path to #1 |
| **3. Concurrency lanes** | [`orchestrator/deploy/dagster.yaml`](orchestrator/deploy/dagster.yaml) → merge into your instance's `dagster.yaml` | this repo (copy the lines) | how many full vs diff backups run at once |
| **4. Code-location entry** | your `workspace.yaml` (OSS) or `dagster_cloud.yaml` (Dagster+) | your Dagster repo | registers this package with Dagster |

Defaults: only `NEO4J_PASSWORD` is required (everything else has sane defaults), so the
**only file you must write is #1, the policy** — see the **[Policy](POLICY.md)** page for
a complete annotated example and the full field reference.

**What to put in each →** the step-by-step
[Configuration walkthrough](orchestrator/README.md) (in the orchestrator README).

## Who this is for

Ops / platform / data-engineering teams that:

- self-host **Neo4j Enterprise** — online backup and seed-from-URI restore are Enterprise
  features (Community has only offline dump/load);
- already run **Dagster** or **Airflow** and want to add backups as a code location /
  DAG set (the two adapters are interchangeable — [Airflow](airflow/README.md));
- store artifacts in an **S3-compatible object store**;
- are comfortable operating `neo4j-admin`, Cypher, and Dagster; and
- have apps connect via Neo4j **aliases**, or are willing to migrate to them
  (the restore model is an alias swap — see
  [orchestrator/README.md](orchestrator/README.md)).

Not for: Neo4j **Community** (no online backup), Neo4j **Aura** (managed, has its own
snapshots), or anyone wanting a turnkey/GUI product. This is tooling to adapt, not an
appliance — deployment specifics are yours.

## No lock-in

It is just orchestration around standard pieces:

- **Standard `neo4j-admin` backups** — ordinary `.backup` artifacts in your bucket,
  restorable with `neo4j-admin` / seed-from-URI **without this package**.
- **Standard Cypher** for restore (`CREATE DATABASE … seedURI`, `ALTER ALIAS`) — nothing
  proprietary, no custom format.
- **Plain-YAML policy**, standard Neo4j **aliases**, a standard Dagster **code location**.
- **Your** object store, IAM, KMS, and placement — the pipeline is object-store-agnostic.

Remove the tooling and you are left with standard Neo4j backups in your own bucket.

## Configurable & resilient (optional)

Beyond the four configs, the pipeline exposes optional knobs — **all default to the validated
behaviour**, and the full list is the
[env table](orchestrator/README.md#environment-variables). Every seam lives in the shared core,
so both adapters inherit it:

- **Credentials** — `SECRET_PROVIDER` pulls the Neo4j password from AWS Secrets Manager (or a
  custom provider), resolved per connection so rotation needs no redeploy.
- **Bolt resilience** — automatic bounded retry on transient cluster failures (leader
  re-election, dropped sessions, expired tokens), classified by Neo4j **status code**, not
  message text.
- **Seed topology** — declare `TOPOLOGY n PRIMARIES m SECONDARIES` in policy so a restore comes
  back with its intended redundancy, not the DBMS default.
- **Cypher version** — `SEED_CYPHER_VERSION=5` on a Cypher-5 cluster (adds the required
  `existingData`; Cypher 25 omits it).
- **Cutover** — default alias swap, or repoint an external router/proxy via
  `CUTOVER_STRATEGY=external`.
- **Path layout** — bring your own object-store key scheme via `PATH_LAYOUT`.
- **Encryption on every write** — `S3_SSE` sets the SSE-KMS header on the pipeline's boto3
  PUT/COPY; `BACKUP_UPLOAD=pipeline` routes neo4j-admin's writes through boto3 too (it has no
  SSE setting of its own), so **strict buckets that deny header-less PutObject** work end to end.

## Documentation map

| Doc | What |
|---|---|
| [POLICY.md](POLICY.md) | The backup policy — a complete annotated example + every field. |
| [RECOVERY.md](RECOVERY.md) | The three recovery modes (full / differential / PITR) with exact Cypher. |
| [DESIGN.md](DESIGN.md) | The architecture: execution surface, db-group model, naming authority, encryption, runner resources, Dagster pipeline, restore + verification, and the configurable seams + resilience (§11.5). The main read. |
| [STACK.md](STACK.md) | The local stack and how to run it (`just fresh` → `backup` → `restore`). |
| [orchestrator/](orchestrator/README.md) | The `neo4j_backup_dagster` package (the Dagster orchestration), with its own README + `deploy/`. **Includes the configuration walkthrough.** |
| [airflow/](airflow/README.md) | The equivalent Airflow 3.x DAG set over the same core — the DAG inventory, pools-as-lanes, `dag.test()` validation, and the Dagster↔Airflow concept map. |

## Diagrams

Graphviz sources in [`diagrams/`](diagrams/README.md); render to SVG with `just diagrams`
(requires graphviz).

| Diagram | Shows |
|---|---|
| [architecture](diagrams/architecture.svg) | Execution surface — agentless instances, runner does backup, Cypher does restore |
| [storage-layout](diagrams/storage-layout.svg) | `<group>/<slug>/<physical>/` layout and per-store chains |
| [restore-cutover](diagrams/restore-cutover.svg) | Seed a fresh physical → verify → move the alias |
| [dagster-pipeline](diagrams/dagster-pipeline.svg) | Assets, jobs, schedules, sensor |
| [naming](diagrams/naming.svg) | alias → slug → physical naming authority |

## Quick start (local)

```bash
just fresh          # boot the stack + demo group from scratch
just backup demo    # online backup to object storage (SSE-KMS)
just restore demo   # seed-from-URI restore + alias swap
just demo-pitr      # point-in-time recovery over a real chain
just diagrams       # render the diagrams to SVG
```

The Dagster package is validated against this same stack — see
[orchestrator/README.md](orchestrator/README.md).

## What's validated

The full loop runs four ways: shell (`just`), Dagster (`orchestrator/smoke_*.py`), Airflow
(`airflow/smoke_*.py`), and PITR. Backups are SSE-KMS encrypted, restore reads them via seed-from-URI over Bolt,
verification consistency-checks artifacts non-destructively, and the per-store layout
makes differential chains correct. `RUNNER_MODE=k8s` is validated on k3d for both adapters
(`just k3d-smoke` for Dagster, `just airflow-k8s-smoke` for Airflow). Decisions locked and
open risks are in [DESIGN.md §13](DESIGN.md).

## CI & docs site

- **CI** (`.github/workflows/ci.yml`): naming parity (`naming.py` == `naming.sh`), Dagster
  definitions validation, and the Airflow DAG import-error check on every push — no
  Docker/Neo4j needed.
- **Docs** (`.github/workflows/pages.yml`): this documentation + rendered diagrams
  publish to GitHub Pages. One-time: repo Settings → Pages → Source = "GitHub Actions".
  Build locally with `just docs` (needs `pip install mkdocs-material`).
