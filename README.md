# Neo4j Policy-Based Backup / Restore

**Validated, policy-driven Dagster pipelines** for backing up and restoring self-hosted
Neo4j Enterprise — exercised end to end against a real Neo4j Enterprise + object-store
stack. The database instances stay **agentless**: restore is pure Cypher over Bolt
(seed-from-URI + alias swap), and backup is `neo4j-admin` run from a separate runner.

The deliverable is the orchestration (the [`neo4j_backup_dagster`](orchestrator/README.md)
package) and its validation. The pipeline is **object-store-agnostic** — it takes a
configurable bucket and writes `<group>/<slug>/<physical>/` prefixes; teams bring their
own bucket / IAM / KMS structure and adapt the runner placement to their environment
(see [Adapting](orchestrator/deploy/DEPLOY.md)). Cloud provisioning is out of scope.

## Who this is for

Ops / platform / data-engineering teams that:

- self-host **Neo4j Enterprise** — online backup and seed-from-URI restore are Enterprise
  features (Community has only offline dump/load);
- already run **Dagster** and want to add backups as a code location;
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

## Documentation map

| Doc | What |
|---|---|
| [DESIGN.md](DESIGN.md) | The architecture: execution surface, db-group model, naming authority, encryption, runner resources, Dagster pipeline, restore + verification. The main read. |
| [STACK.md](STACK.md) | The local stack and how to run it (`just fresh` → `backup` → `restore`). |
| [ROADMAP.md](ROADMAP.md) | Phase-by-phase status and validated findings. |
| [orchestrator/](orchestrator/README.md) | The `neo4j_backup_dagster` package (the real orchestration), with its own README + `deploy/`. |
| [RESEARCH.md](RESEARCH.md) | The original landscape research: feature matrix and gaps (native vs commercial vs OSS). |
| [findings.md](findings.md) · [second-pass-neo4j-docs.md](second-pass-neo4j-docs.md) | The verified claim ledger and docs deep-dive behind the research. |

## Diagrams

Graphviz sources in [`diagrams/`](diagrams/README.md); render to SVG with `just diagrams`
(requires graphviz).

| Diagram | Shows |
|---|---|
| [architecture](diagrams/architecture.dot) | Execution surface — agentless instances, runner does backup, Cypher does restore |
| [storage-layout](diagrams/storage-layout.dot) | `<group>/<slug>/<physical>/` layout and per-store chains |
| [restore-cutover](diagrams/restore-cutover.dot) | Seed a fresh physical → verify → move the alias |
| [dagster-pipeline](diagrams/dagster-pipeline.dot) | Assets, jobs, schedules, sensor |
| [naming](diagrams/naming.dot) | alias → slug → physical naming authority |

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

The full loop runs three ways: shell (`just`), Dagster (`orchestrator/smoke_*.py`), and
PITR. Backups are SSE-KMS encrypted, restore reads them via seed-from-URI over Bolt,
verification consistency-checks artifacts non-destructively, and the per-store layout
makes differential chains correct. `RUNNER_MODE=k8s` is validated on k3d
(`just k3d-up` + `just k3d-smoke`). See ROADMAP for phase status and the issues that
running it surfaced.

## CI & docs site

- **CI** (`.github/workflows/ci.yml`): naming parity (`naming.py` == `naming.sh`) and
  Dagster definitions validation on every push — no Docker/Neo4j needed.
- **Docs** (`.github/workflows/pages.yml`): this documentation + rendered diagrams
  publish to GitHub Pages. One-time: repo Settings → Pages → Source = "GitHub Actions".
  Build locally with `just docs` (needs `pip install mkdocs-material`).
