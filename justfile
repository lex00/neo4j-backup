set dotenv-load := true
set shell := ["bash", "-uc"]

compose := "docker compose --env-file .env -f docker/compose.yaml"

# list recipes
default:
    @just --list

# create .env from template if missing
_env:
    @[ -f .env ] || (cp .env.example .env && echo "created .env from .env.example")

# start the single-node stack (neo4j + floci S3 emulator + runner)
up: _env
    {{compose}} up -d --wait

# stop and remove the stack + volumes
down:
    # every profile, so Azurite, neo4j-b and the cluster go too (their containers would keep
    # pointing at the removed networks)
    {{compose}} -f docker/compose.cluster.yaml --profile tools --profile azure --profile gcp --profile multi --profile cluster down -v

# boot the demo group + data from scratch
bootstrap:
    ./bootstrap/bootstrap.sh

# up + bootstrap (full from-scratch local environment)
fresh: up bootstrap

# policy-driven backup of a group to object storage
backup group="demo":
    ./bootstrap/backup.sh {{group}}

# restore a group via seed-from-URI; optional PITR timestamp (ISO-8601)
restore group="demo" until="":
    ./bootstrap/restore.sh {{group}} "{{until}}"

# demonstrate point-in-time recovery (builds a full->change->diff chain)
demo-pitr:
    ./bootstrap/demo_pitr.sh

# offline restore of the `system` database (exact metadata: native passwords/roles/privileges)
restore-system key="":
    ./bootstrap/restore_system.sh "{{key}}"

# render the graphviz diagrams to SVG (requires graphviz)
diagrams:
    ./diagrams/render.sh

# build the docs site locally (requires `pip install mkdocs-material`)
docs:
    ./mkdocs-stage.sh && mkdocs build

# preview the docs site locally
docs-serve:
    ./mkdocs-stage.sh && mkdocs serve

# k3d: create a local cluster to validate RUNNER_MODE=k8s (requires the Compose stack up)
k3d-up:
    ./k3d/up.sh

# k3d: run the k8s-mode backup validation (pod runs neo4j-admin in the cluster)
k3d-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_k8s.py

# k3d: loads with RUNNER_MODE=k8s (#107, #109): an import build and an image build in pods
k3d-load-smoke:
    k3d image import amazon/aws-cli:2.27.50 -c neo4j-backup
    orchestrator/.venv/bin/python orchestrator/smoke_k8s_load.py

# k3d: a k8s-mode backup with bucket credentials from a Kubernetes Secret (#229)
k3d-secret-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_k8s_secret.py

# k3d: delete the validation cluster
k3d-down:
    ./k3d/down.sh

# install the Airflow adapter into its own venv (uv)
airflow-install:
    ./airflow/install.sh

# run Airflow standalone (UI on :8080) wired to the Compose stack
airflow-standalone:
    ./airflow/standalone.sh

# CLI: full loop (targets/backup/verify/aggregate/restore/metadata/system/prune) via neo4j-backup
cli-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_cli.py

# MCP: read-only round trip against the operator server (needs the [mcp] extra + a backup)
mcp-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_mcp.py

# Artifact sidecars (#118): every demo artifact has metadata; chains read; backfill (needs a backup)
sidecar-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_sidecars.py

# Load (#107): CSV and Parquet builds, read-only and read-write groups
load-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_load.py

# Rebuild lane (#108): the Dagster `<tier>_rebuild` schedule and the Airflow `neo4j_rebuild_<tier>` DAG
rebuild-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_rebuild_dagster.py
    airflow/.venv/bin/python airflow/smoke_rebuild.py

# Restore operations (#121, #122, #93, #92, #123, #88): member restore, rollback, cleanup, adopt, status, naming
ops-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_ops.py

# Restore into another cluster (#115): starts a second Neo4j (`neo4j-b`, profile multi) as cluster `west`
move-smoke:
    {{compose}} --profile multi up -d --wait neo4j-b
    orchestrator/.venv/bin/python orchestrator/smoke_move.py

# Three-server cluster (#234): backup from the cluster, restore with a topology, alias on every member
cluster-smoke:
    {{compose}} -f docker/compose.cluster.yaml --profile cluster up -d --wait core1 core2 core3
    orchestrator/.venv/bin/python orchestrator/smoke_cluster.py

# Guards from the v0.6.0 audit (#216): fence in Dagster, store-id change, seed wait, driver reuse, status
guards-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_guards.py

# Azure backend on Azurite (#79, #80): upload, copy with wait, streamed download (needs a backup)
azure-smoke:
    docker compose --env-file .env -f docker/compose.yaml --profile azure up -d azurite
    orchestrator/.venv/bin/python orchestrator/smoke_azure.py

# Dump path (#161): import -> dump -> seed -> baseline -> diff -> point-in-time -> export
dump-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_dump.py

# Bulk import (#16): import a tiny CSV -> adopt -> backup -> seed, end to end (needs the stack)
import-smoke:
    orchestrator/.venv/bin/python orchestrator/smoke_import.py

# Airflow: backup -> verify -> restore -> prune against the stack (dag.test, in-process)
airflow-smoke:
    airflow/.venv/bin/python airflow/smoke_e2e.py

# Airflow: real differential chain + point-in-time restore validation
airflow-pitr:
    airflow/.venv/bin/python airflow/smoke_pitr.py

# Airflow: k8s execution mode (KubernetesPodOperator) against k3d (needs `just k3d-up`)
airflow-k8s-smoke:
    airflow/.venv/bin/python airflow/smoke_k8s.py

# list backup artifacts in object storage
artifacts prefix="":
    {{compose}} run --rm -T awscli s3 ls --recursive "s3://$BACKUP_BUCKET/{{prefix}}"

# tail logs (optionally for one service)
logs service="":
    {{compose}} logs -f {{service}}

# show stack status
ps:
    {{compose}} ps

# print service URLs / credentials
urls:
    @echo "Neo4j Browser : http://localhost:7474  (neo4j / $NEO4J_PASSWORD)"
    @echo "S3 (floci)    : http://localhost:9000  (any credentials; list with: just artifacts)"

# validate a release: __version__ matches, CHANGELOG has the section, then print tag/push cmds
release version:
    @v=$(grep -oE '__version__ = "[^"]+"' orchestrator/neo4j_backup_core/__init__.py | cut -d'"' -f2); \
     [ "$v" = "{{version}}" ] || { echo "!! __version__ is $v, not {{version}} — bump orchestrator/neo4j_backup_core/__init__.py first" >&2; exit 1; }; \
     grep -q "^## \[{{version}}\]" CHANGELOG.md || { echo "!! CHANGELOG.md has no section for {{version}}" >&2; exit 1; }; \
     git diff --quiet && git diff --cached --quiet || { echo "!! working tree not clean — commit the bump first" >&2; exit 1; }; \
     echo ">> {{version}} looks release-ready. Tag + push (from main, CI green):"; \
     echo "     git tag v{{version}} && git push origin v{{version}}"

# validate restore cloud-agnostically via file:// seed (#52) — needs the stack + a backup
file-restore-smoke:
    AWS_ENDPOINT_URL_S3=http://localhost:9000 orchestrator/.venv/bin/python orchestrator/smoke_file_restore.py
