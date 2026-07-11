set dotenv-load := true
set shell := ["bash", "-uc"]

compose := "docker compose --env-file .env -f docker/compose.yaml"

# list recipes
default:
    @just --list

# create .env from template if missing
_env:
    @[ -f .env ] || (cp .env.example .env && echo "created .env from .env.example")

# start the single-node stack (neo4j + minio + runner)
up: _env
    {{compose}} up -d --wait

# stop and remove the stack + volumes
down:
    {{compose}} --profile tools down -v

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
    {{compose}} run --rm -T mc "mc alias set local http://minio:9000 $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY >/dev/null && mc ls -r local/$BACKUP_BUCKET/{{prefix}}"

# tail logs (optionally for one service)
logs service="":
    {{compose}} logs -f {{service}}

# show stack status
ps:
    {{compose}} ps

# print service URLs / credentials
urls:
    @echo "Neo4j Browser : http://localhost:7474  (neo4j / $NEO4J_PASSWORD)"
    @echo "MinIO console : http://localhost:9001  ($AWS_ACCESS_KEY_ID / $AWS_SECRET_ACCESS_KEY)"

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
    orchestrator/.venv/bin/python orchestrator/smoke_file_restore.py
