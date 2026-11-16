#!/usr/bin/env bash
# Stage the curated docs (which live at the repo root for GitHub) into docs/ for MkDocs,
# preserving structure so relative links resolve. Also renders the diagrams to SVG.
set -euo pipefail
cd "$(dirname "$0")"

command -v dot >/dev/null 2>&1 && ./diagrams/render.sh || echo "!! graphviz not found; SVGs may be stale"

rm -rf docs
mkdir -p docs/orchestrator/deploy docs/diagrams docs/bootstrap docs/airflow/deploy docs/examples/ci docs/policies docs/docker

cp README.md GETTING-STARTED.md OPERATOR.md CUSTOMERS.md POLICY.md RECOVERY.md DESIGN.md STACK.md CLI-CONTRACT.md AGENTS.md CI.md MCP.md IMPORT.md ENVIRONMENT.md GLOSSARY.md docs/
cp CHANGELOG.md RELEASING.md docs/          # linked from the README doc map
cp orchestrator/README.md docs/orchestrator/
cp airflow/README.md docs/airflow/
cp orchestrator/deploy/DEPLOY.md orchestrator/deploy/dagster.yaml orchestrator/deploy/helm-values.yaml \
   orchestrator/deploy/Dockerfile docs/orchestrator/deploy/
cp airflow/deploy/* docs/airflow/deploy/     # the Airflow deploy kit
cp docker/compose.yaml docs/docker/          # the demo server settings (GETTING-STARTED.md)
cp diagrams/README.md diagrams/*.dot diagrams/*.svg docs/diagrams/ 2>/dev/null || true
cp bootstrap/adopt.sh docs/bootstrap/    # referenced from orchestrator docs and OPERATOR.md
cp policies/demo.yaml policies/example.yaml docs/policies/   # linked from GETTING-STARTED.md
cp examples/ci/*.yml examples/ci/README.md docs/examples/ci/   # linked from CI.md

echo ">> staged docs/ for mkdocs"
