#!/usr/bin/env bash
# Stage the curated docs (which live at the repo root for GitHub) into docs/ for MkDocs,
# preserving structure so relative links resolve. Also renders the diagrams to SVG.
set -euo pipefail
cd "$(dirname "$0")"

command -v dot >/dev/null 2>&1 && ./diagrams/render.sh || echo "!! graphviz not found; SVGs may be stale"

rm -rf docs
mkdir -p docs/orchestrator/deploy docs/diagrams docs/bootstrap

cp README.md POLICY.md DESIGN.md STACK.md ROADMAP.md docs/
cp orchestrator/README.md docs/orchestrator/
cp orchestrator/deploy/DEPLOY.md orchestrator/deploy/dagster.yaml docs/orchestrator/deploy/
cp diagrams/README.md diagrams/*.dot diagrams/*.svg docs/diagrams/ 2>/dev/null || true
cp bootstrap/adopt.sh docs/bootstrap/    # referenced from orchestrator docs

echo ">> staged docs/ for mkdocs"
