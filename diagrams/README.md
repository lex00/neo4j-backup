# Diagrams

Graphviz (`.dot`) sources. Render to SVG with `just diagrams` (from the repo root) or
`./render.sh` here — requires graphviz (`brew install graphviz`).

| Source | Renders | Shows |
|---|---|---|
| `architecture.dot` | `architecture.svg` | Execution surface: agentless instances, runner runs `neo4j-admin` (backup), Cypher does restore, DB nodes pull seeds |
| `storage-layout.dot` | `storage-layout.svg` | `<group>/<slug>/<physical>/<artifact>.backup` and per-store chains |
| `restore-cutover.dot` | `restore-cutover.svg` | Seed a fresh physical → verify → `ALTER ALIAS` → rollback/cleanup |
| `dagster-pipeline.dot` | `dagster-pipeline.svg` | Code location: backup/aggregate/verify/prune assets, restore job, schedules, sensor |
| `naming.dot` | `naming.svg` | alias → slug → physical naming authority |

SVGs are generated artifacts; re-render after editing a `.dot`.
