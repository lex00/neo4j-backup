# CI recipes

Copy-and-adapt templates for scheduling the `neo4j-backup` CLI from CI, for teams without an
orchestrator. Full write-up, execution model, secrets, and caveats: [CI.md](../../CI.md).

- [`github-actions.yml`](github-actions.yml) — GitHub Actions (self-hosted runner).
- [`gitlab-ci.yml`](gitlab-ci.yml) — GitLab CI (`resource_group` lanes — the best fit).
- [`forgejo-actions.yml`](forgejo-actions.yml) — Forgejo / Gitea Actions (a runner *is* a backup runner).

These are templates, not wired into this repo's CI. Each runs on a self-hosted runner that has
`neo4j-admin`, egress to the DB backup port (6362) and object store, and a sized scratch disk.
