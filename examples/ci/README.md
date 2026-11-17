# CI recipes

Copy-and-adapt templates for scheduling the `neo4j-backup` CLI from CI, for teams that skip an
orchestrator. Execution model, secrets and limits: [CI.md](../../CI.md).

- [`github-actions.yml`](github-actions.yml): GitHub Actions on a self-hosted runner.
- [`gitlab-ci.yml`](gitlab-ci.yml): GitLab CI, with `resource_group` lanes (the best fit).
- [`forgejo-actions.yml`](forgejo-actions.yml): Forgejo or Gitea Actions (the CI runner is the backup runner).

This repo's CI leaves them unwired. Each needs a self-hosted runner with `neo4j-admin` and a sized scratch disk.
The runner needs egress to the backup port (6362) and the object store.
