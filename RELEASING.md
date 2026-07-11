# Releasing

This project is consumed by **vendoring the source and pinning a git tag** — there is no PyPI
publish. A release is a version bump, a `CHANGELOG` entry, and a `vX.Y.Z` tag; pushing the tag
triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which creates the
GitHub Release (auto-generated notes + the tag's source archive).

## Versioning

[Semantic Versioning](https://semver.org/), 0.x cadence:

- **minor** (`0.N.0`) — new features **or** breaking changes (pre-1.0, both bump the minor)
- **patch** (`0.N.P`) — bug fixes only

The version lives in **one place**: `__version__` in
[`orchestrator/neo4j_backup_core/__init__.py`](orchestrator/neo4j_backup_core/__init__.py).
`pyproject.toml` derives it (`[tool.setuptools.dynamic]`), and `neo4j_backup_core.__version__`
reports it at runtime — which works even when the source is vendored (not pip-installed).

## Cut a release

1. Bump `__version__` in `neo4j_backup_core/__init__.py`.
2. In `CHANGELOG.md`, rename the `[Unreleased]` section to `[X.Y.Z] — YYYY-MM-DD` (keep an empty
   `[Unreleased]` above it) and update the compare/link footnotes.
3. Commit on a branch, open a PR, merge to `main` with CI green.
4. From `main`: `just release X.Y.Z` prints the checks + the exact tag/push commands. Then run
   them:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
5. `release.yml` fires on the tag and publishes the GitHub Release. Verify at
   <https://github.com/lex00/neo4j-backup/releases>.

Tagging is a deliberate human step — the `just release` recipe validates but does **not** push a
tag for you.

## Consuming a release

```
# vendor a specific tag
git clone --branch vX.Y.Z https://github.com/lex00/neo4j-backup

# or pip-install the package from the tag
pip install "neo4j-backup-dagster @ git+https://github.com/lex00/neo4j-backup@vX.Y.Z#subdirectory=orchestrator"
```
