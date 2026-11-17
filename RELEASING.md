# Releasing

Users vendor the source or pin a git tag; releases go to GitHub only, with PyPI out of scope.
A release is a version bump, a `CHANGELOG` entry and a `vX.Y.Z` tag. Pushing the tag
triggers [`.github/workflows/release.yml`](.github/workflows/release.yml). It creates the
GitHub Release with auto-generated notes and the tag's source archive.

## Versioning

[Semantic Versioning](https://semver.org/), 0.x cadence:

- Minor (`0.N.0`): new features or breaking changes (pre-1.0, both bump the minor).
- Patch (`0.N.P`): bug fixes only.

The version lives in one place: `__version__` in
[`orchestrator/neo4j_backup_core/__init__.py`](orchestrator/neo4j_backup_core/__init__.py).
`pyproject.toml` derives it (`[tool.setuptools.dynamic]`). `neo4j_backup_core.__version__`
reports it at runtime, in vendored source as well as a pip install.

## Cut a release

1. Bump `__version__` in `neo4j_backup_core/__init__.py`.
2. In `CHANGELOG.md`, rename the `[Unreleased]` section to `[X.Y.Z] - YYYY-MM-DD` (keep an empty
   `[Unreleased]` above it) and update the compare/link footnotes.
3. Commit on a branch, open a PR, merge to `main` with CI green.
4. From `main`: `just release X.Y.Z` prints the checks and the exact tag and push commands. Run
   them:
   ```bash
   git tag vX.Y.Z
   git push origin vX.Y.Z
   ```
5. `release.yml` fires on the tag and publishes the GitHub Release. Verify at
   <https://github.com/lex00/neo4j-backup/releases>.

A person tags the release. The `just release` recipe validates and leaves the tag push to you.

## Consuming a release

```
# vendor a specific tag
git clone --branch vX.Y.Z https://github.com/lex00/neo4j-backup

# or pip-install the package from the tag
pip install "neo4j-backup-dagster @ git+https://github.com/lex00/neo4j-backup@vX.Y.Z#subdirectory=orchestrator"
```
