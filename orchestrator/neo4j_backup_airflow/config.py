"""Build `neo4j_backup_core` clients from the runtime environment, for the Airflow DAGs.

Thin re-export of `neo4j_backup_core.env` (the same builder the CLI uses) so the DAGs keep
calling `config.neo4j()` / `config.store()` / `config.runner()` / `config.policy_path()`.
Importing this pulls in no Airflow — only `neo4j_backup_core`.
"""

from neo4j_backup_core.env import neo4j, policy_path, runner, store  # noqa: F401


def pool_for(cluster: str, lane: str) -> str:
    """The Airflow pool for a cluster's lane (#106): `neo4j_<lane>` for the default cluster,
    `neo4j_<cluster>_<lane>` for a named one. Shared by the backup and rebuild DAGs (#232)."""
    return f"neo4j_{lane}" if cluster == "default" else f"neo4j_{cluster}_{lane}"
