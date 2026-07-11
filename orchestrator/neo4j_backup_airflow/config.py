"""Build `neo4j_backup_core` clients from the runtime environment, for the Airflow DAGs.

Thin re-export of `neo4j_backup_core.env` (the same builder the CLI uses) so the DAGs keep
calling `config.neo4j()` / `config.store()` / `config.runner()` / `config.policy_path()`.
Importing this pulls in no Airflow — only `neo4j_backup_core`.
"""

from neo4j_backup_core.env import neo4j, policy_path, runner, store  # noqa: F401
