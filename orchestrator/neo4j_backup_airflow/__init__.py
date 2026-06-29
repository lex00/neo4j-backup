"""Neo4j backup orchestration — Airflow adapter over neo4j_backup_core.

Helpers here (e.g. `config`) import only `neo4j_backup_core`, so they're importable and
testable without Airflow installed. The DAGs (in `airflow/dags/`) import `airflow.sdk`.
"""
