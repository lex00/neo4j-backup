"""Airflow failure callback (#246): every DAG of this package sets it, and it posts to
NEO4J_BACKUP_ALERT_WEBHOOK when a task fails (nothing when unset). For DAGs of your own, or to
use Airflow's own notifiers instead, see airflow/README.md."""

from neo4j_backup_core import alerts


def on_failure(context) -> None:
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or getattr(context.get("dag"), "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    link = getattr(ti, "log_url", None)
    alerts.send(alerts.message("Airflow DAG", f"{dag_id}.{task_id}",
                               str(context.get("exception") or ""), link))


DEFAULT_ARGS = {"on_failure_callback": on_failure}
