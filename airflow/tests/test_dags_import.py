"""CI-friendly check: every DAG in airflow/dags parses with no import errors.

Needs no live Neo4j/S3 — DAG modules must not connect at parse time (only inside tasks).
"""

import os
from pathlib import Path

from airflow.models import DagBag


def test_no_import_errors():
    repo = Path(__file__).resolve().parents[2]
    os.environ.setdefault("NEO4J_BACKUP_POLICY", str(repo / "policies" / "demo.yaml"))
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert bag.dags, "no DAGs discovered"


def test_bad_policy_is_an_import_error(tmp_path, monkeypatch):
    # #75: a policy that fails to load shows as an import error, not as missing DAGs
    bad = tmp_path / "bad.yaml"
    bad.write_text("db_groups: [ {id: x, aliases: [a], tier: nope, s3_prefix: 's3://x/'} ]\n"
                   "tiers: {}\n")
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(bad))
    from neo4j_backup_core import policy
    policy._cache.clear()
    repo = Path(__file__).resolve().parents[2]
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    errors = {Path(k).name: v for k, v in bag.import_errors.items()}
    assert "backup_dag.py" in errors and "unknown tier 'nope'" in errors["backup_dag.py"]


def test_profiled_policy_dags_see_resolved_groups(tmp_path, monkeypatch):
    # #101: a group whose tier comes from a profile gets the tier's DAGs and is in its targets
    import yaml
    repo = Path(__file__).resolve().parents[2]
    raw = yaml.safe_load((repo / "policies" / "demo.yaml").read_text())
    raw["db_groups"].append({"id": "shop", "profile": "app-live", "aliases": ["shop"],
                             "s3_prefix": "s3://x/"})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    from neo4j_backup_core import policy
    policy._cache.clear()
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert "neo4j_backup_gold_full" in bag.dags
    from neo4j_backup_airflow import config
    gold = policy.load_policy(config.policy_path()).groups_for_tier("gold")  # the targets task
    assert "shop" in [g.id for g in gold]


def test_policy_crons_drive_every_dag(tmp_path, monkeypatch):
    # #104: verify/aggregate/prune per tier and the DBMS DAGs take their crons from the policy
    import yaml
    repo = Path(__file__).resolve().parents[2]
    raw = yaml.safe_load((repo / "policies" / "demo.yaml").read_text())
    raw["tiers"]["gold"]["verify_cron"] = "30 4 * * *"
    raw["dbms"] = {"metadata_cron": "15 3 * * *"}
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    from neo4j_backup_core import policy
    policy._cache.clear()
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    sched = {d: str(bag.dags[d].schedule) for d in bag.dags}
    assert "30 4 * * *" in sched["neo4j_verify_gold"]
    assert "neo4j_aggregate_silver" not in sched  # no default aggregate (#219)
    assert "0 7 * * 0" in sched["neo4j_prune_bronze"]
    assert "15 3 * * *" in sched["neo4j_dbms_metadata_default"]
    assert "0 1 * * *" in sched["neo4j_dbms_system_default"]
    assert not {"neo4j_verify", "neo4j_aggregate", "neo4j_prune", "neo4j_metadata_backup",
                "neo4j_system_backup"} & set(bag.dags)
    # the rebuild-only tier gets its rebuild DAG (#108) and no maintenance DAGs
    nightly = sorted(d for d in bag.dags if d.endswith("_rebuild_nightly"))
    assert nightly == ["neo4j_rebuild_rebuild_nightly"], nightly
    assert "neo4j_load" in bag.dags


def test_clusters_get_their_own_dbms_dags_and_pools(tmp_path, monkeypatch):
    # #106: DBMS DAGs per cluster; backup tasks per cluster in neo4j_<cluster>_<lane> pools
    import yaml
    repo = Path(__file__).resolve().parents[2]
    raw = yaml.safe_load((repo / "policies" / "demo.yaml").read_text())
    raw["clusters"] = {"eu": {"bolt_uri": "neo4j://eu:7687"}}
    raw["db_groups"].append({"id": "shop", "aliases": ["shop"], "tier": "gold", "cluster": "eu"})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    from neo4j_backup_core import policy
    policy._cache.clear()
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    assert {"neo4j_dbms_metadata_eu", "neo4j_dbms_system_eu",
            "neo4j_dbms_metadata_default"} <= set(bag.dags)
    pools = {t.task_id: t.pool for t in bag.dags["neo4j_backup_gold_diff"].tasks}
    assert pools["backup"] == "neo4j_diff" and pools["backup_full"] == "neo4j_full"
    assert pools["backup_eu"] == "neo4j_eu_diff" and pools["backup_full_eu"] == "neo4j_eu_full"


def test_rebuild_tasks_use_their_clusters_pool(tmp_path, monkeypatch):
    """#232: a named cluster's rebuilds run in `neo4j_<cluster>_full`, like its backups."""
    import yaml
    from airflow.models import DagBag
    repo = Path(__file__).resolve().parents[2]
    raw = yaml.safe_load((repo / "policies" / "demo.yaml").read_text())
    raw["clusters"] = {"west": {"bolt_uri": "neo4j://w:7687"}}
    raw["db_groups"].append({"id": "westcat", "tier": "rebuild_nightly", "cluster": "west",
                             "aliases": ["westcat"], "load": {
                                 "mode": "rebuild", "writes": "read-only",
                                 "build": {"kind": "import", "input_type": "csv",
                                           "args": ["--nodes=n.csv"]}}})
    f = tmp_path / "p.yaml"
    f.write_text(yaml.safe_dump(raw))
    monkeypatch.setenv("NEO4J_BACKUP_POLICY", str(f))
    from neo4j_backup_core import policy
    policy._cache.clear()
    bag = DagBag(dag_folder=str(repo / "airflow" / "dags"), include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    pools = {t.task_id: t.pool for t in bag.dags["neo4j_rebuild_rebuild_nightly"].tasks}
    assert pools["rebuild_west"] == "neo4j_west_full"
