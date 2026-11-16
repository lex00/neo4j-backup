"""Shared by the rebuild smokes (#108): a policy with one CSV rebuild group on its own tier."""

import os
import tempfile

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GROUP, ALIAS, TIER = "rbsmoke", "rb-smoke", "rbtier"
NODES = "id:ID,name,:LABEL\n1,a,Item\n2,b,Item\n3,c,Item\n"


def policy_file() -> str:
    raw = yaml.safe_load(open(os.path.join(REPO, "policies", "demo.yaml")))
    raw["db_groups"].append({"id": GROUP, "aliases": [ALIAS], "tier": TIER, "load": {
        "mode": "rebuild", "writes": "read-only",
        "build": {"kind": "import", "input_type": "csv", "args": ["--nodes=nodes.csv"]},
        "checks": [{"query": "MATCH (i:Item) RETURN count(i)", "expect": "== 3"}]}})
    raw["tiers"][TIER] = {"rebuild_cron": "0 1 * * *"}
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
    yaml.safe_dump(raw, f)
    f.close()
    return f.name


def put_source(store) -> None:
    store.put_text(f"{GROUP}/_source/nodes.csv", NODES)


def drop_source(store) -> None:
    store.delete_keys([f"{GROUP}/_source/nodes.csv"])


def alias_target(neo) -> str | None:
    return neo.alias_target(ALIAS)
