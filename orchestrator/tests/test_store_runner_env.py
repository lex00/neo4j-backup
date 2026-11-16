"""#105 (second part): secret references, per-store credentials, and neo4j-admin's environment
for a named store."""

import json

import pytest

from neo4j_backup_core import env, ops, secrets
from neo4j_backup_core.clients import BackupRunner
from neo4j_backup_core.policy import StoreConfig


def test_resolve_ref_syntax(monkeypatch):
    monkeypatch.setenv("FIN", "s3cret")
    assert secrets.resolve_ref("env:FIN") == "s3cret"
    assert secrets.resolve_ref("FIN") == "s3cret"  # bare: the configured provider (env)
    with pytest.raises(RuntimeError):
        secrets.resolve_ref("")


def test_key_pair(monkeypatch):
    monkeypatch.setenv("KP", json.dumps({"access_key_id": "AK", "secret_access_key": "SK"}))
    assert secrets.key_pair("env:KP") == {"access_key_id": "AK", "secret_access_key": "SK"}
    monkeypatch.setenv("BAD", "not json")
    with pytest.raises(RuntimeError, match="access_key_id"):
        secrets.key_pair("env:BAD")


def test_named_store_carries_admin_env(monkeypatch):
    monkeypatch.setenv("KP", json.dumps({"access_key_id": "AK", "secret_access_key": "SK",
                                         "session_token": "ST"}))
    st = env.store_from_config(StoreConfig(
        url="s3://fin", region="eu-west-1", endpoint="http://localhost:9000",
        runner_endpoint="http://s3:4566", credentials_ref="env:KP"))
    assert st.admin_env() == {"AWS_REGION": "eu-west-1", "AWS_ENDPOINT_URL_S3": "http://s3:4566",
                              "AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK",
                              "AWS_SESSION_TOKEN": "ST"}
    assert st.credentials["access_key_id"] == "AK"


def test_environment_store_adds_nothing():
    assert env.env_store().admin_env() == {}


def test_credentials_ref_only_for_s3():
    with pytest.raises(RuntimeError, match="s3:// stores only"):
        env.store_from_config(StoreConfig(url="gs://b", credentials_ref="env:X"))


def test_with_env_adds_docker_exec_flags():
    r = BackupRunner(exec_prefix=["docker", "compose", "exec", "-T", "runner"])
    cmd = r.backup_command("db", "s3://fin/p/")
    out = r.with_env(cmd, {"AWS_ACCESS_KEY_ID": "AK"})
    assert out[:5] == ["docker", "compose", "exec", "-e", "AWS_ACCESS_KEY_ID"]
    assert "AK" not in out  # the value never appears in the argv
    assert BackupRunner().with_env(["neo4j-admin"], {"X": "1"}) == ["neo4j-admin"]


def test_ops_pass_the_stores_env_to_neo4j_admin():
    from test_artifacts import MemStore
    store = MemStore()
    store.admin_env = lambda: {"AWS_REGION": "eu-west-1"}
    seen = []

    def run(cmd, env=None):
        seen.append(env)
        if "inspect" not in cmd:
            store.add("g/a/p/x.backup")
        return "neo4j 2026.05.0\n[]"
    ops.run_backup(run, BackupRunner(), store, "p", "g/a/p/", "FULL")
    assert seen and all(e == {"AWS_REGION": "eu-west-1"} for e in seen)


def test_check_stores_reports_each_store(monkeypatch):
    from neo4j_backup_core.policy import Policy
    from test_artifacts import MemStore
    ok, broken = MemStore(), MemStore()

    def boom(*a, **k):
        raise PermissionError("AccessDenied")
    broken.put_text = boom
    p = Policy.model_validate({
        "storage": {"finance": {"url": "s3://fin", "endpoint": "http://x"}},
        "db_groups": [{"id": "billing", "aliases": ["b"], "tier": "gold", "storage": "finance"},
                      {"id": "shop", "aliases": ["s"], "tier": "gold"}],
        "tiers": {"gold": {"full_cron": "0 2 * * *", "diff_cron": "0 * * * *"}}})
    s = env.StoreSet(ok, policy=p)
    s._built["finance"] = broken
    rows = {r["store"]: r for r in env.check_stores(p, s)}
    assert rows["default"]["write"] is True and rows["default"]["groups"] == ["shop"]
    assert rows["finance"]["write"] is False and "AccessDenied" in rows["finance"]["errors"][0]
    assert "servers" in rows["finance"] and "servers_note" in rows["finance"]
    assert not [k for k in ok.objects if k.startswith("_policy_check/")]  # probe cleaned up
