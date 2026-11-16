"""#246: failure alerts to a webhook."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from neo4j_backup_core import alerts


def test_message_is_one_line_for_chat():
    m = alerts.message("Dagster job", "status_job", "2 target(s) past rpo_minutes: a, b\nTrace",
                       "http://dagster/runs/1")
    assert m["text"] == ("neo4j-backup: Dagster job status_job failed: 2 target(s) past "
                         "rpo_minutes: a, b (http://dagster/runs/1)")


def test_send_posts_json_and_never_raises(monkeypatch):
    got = []

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            got.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.handle_request, daemon=True).start()
    monkeypatch.setenv("NEO4J_BACKUP_ALERT_WEBHOOK", f"http://127.0.0.1:{srv.server_port}/hook")
    assert alerts.send({"text": "x"}) is True and got == [{"text": "x"}]
    monkeypatch.setenv("NEO4J_BACKUP_ALERT_WEBHOOK", "http://127.0.0.1:9/nothing")
    assert alerts.send({"text": "x"}) is False
    monkeypatch.delenv("NEO4J_BACKUP_ALERT_WEBHOOK")
    assert alerts.send({"text": "x"}) is False


def test_dagster_sensor_is_defined_and_off_without_a_webhook():
    dg = pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D
    s = next(x for x in D.defs.sensors if x.name == "alert_on_failure")
    assert s.default_status == dg.DefaultSensorStatus.STOPPED


def test_airflow_callback_builds_the_message(monkeypatch):
    import types
    from neo4j_backup_airflow import alerts as af
    sent = []
    monkeypatch.setattr(alerts, "send", lambda body, url=None: sent.append(body) or True)
    ti = types.SimpleNamespace(dag_id="neo4j_status", task_id="check", log_url="http://af/log")
    af.on_failure({"task_instance": ti, "exception": RuntimeError("1 target(s) past rpo")})
    assert sent[0]["text"] == ("neo4j-backup: Airflow DAG neo4j_status.check failed: 1 target(s) "
                               "past rpo (http://af/log)")


def test_dagster_sensor_posts_a_failed_run(monkeypatch):
    """#246: a real failed run, evaluated by the sensor, reaches the webhook."""
    dg = pytest.importorskip("dagster")
    from neo4j_backup_dagster import definitions as D
    posted = []
    monkeypatch.setattr(alerts, "send", lambda body, url=None: posted.append(body) or True)

    @dg.op
    def boom():
        raise RuntimeError("2 target(s) past rpo_minutes: demo/acme-orders")

    @dg.job
    def status_job_like():
        boom()

    with dg.instance_for_test() as inst:
        res = status_job_like.execute_in_process(instance=inst, raise_on_error=False)
        assert not res.success
        failure = next(e for e in res.all_events if e.event_type_value == "PIPELINE_FAILURE")
        ctx = dg.build_run_status_sensor_context(
            sensor_name="alert_on_failure", dagster_instance=inst,
            dagster_run=res.dagster_run, dagster_event=failure).for_run_failure()
        D.alert_on_failure(ctx)
    assert posted and posted[0]["name"] == "status_job_like", posted
    assert posted[0]["text"].startswith("neo4j-backup: Dagster job status_job_like failed")
