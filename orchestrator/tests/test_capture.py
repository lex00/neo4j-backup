"""#118: run_admin returns neo4j-admin's output in every adapter (capture.py)."""

import base64
import io
import json
import subprocess

import pytest

from neo4j_backup_core import capture


def test_run_captured_streams_and_returns():
    sink = io.StringIO()
    out = capture.run_captured(["sh", "-c", "echo one; echo two >&2"], stream=sink)
    assert out.splitlines() == ["one", "two"] and sink.getvalue() == out


def test_run_captured_raises_with_output():
    with pytest.raises(subprocess.CalledProcessError) as e:
        capture.run_captured(["sh", "-c", "echo bad; exit 3"], stream=io.StringIO())
    assert e.value.returncode == 3 and "bad" in e.value.output


def test_tee_to_file_keeps_exit_code(tmp_path):
    path = str(tmp_path / "out")
    proc = subprocess.run(capture.tee_to_file(["sh", "-c", "echo hi; exit 5"], path),
                          capture_output=True, text=True)
    assert proc.returncode == 5 and open(path).read() == "hi\n" and proc.stdout == "hi\n"


def test_decoders():
    b64 = base64.b64encode(b"neo4j 2026.05.0\n[]").decode()
    assert capture.decode_xcom({"output_b64": b64}) == "neo4j 2026.05.0\n[]"
    assert capture.decode_xcom(json.dumps({"output_b64": b64})) == "neo4j 2026.05.0\n[]"
    assert capture.decode_pipes([{"other": 1}, {capture.PIPES_MARK: b64}]) == "neo4j 2026.05.0\n[]"
    assert capture.decode_pipes([]) == ""


def test_pipes_wrapper_emits_a_custom_message():
    proc = subprocess.run(capture.wrap_for_pipes(["sh", "-c", "echo hello; exit 2"]),
                          capture_output=True, text=True)
    assert proc.returncode == 2
    msg = json.loads(proc.stdout.splitlines()[-1])
    assert msg["method"] == "report_custom_message"
    assert capture.decode_pipes([msg["params"]["payload"]]) == "hello\n"


def test_xcom_wrapper_writes_the_output(tmp_path, monkeypatch):
    monkeypatch.setattr(capture, "XCOM_FILE", str(tmp_path / "xcom" / "return.json"))
    proc = subprocess.run(capture.wrap_for_xcom(["sh", "-c", "echo hello"]),
                          capture_output=True, text=True)
    assert proc.returncode == 0
    assert capture.decode_xcom(open(capture.XCOM_FILE).read()) == "hello\n"
