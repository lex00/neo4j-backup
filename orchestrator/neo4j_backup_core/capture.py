"""Capture a neo4j-admin command's output while still streaming it to the logs (#118).

Every adapter's `run_admin(cmd)` returns the command's combined stdout/stderr as text. A local
subprocess is teed line by line. A k8s pod can't hand its stdout back directly, so the command is
wrapped in bash: the output is teed to a file in the pod, then base64-encoded into the channel the
orchestrator reads back after the pod ends (an Airflow XCom file, or a Dagster Pipes custom message
on stdout). The wrapper keeps neo4j-admin's exit code.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
from typing import IO

XCOM_FILE = "/airflow/xcom/return.json"  # read back by KubernetesPodOperator(do_xcom_push=True)
_OUT = "/tmp/neo4j-backup-admin.out"


def run_captured(cmd: list, env: dict | None = None, stream: IO[str] | None = None) -> str:
    """Run `cmd`, copy each output line to `stream` (default stdout), return all of it. Raises
    CalledProcessError (with `.output`) on a non-zero exit."""
    stream = stream or sys.stdout
    proc = subprocess.Popen(cmd, env={**os.environ, **(env or {})}, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, bufsize=1)
    lines: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        stream.write(line)
        stream.flush()
        lines.append(line)
    rc = proc.wait()
    out = "".join(lines)
    if rc:
        raise subprocess.CalledProcessError(rc, cmd, output=out)
    return out


def tee_to_file(cmd: list, path: str) -> list:
    """`cmd` wrapped to copy its output into `path` on the machine running the wrapper, keeping the
    exit code. For launchers that stream a child's output but don't return it (Dagster's
    PipesSubprocessClient): wrap the whole argv, exec prefix included, so `path` is local."""
    return ["bash", "-c", 'set -o pipefail; "$@" 2>&1 | tee "$0"', path, *cmd]


def _wrap(cmd: list, emit: str) -> list:
    # `base64 < file | tr -d '\n'` works with GNU and BSD base64 alike
    encode = "b64=\"$(base64 < " + _OUT + " | tr -d '\\n')\""
    script = (f'set -o pipefail; "$@" 2>&1 | tee {_OUT}; rc=$?; {encode}; {emit}; exit $rc')
    return ["bash", "-c", script, "neo4j-admin-capture", *cmd]


def wrap_for_xcom(cmd: list) -> list:
    """`cmd` wrapped so its output lands in the KPO XCom file as {"output_b64": ...}."""
    return _wrap(cmd, f'mkdir -p {os.path.dirname(XCOM_FILE)}; '
                      f'printf \'{{"output_b64":"%s"}}\' "$b64" > {XCOM_FILE}')


def decode_xcom(value) -> str:
    if isinstance(value, str):
        value = json.loads(value)
    return base64.b64decode((value or {}).get("output_b64", "")).decode(errors="replace")


PIPES_MARK = "neo4j_backup_admin_output"


def wrap_for_pipes(cmd: list) -> list:
    """`cmd` wrapped so its output is reported as a Dagster Pipes custom message on stdout, which
    PipesK8sClient's pod-log reader parses."""
    msg = ('{"__dagster_pipes_version":"0.1","method":"report_custom_message",'
           '"params":{"payload":{"' + PIPES_MARK + '":"%s"}}}\\n')
    # neo4j-admin's last line may lack a newline; the message must be a line of its own, since
    # the pod-log reader only parses a line that is entirely one JSON message
    return _wrap(cmd, f"echo; printf '{msg}' \"$b64\"")


def decode_pipes(messages: list) -> str:
    for m in reversed(messages or []):
        if isinstance(m, dict) and PIPES_MARK in m:
            return base64.b64decode(m[PIPES_MARK]).decode(errors="replace")
    return ""
