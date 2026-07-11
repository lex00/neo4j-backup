"""#58 P5 — the MCP server scope wiring. Needs the `[mcp]` extra, so it is skipped where the SDK
isn't installed (e.g. CI's base install); the tool *logic* is covered dependency-free in
test_mcp_tools.py."""

import asyncio

import pytest

pytest.importorskip("mcp")

from neo4j_backup_mcp import server


def _tool_names(mode):
    s = server.build_server(mode)
    return sorted(t.name for t in asyncio.run(s.list_tools()))


def test_read_only_scope_exposes_no_mutations():
    names = _tool_names("read-only")
    assert {"list_targets", "backup_status", "preview_restore", "preview_prune"} <= set(names)
    assert not any(n.startswith("run_") for n in names)  # no mutating tools without read-write


def test_read_write_scope_exposes_guarded_mutations():
    names = set(_tool_names("read-write"))
    assert {"run_backup", "run_verify", "run_aggregate", "run_restore", "run_prune"} <= names
    assert {"list_targets", "backup_status"} <= names  # read tools still present
