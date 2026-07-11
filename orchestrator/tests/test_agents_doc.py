"""#59/#58-P3 doc-drift guard: every `neo4j-backup --json <command>` shown in the agent guide, the
CI docs, and the CI example recipes is a real subcommand. These are point-at / copy-paste artifacts;
if the CLI surface changes and they don't, an agent or an operator runs commands that don't exist.
Also asserts the CI example YAML parses."""

import glob
import os
import re

import yaml

from neo4j_backup_cli.__main__ import build_parser

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Docs + recipes that invoke the CLI. All real invocations use `--json`, which anchors the scan and
# keeps it clear of prose, headings, and the `<command>` template placeholder.
DOCS = ["AGENTS.md", "llms.txt", "CI.md", "README.md",
        "examples/ci/github-actions.yml", "examples/ci/gitlab-ci.yml",
        "examples/ci/forgejo-actions.yml", "examples/ci/README.md"]


def _cli_surface():
    parser = build_parser()
    commands = set()
    for action in parser._subparsers._group_actions:
        for name, sp in action.choices.items():
            commands.add(name)
            if name == "metadata":
                for sub in sp._subparsers._group_actions:
                    commands |= {f"metadata {s}" for s in sub.choices}
    return commands


def _documented_commands(text):
    found = set()
    for cmd, sub in re.findall(r"neo4j-backup\s+--json\s+([a-z]+)(?:\s+([a-z]+))?", text):
        if cmd == "metadata" and sub:
            found.add(f"metadata {sub}")
        elif cmd != "metadata":
            found.add(cmd)
    return found


def test_docs_reference_only_real_commands():
    surface = _cli_surface()
    assert {"backup", "restore", "prune", "metadata export", "metadata restore"} <= surface
    for doc in DOCS:
        documented = _documented_commands(open(os.path.join(REPO, doc)).read())
        unknown = documented - surface
        assert not unknown, f"{doc} references non-existent commands: {sorted(unknown)}"
    # the agent guide exercises the surface; the CI recipes collectively cover backup/verify/prune
    assert len(_documented_commands(open(os.path.join(REPO, "AGENTS.md")).read())) >= 5
    recipe_cmds = set()
    for r in ("examples/ci/github-actions.yml", "examples/ci/gitlab-ci.yml",
              "examples/ci/forgejo-actions.yml"):
        recipe_cmds |= _documented_commands(open(os.path.join(REPO, r)).read())
    assert {"backup", "verify", "prune"} <= recipe_cmds


def test_ci_examples_are_valid_yaml():
    for path in glob.glob(os.path.join(REPO, "examples", "ci", "*.yml")):
        with open(path) as f:
            docs = list(yaml.safe_load_all(f))
        assert docs and docs[0], f"{path} did not parse to a document"
