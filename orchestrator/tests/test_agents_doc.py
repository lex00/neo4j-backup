"""#59 doc-drift guard: every `neo4j-backup <command>` shown in AGENTS.md / llms.txt is a real
subcommand. These are the agent-facing point-at artifacts; if the CLI surface changes and the docs
don't, an agent gets told to run commands that don't exist. Parse the real argparse surface and
assert the docs only reference commands that exist."""

import os
import re

from neo4j_backup_cli.__main__ import build_parser

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _cli_surface():
    """Top-level subcommands plus `metadata <sub>` pairs, straight from the parser."""
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
    """Every `neo4j-backup [flags] <command> [sub]` invocation mentioned in a doc — skipping flags
    and stopping at placeholders like `<group>` (so group args aren't mistaken for commands)."""
    found = set()
    text = re.sub(r"```.*?```", "", text, flags=re.S)   # drop fenced blocks (odd backticks desync pairing)
    for span in re.findall(r"`([^`]+)`", text):         # inline-code spans only, not prose/headings
        if not span.startswith("neo4j-backup"):
            continue
        cmd = sub = None
        for tok in span.split()[1:]:
            if tok.startswith("-"):                    # a flag — skip
                continue
            if not re.fullmatch(r"[a-z]+", tok):       # placeholder / value — end of command
                break
            if cmd is None:
                cmd = tok
                if cmd != "metadata":
                    break
            else:
                sub = tok
                break
        if cmd == "metadata" and sub:
            found.add(f"metadata {sub}")
        elif cmd and cmd != "metadata":
            found.add(cmd)
    return found


def test_docs_reference_only_real_commands():
    surface = _cli_surface()
    assert {"backup", "restore", "metadata export", "metadata restore"} <= surface  # introspection sane
    for doc in ("AGENTS.md", "llms.txt"):
        documented = _documented_commands(open(os.path.join(REPO, doc)).read())
        unknown = documented - surface
        assert not unknown, f"{doc} references non-existent commands: {sorted(unknown)}"
    # AGENTS.md should actually exercise the surface, not just mention one command
    assert len(_documented_commands(open(os.path.join(REPO, 'AGENTS.md')).read())) >= 5
