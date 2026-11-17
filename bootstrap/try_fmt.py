"""Print a few readable lines from one `neo4j-backup --json` result, for `just try` (#269).

Usage: neo4j-backup --json <command> ... | python3 bootstrap/try_fmt.py <command>
"""
import json
import sys


def short(ts):
    """2026-11-17T06:42:34.233000+00:00 -> 06:42:34"""
    return (ts or "").split("T")[-1][:8]


def main():
    kind = sys.argv[1]
    raw = sys.stdin.read()
    try:
        doc = json.loads(raw)
    except ValueError:
        print(raw)
        sys.exit(1)
    if not doc.get("ok", True) or doc.get("error"):
        print(json.dumps(doc, indent=2))
        sys.exit(1)
    r = doc.get("result", doc)
    out = []
    if kind == "targets":
        out.append("targets: " + ", ".join(r["targets"]))
    elif kind == "backup":
        for b in r["backups"]:
            out.append(f"{b['alias']:12} {b['taken']:4} backup -> {b['artifact'].rsplit('/', 1)[-1]}")
    elif kind == "restore-points":
        for m in r["members"]:
            for c in m["chains"]:
                out.append(f"{m['member']:12} restorable from {short(c['from'])} to {short(c['to'])}"
                           f" ({c['artifacts']} backups)")
    elif kind == "preview":
        for m in r["plan"]["members"]:
            s = m["seed"]
            what = (f"replay to {short(s['until'])}" if s.get("until")
                    else "seed from its last backup (unchanged since)")
            out.append(f"{m['alias']:12} {what}, then swap the alias from {m['old']}")
    elif kind == "restore":
        for m in r["members"]:
            out.append(f"{m['alias']:12} alias {m['old']} -> {m['newdb']}")
    elif kind == "rollback":
        for m in r["plan"]["members"]:
            out.append(f"{m['alias']:12} rollback target {m['newdb']}")
    print("\n".join("   " + line for line in out))


if __name__ == "__main__":
    main()
