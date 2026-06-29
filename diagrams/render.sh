#!/usr/bin/env bash
# Render every *.dot in this directory to SVG. Requires graphviz (the `dot` binary).
set -euo pipefail
cd "$(dirname "$0")"

if ! command -v dot >/dev/null 2>&1; then
  echo "!! graphviz 'dot' not found. Install it (e.g. 'brew install graphviz')." >&2
  exit 1
fi

shopt -s nullglob
rendered=0
for f in *.dot; do
  out="${f%.dot}.svg"
  dot -Tsvg "$f" -o "$out"
  echo "rendered $out"
  rendered=$((rendered + 1))
done
[ "$rendered" -gt 0 ] || { echo "no .dot files found" >&2; exit 1; }
echo ">> $rendered diagram(s) rendered to SVG"
