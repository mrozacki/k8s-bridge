#!/usr/bin/env bash
# Guards the runnable documentation against the single most common defect
# found in it: a command block that references a file by a path which does
# not resolve from the directory the document tells you to be in.
#
# Every bash block in the documents below is contractually repo-root-relative
# (each document states this in its preamble). This script enforces that
# contract mechanically:
#
#   1. no "../" inside a bash block  — under the root-relative convention any
#      such path escapes the repository, which is exactly the bug that made
#      sections 8-17 of the demo runbook unrunnable;
#   2. every repo-relative path referenced from a bash block exists;
#   3. every ./script.sh invoked from a bash block is executable.
#
# Blocks are read verbatim, so a path only referenced from a comment line is
# deliberately NOT checked — comments routinely point at illustrative or
# historical locations.
set -euo pipefail
cd "$(dirname "$0")/.."

DOCS=(
  experiments/DEMO.md
  docs/tutorial.md
  docs/installation.md
)

python3 - "${DOCS[@]}" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path.cwd()
# A path-shaped token: contains a directory separator and a known extension.
# The lookbehind anchors the match to a token boundary, so an absolute path
# like /tmp/x.yaml is not silently re-read as the relative "tmp/x.yaml".
TOKEN = re.compile(
    r"(?<![A-Za-z0-9._/-])[A-Za-z0-9._][A-Za-z0-9._/-]*\.(?:yaml|yml|sh|json|go|md)\b"
)
URL = re.compile(r"https?://\S+")

failures = []
checked = 0

for name in sys.argv[1:]:
    doc = root / name
    if not doc.exists():
        failures.append(f"{name}: document does not exist")
        continue

    in_block = False
    for lineno, raw in enumerate(doc.read_text().splitlines(), start=1):
        stripped = raw.strip()
        if stripped.startswith("```"):
            in_block = stripped == "```bash"
            continue
        if not in_block or not stripped or stripped.startswith("#"):
            continue

        if "../" in stripped:
            failures.append(
                f"{name}:{lineno}: '../' in a command block — blocks are "
                f"repo-root-relative:\n      {stripped}"
            )

        # URLs carry paths that are not ours to resolve.
        line = URL.sub(" ", stripped)
        for match in TOKEN.finditer(line):
            candidate = match.group(0)
            if candidate.startswith("/") or "/" not in candidate:
                continue
            candidate = candidate.removeprefix("./")
            checked += 1
            target = root / candidate
            if not target.exists():
                failures.append(f"{name}:{lineno}: missing file: {candidate}")
            elif candidate.endswith(".sh") and not target.stat().st_mode & 0o111:
                failures.append(f"{name}:{lineno}: not executable: {candidate}")

if failures:
    print("doc-command check FAILED:\n")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)

print(f"doc-command check OK ({checked} path references resolved)")
PY
