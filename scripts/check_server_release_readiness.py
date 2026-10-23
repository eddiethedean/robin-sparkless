#!/usr/bin/env python3
"""Require evidence for the native HTTP/launcher surface shipped in 4.16.

Upstream emulator/SDK integration and experimental Connect (V01–V04) are
separate certifications and do not block publication of the HTTP release.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REQUIRED_GATES = [*(f"V{number:02}" for number in range(5, 15)), "V29"]
VALIDATION_FILE = Path(__file__).resolve().parents[1] / "docs/plans/sparkless-server/VALIDATION.md"
GATE_PATTERN = re.compile(r"^\|\s*(V\d+)\s*\|\s*(pending|passed|failed|blocked)\s*\|", re.MULTILINE)


def main() -> int:
    document = VALIDATION_FILE.read_text(encoding="utf-8")
    section_marker = "## 4.16 release publication gate"
    if section_marker not in document:
        print(f"missing required release gate section in {VALIDATION_FILE}", file=sys.stderr)
        return 1
    gate_section = document.split(section_marker, maxsplit=1)[1].split("\n## ", maxsplit=1)[0]
    gate_rows = GATE_PATTERN.findall(gate_section)
    statuses = dict(gate_rows)
    duplicate_gates = sorted(
        gate for gate in REQUIRED_GATES if sum(row_gate == gate for row_gate, _ in gate_rows) != 1
    )
    blocked = [gate for gate in REQUIRED_GATES if statuses.get(gate) == "blocked"]
    incomplete = [gate for gate in REQUIRED_GATES if statuses.get(gate) != "passed"]
    if incomplete or duplicate_gates:
        reasons = []
        if blocked:
            reasons.append("unresolved prerequisites block: " + ", ".join(blocked))
        awaiting_evidence = [gate for gate in incomplete if gate not in blocked]
        if awaiting_evidence:
            reasons.append("passing evidence is missing for: " + ", ".join(awaiting_evidence))
        if duplicate_gates:
            reasons.append("required gates must appear exactly once: " + ", ".join(duplicate_gates))
        print(
            "release publication is blocked; " + "; ".join(reasons),
            file=sys.stderr,
        )
        return 1
    print("required native Sparkless Server 4.16 release gates have passing evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
