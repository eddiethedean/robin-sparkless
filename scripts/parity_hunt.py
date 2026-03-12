#!/usr/bin/env python3
"""
Parity hunt: run the same PySpark-idiomatic scenarios against PySpark and Sparkless,
diff results (data/schema/ui/errors), and optionally file GitHub issues for new gaps.

Usage:
  python scripts/parity_hunt.py --backend both --report-dir tmp/parity_hunt
  python scripts/parity_hunt.py --backend both --create-issues --max-issues 10
"""

from __future__ import annotations

# ruff: noqa: E402

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from parity_hunt.dedupe import DedupeIndex
from parity_hunt.diff import diff_scenario_results
from parity_hunt.issues import create_issue_for_gap
from parity_hunt.runner import Backend, run_scenarios
from parity_hunt.scenarios import all_scenarios


def _is_pyspark_env_failure(c) -> bool:
    # Avoid filing parity issues when PySpark is failing due to local environment.
    # Common: Spark worker Python version mismatch.
    msg = ""
    if isinstance(getattr(c, "details", None), dict):
        perr = c.details.get("pyspark_error") or {}
        if isinstance(perr, dict):
            msg = str(perr.get("message") or "")
    return "PYTHON_VERSION_MISMATCH" in msg


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--backend",
        choices=["pyspark", "sparkless", "both"],
        default="both",
        help="Which backends to run. 'both' compares PySpark vs Sparkless.",
    )
    p.add_argument(
        "--report-dir",
        default="tmp/parity_hunt",
        help="Directory for JSON/markdown report outputs.",
    )
    p.add_argument(
        "--create-issues",
        action="store_true",
        help="If set, file new parity gaps as GitHub issues via `gh`.",
    )
    p.add_argument(
        "--max-issues",
        type=int,
        default=10,
        help="Safety cap for number of GitHub issues created in one run.",
    )
    p.add_argument(
        "--scenario",
        action="append",
        default=[],
        help="Run only matching scenario ids (can be repeated).",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    scenarios = all_scenarios()
    if args.scenario:
        wanted = set(args.scenario)
        scenarios = [s for s in scenarios if s.id in wanted]

    backends: list[Backend]
    if args.backend == "both":
        backends = [Backend.PYSPARK, Backend.SPARKLESS]
    elif args.backend == "pyspark":
        backends = [Backend.PYSPARK]
    else:
        backends = [Backend.SPARKLESS]

    results = run_scenarios(scenarios, backends=backends)

    # Compare only when both are present.
    comparisons = []
    for sid, per_backend in results.by_scenario.items():
        if (
            Backend.PYSPARK.value not in per_backend
            or Backend.SPARKLESS.value not in per_backend
        ):
            continue
        pys = per_backend[Backend.PYSPARK.value]
        spk = per_backend[Backend.SPARKLESS.value]
        comparisons.append(diff_scenario_results(sid, pys, spk))

    json_path = report_dir / "parity_hunt_report.json"
    json_path.write_text(
        json.dumps(
            {
                "run": asdict(results.meta),
                "scenarios": [
                    {"id": s.id, "title": s.title, "tags": s.tags} for s in scenarios
                ],
                "results": results.to_jsonable(),
                "comparisons": [c.to_jsonable() for c in comparisons],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )

    md_lines = [
        "## Parity hunt report",
        "",
        f"- **Scenarios run**: {len(scenarios)}",
        f"- **Compared**: {len(comparisons)}",
        "",
        "### Mismatches",
        "",
    ]
    mismatches = [c for c in comparisons if c.is_mismatch]
    if not mismatches:
        md_lines.append("- None")
    else:
        for c in mismatches:
            md_lines.append(f"- **{c.scenario_id}**: {c.summary}")
    (report_dir / "parity_hunt_report.md").write_text("\n".join(md_lines) + "\n")

    if args.create_issues and mismatches:
        idx = DedupeIndex.load()
        created = 0
        for c in mismatches:
            if created >= args.max_issues:
                break
            if _is_pyspark_env_failure(c):
                continue
            if idx.is_known_gap(c):
                continue
            issue_url = create_issue_for_gap(c)
            if issue_url:
                idx.record_created(c, issue_url)
                created += 1
        idx.save()
        print(f"Created issues: {created}")

    print(f"Wrote report: {json_path}")
    print(f"Wrote report: {report_dir / 'parity_hunt_report.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
