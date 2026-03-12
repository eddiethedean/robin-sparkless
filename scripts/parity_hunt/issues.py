from __future__ import annotations

import inspect
import subprocess
from pathlib import Path
from typing import Optional

from parity_hunt.diff import Comparison
from parity_hunt.scenarios import all_scenarios


REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_gh_create(title: str, body: str, label: Optional[str] = None) -> Optional[str]:
    cmd = ["gh", "issue", "create", "--title", title, "--body", body]
    if label:
        cmd += ["--label", label]
    out = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if out.returncode != 0:
        return None
    return out.stdout.strip()


def _format_details(c: Comparison) -> str:
    # Keep body under gh size limits; include key diffs only.
    parts = []
    if "schema" in c.details:
        parts.append("### Schema diff\n")
        sch = c.details["schema"]
        parts.append(
            "**PySpark:**\n```text\n" + (sch.get("pyspark") or "")[:8000] + "\n```\n"
        )
        parts.append(
            "**Sparkless:**\n```text\n"
            + (sch.get("sparkless") or "")[:8000]
            + "\n```\n"
        )
    if "data" in c.details:
        parts.append("### Data diff\n")
        d = c.details["data"]
        parts.append(
            "**PySpark:**\n```json\n" + str(d.get("pyspark"))[:12000] + "\n```\n"
        )
        parts.append(
            "**Sparkless:**\n```json\n" + str(d.get("sparkless"))[:12000] + "\n```\n"
        )
    if "ui" in c.details:
        parts.append("### UI diff (first section)\n")
        ui = c.details["ui"]
        for k, v in ui.items():
            parts.append(f"#### {k}\n")
            parts.append(
                "**PySpark:**\n```text\n" + (v.get("pyspark") or "")[:4000] + "\n```\n"
            )
            parts.append(
                "**Sparkless:**\n```text\n"
                + (v.get("sparkless") or "")[:4000]
                + "\n```\n"
            )
            break
    if "pyspark_error" in c.details or "sparkless_error" in c.details:
        parts.append("### Error diff\n")
        parts.append(
            "```json\n"
            + str({k: c.details.get(k) for k in ["pyspark_error", "sparkless_error"]})[
                :12000
            ]
            + "\n```\n"
        )
    return "\n".join(parts).strip()


def _format_scenario_code(scenario_id: str) -> str:
    scenario = next((s for s in all_scenarios() if s.id == scenario_id), None)
    if scenario is None:
        return ""
    try:
        src = inspect.getsource(scenario.fn)
    except Exception:
        return ""

    src = src.rstrip("\n")
    # IMPORTANT: do not run `dedent()` on the whole block including `{src}` or we
    # can destroy the indentation inside the function body, producing invalid code.
    return "\n".join(
        [
            "## Repro code (exact scenario)",
            "",
            "The harness runs the same scenario function against **both** backends.",
            "Copy/paste this exact function as the repro:",
            "",
            "```python",
            src,
            "```",
            "",
            "### Run against PySpark",
            "",
            "```python",
            "from pyspark.sql import SparkSession",
            "",
            "spark = SparkSession.builder.master('local[1]').appName('parity_hunt_repro').getOrCreate()",
            f"df = {scenario.fn.__name__}(spark)",
            "df.show()",
            "```",
            "",
            "### Run against Sparkless",
            "",
            "```python",
            "from sparkless.sql import SparkSession",
            "",
            "spark = SparkSession.builder.appName('parity_hunt_repro').getOrCreate()",
            f"df = {scenario.fn.__name__}(spark)",
            "df.show()",
            "```",
        ]
    ).strip()


def create_issue_for_gap(c: Comparison) -> Optional[str]:
    title = f"PySpark parity gap: {c.scenario_id} ({c.summary})"
    body = "\n".join(
        [
            f"**Scenario:** `{c.scenario_id}`",
            "",
            f"**Mismatch summary:** {c.summary}",
            "",
            _format_scenario_code(c.scenario_id),
            "",
            "## Details",
            _format_details(c),
            "",
            "## Minimal repro",
            "The parity hunt harness can reproduce this via:",
            "",
            "```bash",
            f"python scripts/parity_hunt.py --backend both --scenario {c.scenario_id}",
            "```",
        ]
    ).strip()
    # Heuristic label selection
    label = (
        "bug"
        if "error" in c.summary.lower() or "mismatch" in c.summary.lower()
        else "enhancement"
    )
    return _run_gh_create(title, body, label=label)
