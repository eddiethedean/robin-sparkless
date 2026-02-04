#!/usr/bin/env python3
"""
Generate docs/SIGNATURE_ALIGNMENT_TASKS.md from docs/signature_comparison.json.
Run after compare_signatures.py. Provides a concrete checklist to align
robin-sparkless Python API parameter names and optional args to PySpark.
"""
from __future__ import annotations

import json
from pathlib import Path


def arg_str(args: list[dict]) -> str:
    return ", ".join(
        a["name"] + (f"={a['default']!r}" if a.get("default") is not None else "")
        for a in args
    )


def main() -> int:
    comp_path = Path("docs/signature_comparison.json")
    out_path = Path("docs/SIGNATURE_ALIGNMENT_TASKS.md")
    if not comp_path.exists():
        print(f"Missing {comp_path}. Run compare_signatures.py first.")
        return 1

    with open(comp_path) as f:
        d = json.load(f)

    partial = [x for x in d["functions"] if x.get("classification") == "partial"]
    tasks = []
    for r in partial:
        py_args = r.get("pyspark_args") or []
        robin_args = r.get("robin_args") or []
        py_s = arg_str(py_args)
        robin_s = arg_str(robin_args)
        actions = []
        if len(py_args) != len(robin_args):
            if len(py_args) > len(robin_args):
                extra = [a["name"] for a in py_args[len(robin_args) :]]
                actions.append(f"Add optional: {', '.join(extra)}")
            else:
                actions.append("Param count differs (review PySpark docs)")
        else:
            for pa, ra in zip(py_args, robin_args):
                if pa["name"] != ra["name"]:
                    actions.append(f"{ra['name']} → {pa['name']}")
        tasks.append(
            {
                "name": r["name"],
                "pyspark": py_s,
                "robin": robin_s,
                "actions": actions,
            }
        )

    # Group by action type for checklist
    only_col_to_col = [t for t in tasks if t["actions"] == ["column → col"]]
    add_optional = [t for t in tasks if t["actions"] and "Add optional" in t["actions"][0]]
    param_count_diff = [t for t in tasks if t["actions"] and "Param count" in t["actions"][0]]
    other_renames = [
        t
        for t in tasks
        if t not in only_col_to_col and t not in add_optional and t not in param_count_diff
    ]

    lines = [
        "# Signature alignment tasks: Robin-Sparkless → PySpark",
        "",
        "Checklist derived from [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md) / `signature_comparison.json`. "
        "Goal: make Python parameter names and optional args match PySpark so existing PySpark call sites work unchanged.",
        "",
        "**How to apply:**",
        "- In `src/python/mod.rs`, either (1) rename the `#[pyfunction]` parameter to the PySpark name, or (2) add `#[pyo3(signature = (col, ...))]` and keep the Rust param name.",
        "- For \"Add optional\", add the parameter with the same default as PySpark (check [PySpark SQL API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)).",
        "",
        "---",
        "",
        "## 1. Simple rename: `column` → `col` (single-arg unary)",
        "",
        "These functions only need the first (and only) parameter renamed from `column` to `col` in the Python signature.",
        "",
        "| [ ] | Function |",
        "|-----|----------|",
    ]
    for t in only_col_to_col:
        lines.append(f"| | `{t['name']}` |")
    lines.append("")
    lines.append(f"**Total: {len(only_col_to_col)}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## 2. Add optional parameter(s)",
            "",
            "PySpark has one or more optional parameters that robin-sparkless is missing. Add the param(s) with PySpark's default.",
            "",
            "| [ ] | Function | PySpark signature | Robin today | Add |",
            "|-----|----------|-------------------|-------------|-----|",
        ]
    )
    for t in add_optional:
        add = t["actions"][0].replace("Add optional: ", "") if t["actions"] else ""
        lines.append(f"| | `{t['name']}` | `{t['name']}({t['pyspark']})` | `{t['name']}({t['robin']})` | {add} |")
    lines.append("")
    lines.append(f"**Total: {len(add_optional)}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## 3. Param count / shape differs (review manually)",
            "",
            "PySpark and robin-sparkless have different number of parameters (e.g. variadic vs two args). Check PySpark docs and decide mapping.",
            "",
            "| [ ] | Function | PySpark | Robin |",
            "|-----|----------|---------|-------|",
        ]
    )
    for t in param_count_diff:
        lines.append(f"| | `{t['name']}` | `{t['pyspark']}` | `{t['robin']}` |")
    lines.append("")
    lines.append(f"**Total: {len(param_count_diff)}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## 4. Other renames (multi-param or different names)",
            "",
            "| [ ] | Function | PySpark | Robin | Action |",
            "|-----|----------|---------|-------|--------|",
        ]
    )
    for t in other_renames:
        action = "; ".join(t["actions"])
        lines.append(f"| | `{t['name']}` | `{t['pyspark']}` | `{t['robin']}` | {action} |")
    lines.append("")
    lines.append(f"**Total: {len(other_renames)}**")
    lines.extend(
        [
            "",
            "---",
            "",
            "## Summary",
            "",
            f"- **column → col only:** {len(only_col_to_col)}",
            f"- **Add optional param(s):** {len(add_optional)}",
            f"- **Param count differs:** {len(param_count_diff)}",
            f"- **Other renames:** {len(other_renames)}",
            f"- **Total partial (to align):** {len(tasks)}",
            "",
            "*Regenerate: `python scripts/write_signature_alignment_tasks.py` (after `compare_signatures.py`).*",
            "",
        ]
    )

    out_path.write_text("\n".join(lines))
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
