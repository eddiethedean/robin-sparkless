#!/usr/bin/env python3
"""
Compare PySpark and robin_sparkless signatures from exported JSON files.
Produces comparison JSON and optional markdown table.
Usage:
  python scripts/compare_signatures.py [--pyspark docs/signatures_pyspark.json] [--robin docs/signatures_robin_sparkless.json] [--output docs/signature_comparison.json] [--md]
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

# PySpark re-exports from typing we skip for function comparison
PYSPARK_TYPING_NAMES = frozenset(
    {
        "Any",
        "Callable",
        "Dict",
        "Iterable",
        "List",
        "Optional",
        "Tuple",
        "Type",
        "Union",
        "ValuesView",
        "AbstractSet",
        "ByteString",
        "Container",
        "ContextManager",
        "Counter",
        "DefaultDict",
        "Deque",
        "FrozenSet",
        "Generator",
        "Generic",
        "Hashable",
        "ItemsView",
        "KeysView",
        "Mapping",
        "MappingView",
        "MutableMapping",
        "MutableSet",
        "Sequence",
        "Set",
        "Sized",
        "TYPE_CHECKING",
        "TypeVar",
        "cast",
        "overload",
        "no_type_check",
        "get_args",
        "get_origin",
        "get_type_hints",
        "ParamSpec",
        "Concatenate",
        "TypedDict",
        "NamedTuple",
        "Protocol",
        "runtime_checkable",
        "SupportsAbs",
        "SupportsBytes",
        "SupportsComplex",
        "SupportsFloat",
        "SupportsIndex",
        "SupportsInt",
        "SupportsRound",
    }
)


def camel_to_snake(name: str) -> str:
    s = re.sub(r"(?<!^)(?<![A-Z])(?=[A-Z])", "_", name)
    return s.lower()


def normalize_name_for_match(name: str, from_pyspark: bool) -> str:
    """Canonical name for matching: prefer snake_case."""
    if from_pyspark and "_" not in name and name[0].islower():
        return camel_to_snake(name)
    return name


def args_signature(args: list[dict]) -> str:
    """String representation of argument list for display."""
    parts = []
    for a in args:
        n = a.get("name", "?")
        d = a.get("default")
        parts.append(n if d is None else f"{n}={d!r}")
    return ", ".join(parts)


def args_key(args: list[dict]) -> tuple[tuple[object, object], ...]:
    """Tuple of (name, default) per arg for comparison."""
    return tuple((a.get("name"), a.get("default")) for a in args)


def classify_function(py_sig: dict | None, robin_sig: dict | None) -> str:
    if py_sig is None and robin_sig is None:
        return "none"
    if py_sig is None:
        return "extra"  # in robin only
    if robin_sig is None:
        return "missing"  # in pyspark only
    py_args = py_sig.get("args") or []
    robin_args = robin_sig.get("args") or []
    # Compare param names and defaults (ignore annotation for exact match)
    py_key = args_key(py_args)
    robin_key = args_key(robin_args)
    if py_key == robin_key:
        return "exact"
    # Check compatible: same set of names and defaults, maybe different order
    py_set = set((a.get("name"), a.get("default")) for a in py_args)
    robin_set = set((a.get("name"), a.get("default")) for a in robin_args)
    if py_set == robin_set:
        return "compatible"
    # Partial: overlap but not identical
    if py_set & robin_set:
        return "partial"
    return "partial"


def load_json(path: Path) -> dict:
    """Load and parse a JSON file; return the root dict."""
    with open(path) as f:
        return json.load(f)


def build_pyspark_function_map(data: dict) -> dict[str, dict]:
    """Map: canonical name -> one pyspark signature (prefer snake_case name)."""
    out = {}
    for item in data.get("functions") or []:
        name = item.get("name")
        if not name or name in PYSPARK_TYPING_NAMES:
            continue
        canonical = normalize_name_for_match(name, True)
        # Prefer snake_case if both exist (e.g. count_distinct over countDistinct)
        if canonical not in out or "_" in name:
            out[canonical] = item
    return out


def build_robin_function_map(data: dict) -> dict[str, dict]:
    """Map: function name -> robin_sparkless signature dict."""
    out = {}
    for item in data.get("functions") or []:
        name = item.get("name")
        if name:
            out[name] = item
    return out


def compare_functions(py_data: dict, robin_data: dict) -> list[dict]:
    py_map = build_pyspark_function_map(py_data)
    robin_map = build_robin_function_map(robin_data)
    all_names = sorted(set(py_map) | set(robin_map))
    results = []
    for name in all_names:
        py_sig = py_map.get(name)
        robin_sig = robin_map.get(name)
        classification = classify_function(py_sig, robin_sig)
        py_args_str = args_signature((py_sig or {}).get("args") or [])
        robin_args_str = args_signature((robin_sig or {}).get("args") or [])
        results.append(
            {
                "name": name,
                "classification": classification,
                "pyspark_signature": f"{name}({py_args_str})" if py_sig else None,
                "robin_signature": f"{name}({robin_args_str})" if robin_sig else None,
                "pyspark_args": (py_sig or {}).get("args"),
                "robin_args": (robin_sig or {}).get("args"),
                "notes": "",
            }
        )
    return results


def compare_class_methods(
    py_data: dict, robin_data: dict, class_name: str
) -> list[dict]:
    py_methods = {
        m["name"]: m for m in (py_data.get("classes") or {}).get(class_name) or []
    }
    robin_methods = {
        m["name"]: m for m in (robin_data.get("classes") or {}).get(class_name) or []
    }
    all_names = sorted(set(py_methods) | set(robin_methods))
    results = []
    for name in all_names:
        py_sig = py_methods.get(name)
        robin_sig = robin_methods.get(name)
        classification = classify_function(py_sig, robin_sig)
        py_args_str = args_signature((py_sig or {}).get("args") or [])
        robin_args_str = args_signature((robin_sig or {}).get("args") or [])
        results.append(
            {
                "class": class_name,
                "name": name,
                "classification": classification,
                "pyspark_signature": f"{name}({py_args_str})" if py_sig else None,
                "robin_signature": f"{name}({robin_args_str})" if robin_sig else None,
                "notes": "",
            }
        )
    return results


def write_gap_analysis_md(out: dict, path: Path) -> None:
    """Write docs/SIGNATURE_GAP_ANALYSIS.md from comparison output."""
    pv = out.get("pyspark_version", "unknown")
    funcs = out.get("functions") or []
    classes = out.get("classes") or {}
    summary = out.get("summary") or {}

    lines = [
        "# PySpark vs Robin-Sparkless: Signature Gap Analysis",
        "",
        "This document compares **signatures** (parameters, types, defaults) of the public Python API of robin-sparkless with **PySpark** to guide alignment.",
        "",
        "## Method",
        "",
        "- **PySpark signatures**: Obtained by introspecting an installed PySpark (`inspect.signature`, optional type hints) for `pyspark.sql.functions`, `SparkSession`, `SparkSession.builder`, `DataFrame`, `GroupedData`, `Column`, `DataFrameStat`, and `DataFrameNa`.",
        "- **Robin-sparkless signatures**: Same approach on the built `robin_sparkless` module (requires `maturin develop --features pyo3`).",
        f"- **PySpark version used**: {pv}",
        "- **Cross-check**: PySpark [official API docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) can be used to fill or correct parameter names and defaults where introspection is incomplete.",
        "",
        "## Summary",
        "",
        "### Functions (pyspark.sql.functions)",
        "",
        "| Classification | Count | Description |",
        "|----------------|-------|-------------|",
        "| exact | "
        + str(summary.get("functions", {}).get("exact", 0))
        + " | Same parameter names, order, and defaults |",
        "| compatible | "
        + str(summary.get("functions", {}).get("compatible", 0))
        + " | Same params/defaults; types may differ |",
        "| partial | "
        + str(summary.get("functions", {}).get("partial", 0))
        + " | Different param names or counts (e.g. `column` vs `col`) |",
        "| missing | "
        + str(summary.get("functions", {}).get("missing", 0))
        + " | In PySpark but not in robin-sparkless |",
        "| extra | "
        + str(summary.get("functions", {}).get("extra", 0))
        + " | In robin-sparkless but not in PySpark (extensions) |",
        "",
        f"- **PySpark function names (excluding typing re-exports):** {summary.get('function_total_pyspark', 0)}",
        f"- **Robin-sparkless function names:** {summary.get('function_total_robin', 0)}",
        "",
        "### Class methods",
        "",
        "| Class | Exact | Partial | Missing | Extra |",
        "|-------|-------|---------|---------|-------|",
    ]
    for cls_name in [
        "SparkSession",
        "SparkSessionBuilder",
        "DataFrame",
        "GroupedData",
        "Column",
        "DataFrameStat",
        "DataFrameNa",
    ]:
        methods = classes.get(cls_name) or []
        exact = sum(1 for m in methods if m.get("classification") == "exact")
        partial = sum(1 for m in methods if m.get("classification") == "partial")
        missing = sum(1 for m in methods if m.get("classification") == "missing")
        extra = sum(1 for m in methods if m.get("classification") == "extra")
        lines.append(f"| {cls_name} | {exact} | {partial} | {missing} | {extra} |")
    lines.extend(
        [
            "",
            "---",
            "",
            "## Function details (sample)",
            "",
            "### Exact match (same signature)",
            "",
        ]
    )
    exact_funcs = [f for f in funcs if f.get("classification") == "exact"]
    for r in exact_funcs[:30]:
        lines.append(f"- `{r.get('pyspark_signature', r.get('name', ''))}`")
    if len(exact_funcs) > 30:
        lines.append(f"- ... and {len(exact_funcs) - 30} more")
    lines.extend(
        [
            "",
            "### Partial (param name or count difference)",
            "",
            "Aligning parameter names to PySpark improves drop-in compatibility. Examples:",
            "",
            "| PySpark | Robin |",
            "|---------|-------|",
        ]
    )
    partial_funcs = [f for f in funcs if f.get("classification") == "partial"]
    for r in partial_funcs[:25]:
        py_sig = r.get("pyspark_signature") or ""
        robin_sig = r.get("robin_signature") or ""
        lines.append(f"| `{py_sig}` | `{robin_sig}` |")
    if len(partial_funcs) > 25:
        lines.append(
            f"| *... and {len(partial_funcs) - 25} more (see signature_comparison.json)* | |"
        )
    lines.extend(
        [
            "",
            "### Missing (in PySpark, not in robin-sparkless)",
            "",
            "Functions present in PySpark but not implemented in robin-sparkless (first 40):",
            "",
        ]
    )
    missing_funcs = [f for f in funcs if f.get("classification") == "missing"]
    for r in missing_funcs[:40]:
        lines.append(f"- `{r.get('pyspark_signature', r.get('name', ''))}`")
    if len(missing_funcs) > 40:
        lines.append(
            f"- ... and {len(missing_funcs) - 40} more (see `signature_comparison.json`)"
        )
    lines.extend(
        [
            "",
            "### Extra (in robin-sparkless only)",
            "",
            "Robin-sparkless extensions (e.g. for Sparkless backend):",
            "",
        ]
    )
    extra_funcs = [f for f in funcs if f.get("classification") == "extra"]
    for r in extra_funcs:
        lines.append(f"- `{r.get('robin_signature', r.get('name', ''))}`")
    lines.extend(
        [
            "",
            "---",
            "",
            "## Recommendations",
            "",
            "1. **Parameter names**: Where classification is **partial**, consider aliasing or renaming parameters to match PySpark (e.g. `column` → `col`, `n` → `months` for `add_months`) so that call sites using keyword arguments match.",
            "2. **Defaults**: Add any missing optional parameters with PySpark defaults (e.g. `format=None`) so that existing PySpark code passes unchanged.",
            "3. **Missing functions**: Prioritize implementing high-use PySpark functions that are **missing**; the full list is in `signature_comparison.json`.",
            "4. **Extra APIs**: Keep robin-only APIs (e.g. `execute_plan`, `create_dataframe_from_rows`) for backend use; document them as extensions.",
            "5. **Class methods**: Align DataFrame, SparkSession, GroupedData, and Column method signatures (parameter names and order) to PySpark where practical.",
            "",
            "---",
            "",
            "*Generated from `scripts/compare_signatures.py`. Regenerate with:*",
            "",
            "```bash",
            "python scripts/export_pyspark_signatures.py --output docs/signatures_pyspark.json",
            ". .venv/bin/activate && python scripts/export_robin_signatures.py --output docs/signatures_robin_sparkless.json",
            "python scripts/compare_signatures.py --output docs/signature_comparison.json --write-md docs/SIGNATURE_GAP_ANALYSIS.md",
            "```",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare PySpark and robin_sparkless signatures"
    )
    parser.add_argument(
        "--pyspark",
        default="docs/signatures_pyspark.json",
        help="PySpark signatures JSON",
    )
    parser.add_argument(
        "--robin",
        default="docs/signatures_robin_sparkless.json",
        help="Robin signatures JSON",
    )
    parser.add_argument(
        "--output",
        default="docs/signature_comparison.json",
        help="Output comparison JSON",
    )
    parser.add_argument(
        "--md", action="store_true", help="Print markdown summary to stdout"
    )
    parser.add_argument(
        "--write-md",
        metavar="PATH",
        help="Write full SIGNATURE_GAP_ANALYSIS.md to PATH",
    )
    args = parser.parse_args()

    py_path = Path(args.pyspark)
    robin_path = Path(args.robin)
    if not py_path.exists():
        print(f"Missing: {py_path}")
        return 1
    if not robin_path.exists():
        print(f"Missing: {robin_path}")
        return 1

    py_data = load_json(py_path)
    robin_data = load_json(robin_path)

    func_comparison = compare_functions(py_data, robin_data)
    classes = [
        "SparkSession",
        "SparkSessionBuilder",
        "DataFrame",
        "GroupedData",
        "Column",
        "DataFrameStat",
        "DataFrameNa",
    ]
    class_comparisons = {
        c: compare_class_methods(py_data, robin_data, c) for c in classes
    }

    counts = {"exact": 0, "compatible": 0, "partial": 0, "missing": 0, "extra": 0}
    for r in func_comparison:
        c = r["classification"]
        if c in counts:
            counts[c] += 1

    out = {
        "pyspark_version": py_data.get("pyspark_version"),
        "functions": func_comparison,
        "classes": class_comparisons,
        "summary": {
            "functions": counts,
            "function_total_pyspark": len(build_pyspark_function_map(py_data)),
            "function_total_robin": len(build_robin_function_map(robin_data)),
        },
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {args.output}")

    if args.write_md:
        write_gap_analysis_md(out, Path(args.write_md))
        print(f"Wrote {args.write_md}")

    if args.md:
        print("\n## Function signature comparison summary\n")
        print("| Classification | Count |")
        print("|----------------|-------|")
        for k, v in counts.items():
            print(f"| {k} | {v} |")
        summary = out["summary"]
        assert isinstance(summary, dict)
        print(
            f"\nPySpark functions (excluding typing): {summary['function_total_pyspark']}"
        )
        print(f"Robin-sparkless functions: {summary['function_total_robin']}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
