#!/usr/bin/env python3
"""
Gap analysis: PySpark (from repo extraction) vs robin-sparkless.
Consumes pyspark_api_from_repo.json and robin signatures (robin_api_from_source.json
or signatures_robin_sparkless.json). Produces GAP_ANALYSIS_PYSPARK_REPO.json and .md.

Usage:
  python scripts/gap_analysis_pyspark_repo.py
  python scripts/gap_analysis_pyspark_repo.py --pyspark docs/pyspark_api_from_repo.json \
    --robin docs/signatures_robin_sparkless.json --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


# PySpark typing re-exports to skip
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
        "TYPE_CHECKING",
        "overload",
        "cast",
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
        "TypeVar",
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
        "warnings",
        "attr_name",
        "attr_value",
        "Functions",
    }
)


def camel_to_snake(name: str) -> str:
    s = re.sub(r"(?<!^)(?<![A-Z])(?=[A-Z])", "_", name)
    return s.lower()


def normalize_name_for_match(name: str, from_pyspark: bool) -> str:
    """Canonical name for matching: prefer snake_case."""
    if from_pyspark and "_" not in name and len(name) > 0 and name[0].islower():
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


def _normalize_default(d: object) -> object:
    """Normalize default for comparison (0/'0', None/'None', True/'True' etc.)."""
    if d is None:
        return None
    if d in ("None", "None)"):
        return None
    if d in (0, "0", 0.0):
        return 0
    if d in (1, "1", 1.0):
        return 1
    if d in (True, "True", "true"):
        return True
    if d in (False, "False", "false"):
        return False
    return d


def args_key(args: list[dict]) -> tuple[tuple[object, object], ...]:
    """Tuple of (name, default) per arg for comparison."""
    return tuple((a.get("name"), a.get("default")) for a in args)


def args_key_normalized(args: list[dict]) -> tuple[tuple[object, object], ...]:
    """Tuple of (name, normalized_default) for comparison (treats 0/'0', None/'None' as same)."""
    return tuple((a.get("name"), _normalize_default(a.get("default"))) for a in args)


def classify_item(py_sig: dict | None, robin_sig: dict | None) -> str:
    """Classify as exact, compatible, partial, missing, extra, or alias."""
    if py_sig is None and robin_sig is None:
        return "none"
    if py_sig is None:
        return "extra"
    if robin_sig is None:
        return "missing"
    py_args = py_sig.get("args") or []
    robin_args = robin_sig.get("args") or []
    py_key = args_key(py_args)
    robin_key = args_key(robin_args)
    if py_key == robin_key:
        return "exact"
    # Normalized comparison: 0/'0', None/'None', True/'True' etc. treated as same
    py_key_norm = args_key_normalized(py_args)
    robin_key_norm = args_key_normalized(robin_args)
    if py_key_norm == robin_key_norm:
        return "exact"
    py_set = set((a.get("name"), _normalize_default(a.get("default"))) for a in py_args)
    robin_set = set(
        (a.get("name"), _normalize_default(a.get("default"))) for a in robin_args
    )
    if py_set == robin_set:
        return "compatible"
    return "partial"


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def build_pyspark_function_map(data: dict) -> dict[str, dict]:
    """From pyspark_api_from_repo.json: functions list -> canonical name -> sig."""
    out = {}
    for item in data.get("functions") or []:
        name = item.get("name")
        if not name or name in PYSPARK_TYPING_NAMES:
            continue
        canonical = normalize_name_for_match(name, True)
        if canonical not in out or "_" in name:
            out[canonical] = item
    return out


def build_robin_function_map(data: dict) -> dict[str, dict]:
    """From signatures_robin_sparkless.json or robin_api_from_source.json."""
    out = {}
    for item in data.get("functions") or []:
        name = item.get("name")
        if name:
            out[name] = item
            # Also add snake_case if camelCase
            canonical = normalize_name_for_match(name, False)
            if canonical != name and canonical not in out:
                out[canonical] = item
    return out


def build_pyspark_class_map(data: dict, class_name: str) -> dict[str, dict]:
    """From pyspark_api_from_repo.json: DataFrame, Column, etc. are top-level keys."""
    items = data.get(class_name) or []
    out = {}
    for m in items:
        name = m.get("name")
        if name:
            canonical = normalize_name_for_match(name, True)
            if canonical not in out or "_" in name:
                out[canonical] = m
    return out


def build_robin_class_map(data: dict, class_name: str) -> dict[str, dict]:
    """From signatures_robin_sparkless.json: classes[class_name]."""
    items = (data.get("classes") or {}).get(class_name) or []
    out = {}
    for m in items:
        name = m.get("name")
        if name:
            out[name] = m
            canonical = normalize_name_for_match(name, False)
            if canonical != name and canonical not in out:
                out[canonical] = m
    return out


def load_annotations(annotations_path: Path) -> dict[str, list[str]]:
    """Load gap_annotations.json (stub, diverges, deferred lists)."""
    if not annotations_path.exists():
        return {}
    data = load_json(annotations_path)
    return {k: v for k, v in data.items() if k != "_comment" and isinstance(v, list)}


def _norm_for_annotation(name: str) -> str:
    """Normalize name for annotation matching (lowercase, no underscores)."""
    return name.lower().replace("_", "")


def apply_semantic_annotations(
    items: list[dict], annotations: dict[str, list[str]], name_key: str = "name"
) -> None:
    """Add semantic_flags to each item based on annotations. Modifies in place."""
    stub_set = {_norm_for_annotation(s) for s in annotations.get("stub", [])}
    diverges_set = {_norm_for_annotation(s) for s in annotations.get("diverges", [])}
    deferred_set = {_norm_for_annotation(s) for s in annotations.get("deferred", [])}
    for item in items:
        name = item.get(name_key) or ""
        norm = _norm_for_annotation(name)
        flags = []
        if norm in stub_set:
            flags.append("stub")
        if norm in diverges_set:
            flags.append("diverges")
        if norm in deferred_set:
            flags.append("deferred")
        if flags:
            item["semantic_flags"] = flags


def compare_functions(py_data: dict, robin_data: dict) -> list[dict]:
    py_map = build_pyspark_function_map(py_data)
    robin_map = build_robin_function_map(robin_data)
    all_names = sorted(set(py_map) | set(robin_map))
    results = []
    for name in all_names:
        py_sig = py_map.get(name)
        robin_sig = robin_map.get(name)
        classification = classify_item(py_sig, robin_sig)
        py_args = (py_sig or {}).get("args") or []
        robin_args = (robin_sig or {}).get("args") or []
        results.append(
            {
                "name": name,
                "classification": classification,
                "pyspark_signature": f"{name}({args_signature(py_args)})"
                if py_sig
                else None,
                "robin_signature": f"{name}({args_signature(robin_args)})"
                if robin_sig
                else None,
                "pyspark_args": py_args,
                "robin_args": robin_args,
                "notes": "",
            }
        )
    return results


def compare_class_methods(
    py_data: dict, robin_data: dict, class_name: str
) -> list[dict]:
    py_map = build_pyspark_class_map(py_data, class_name)
    robin_map = build_robin_class_map(robin_data, class_name)
    all_names = sorted(set(py_map) | set(robin_map))
    results = []
    for name in all_names:
        py_sig = py_map.get(name)
        robin_sig = robin_map.get(name)
        classification = classify_item(py_sig, robin_sig)
        py_args = (py_sig or {}).get("args") or []
        robin_args = (robin_sig or {}).get("args") or []
        results.append(
            {
                "class": class_name,
                "name": name,
                "classification": classification,
                "pyspark_signature": f"{name}({args_signature(py_args)})"
                if py_sig
                else None,
                "robin_signature": f"{name}({args_signature(robin_args)})"
                if robin_sig
                else None,
                "notes": "",
            }
        )
    return results


def write_gap_analysis_md(out: dict, path: Path, pyspark_source: str) -> None:
    """Write GAP_ANALYSIS_PYSPARK_REPO.md."""
    pv = out.get("spark_version", "unknown")
    branch = out.get("branch", "")
    funcs = out.get("functions") or []
    classes = out.get("classes") or {}
    summary = out.get("summary") or {}

    lines = [
        "# Gap Analysis: Robin-Sparkless vs PySpark (from source)",
        "",
        "This document compares robin-sparkless with **Apache PySpark** using API surface extracted directly from the [PySpark source repository](https://github.com/apache/spark/tree/master/python/pyspark).",
        "",
        "## Method",
        "",
        "- **PySpark API**: Extracted from Apache Spark repo via `scripts/extract_pyspark_api_from_repo.py` (AST parsing of `pyspark.sql` sources).",
        f"- **PySpark version/branch**: {pv} (branch/tag: {branch})",
        f"- **Robin-sparkless API**: From {pyspark_source}",
        "- **Scope**: `pyspark.sql` (functions, DataFrame, Column, GroupedData, SparkSession, Reader, Writer, Window).",
        "",
        "## Summary",
        "",
        "### Functions (pyspark.sql.functions)",
        "",
        "| Classification | Count | Description |",
        "|----------------|-------|-------------|",
        f"| exact | {summary.get('functions', {}).get('exact', 0)} | Same parameter names, order, and defaults |",
        f"| compatible | {summary.get('functions', {}).get('compatible', 0)} | Same params/defaults; types may differ |",
        f"| partial | {summary.get('functions', {}).get('partial', 0)} | Different param names or counts |",
        f"| missing | {summary.get('functions', {}).get('missing', 0)} | In PySpark but not in robin-sparkless |",
        f"| extra | {summary.get('functions', {}).get('extra', 0)} | In robin-sparkless only (extensions) |",
        "",
        f"- **PySpark functions:** {summary.get('function_total_pyspark', 0)}",
        f"- **Robin-sparkless functions:** {summary.get('function_total_robin', 0)}",
        "",
        "### Class methods",
        "",
        "| Class | Exact | Partial | Missing | Extra |",
        "|-------|-------|---------|---------|-------|",
    ]
    for cls_name in [
        "SparkSession",
        "DataFrame",
        "Column",
        "GroupedData",
        "DataFrameReader",
        "DataFrameWriter",
        "Window",
        "Catalog",
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
            "### Exact match",
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
            "### Partial (param mismatch)",
            "",
            "| PySpark | Robin |",
            "|---------|-------|",
        ]
    )
    partial_funcs = [f for f in funcs if f.get("classification") == "partial"]
    for r in partial_funcs[:25]:
        py_s = r.get("pyspark_signature") or ""
        robin_s = r.get("robin_signature") or ""
        lines.append(f"| `{py_s}` | `{robin_s}` |")
    if len(partial_funcs) > 25:
        lines.append(f"| *... and {len(partial_funcs) - 25} more* | |")
    lines.extend(
        [
            "",
            "### Missing (PySpark only)",
            "",
        ]
    )
    missing_funcs = [f for f in funcs if f.get("classification") == "missing"]
    for r in missing_funcs[:50]:
        lines.append(f"- `{r.get('pyspark_signature', r.get('name', ''))}`")
    if len(missing_funcs) > 50:
        lines.append(f"- ... and {len(missing_funcs) - 50} more")
    lines.extend(
        [
            "",
            "### Extra (robin-sparkless only)",
            "",
        ]
    )
    extra_funcs = [f for f in funcs if f.get("classification") == "extra"]
    for r in extra_funcs[:30]:
        lines.append(f"- `{r.get('robin_signature', r.get('name', ''))}`")
    if len(extra_funcs) > 30:
        lines.append(f"- ... and {len(extra_funcs) - 30} more")
    # Semantic annotations summary
    all_items = list(funcs) + sum(((v or []) for v in classes.values()), [])
    stub_items = [
        r
        for r in all_items
        if r.get("semantic_flags") and "stub" in r.get("semantic_flags", [])
    ]
    diverges_items = [
        r
        for r in all_items
        if r.get("semantic_flags") and "diverges" in r.get("semantic_flags", [])
    ]
    deferred_items = [
        r
        for r in all_items
        if r.get("semantic_flags") and "deferred" in r.get("semantic_flags", [])
    ]
    if stub_items or diverges_items or deferred_items:
        lines.extend(
            [
                "",
                "---",
                "",
                "## Semantic annotations",
                "",
                "Items tagged from [docs/gap_annotations.json](gap_annotations.json) and [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md):",
                "",
            ]
        )
        if stub_items:
            lines.extend(
                ["**stub** (no-op or placeholder):", ""]
                + [
                    f"- `{r.get('name', r.get('pyspark_signature', ''))}`"
                    for r in stub_items[:25]
                ]
                + [""]
            )
        if diverges_items:
            lines.extend(
                ["**diverges** (behavior differs from PySpark):", ""]
                + [
                    f"- `{r.get('name', r.get('pyspark_signature', ''))}`"
                    for r in diverges_items[:20]
                ]
                + [""]
            )
        if deferred_items:
            lines.extend(
                ["**deferred** (out of scope):", ""]
                + [
                    f"- `{r.get('name', r.get('pyspark_signature', ''))}`"
                    for r in deferred_items[:25]
                ]
                + [""]
            )
        lines.append(
            "Parity fixture coverage: see [PARITY_STATUS.md](PARITY_STATUS.md)."
        )

    lines.extend(
        [
            "",
            "---",
            "",
            "## Regeneration",
            "",
            "```bash",
            "python scripts/extract_pyspark_api_from_repo.py --clone --branch v3.5.0",
            "python scripts/extract_robin_api_from_source.py  # or use existing signatures_robin_sparkless.json",
            "python scripts/gap_analysis_pyspark_repo.py --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md",
            "```",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Gap analysis: PySpark (repo) vs robin-sparkless"
    )
    parser.add_argument(
        "--pyspark",
        default="docs/pyspark_api_from_repo.json",
        help="PySpark API from repo (extract_pyspark_api_from_repo.py output)",
    )
    parser.add_argument(
        "--robin",
        default="docs/signatures_robin_sparkless.json",
        help="Robin signatures (export_robin_signatures.py or robin_api_from_source.json)",
    )
    parser.add_argument(
        "--output",
        default="docs/GAP_ANALYSIS_PYSPARK_REPO.json",
        help="Output JSON",
    )
    parser.add_argument(
        "--write-md",
        metavar="PATH",
        default="docs/GAP_ANALYSIS_PYSPARK_REPO.md",
        help="Write markdown report (default: docs/GAP_ANALYSIS_PYSPARK_REPO.md)",
    )
    parser.add_argument(
        "--no-write-md",
        action="store_true",
        help="Do not write markdown",
    )
    parser.add_argument(
        "--annotations",
        type=Path,
        default=Path("docs/gap_annotations.json"),
        help="Semantic annotations JSON (stub, diverges, deferred)",
    )
    args = parser.parse_args()

    py_path = Path(args.pyspark)
    robin_path = Path(args.robin)
    if not py_path.exists():
        print(
            f"Missing: {py_path}. Run extract_pyspark_api_from_repo.py first.",
            file=__import__("sys").stderr,
        )
        return 1
    if not robin_path.exists():
        print(
            f"Missing: {robin_path}. Run export_robin_signatures.py or extract_robin_api_from_source.py.",
            file=__import__("sys").stderr,
        )
        return 1

    py_data = load_json(py_path)
    robin_data = load_json(robin_path)

    # Detect robin source format
    robin_source = "signatures_robin_sparkless.json (introspection)"
    if robin_data.get("source") == "robin_source":
        robin_source = "robin_api_from_source.json (source extraction)"

    func_comparison = compare_functions(py_data, robin_data)
    class_names = [
        "SparkSession",
        "DataFrame",
        "Column",
        "GroupedData",
        "DataFrameReader",
        "DataFrameWriter",
        "DataFrameWriterV2",
        "Window",
        "WindowSpec",
        "Catalog",
    ]
    class_comparisons = {}
    for c in class_names:
        comp = compare_class_methods(py_data, robin_data, c)
        if comp:
            class_comparisons[c] = comp

    counts = {"exact": 0, "compatible": 0, "partial": 0, "missing": 0, "extra": 0}
    for r in func_comparison:
        c = r["classification"]
        if c in counts:
            counts[c] += 1

    # Apply semantic annotations (stub, diverges, deferred)
    annotations = load_annotations(args.annotations)
    if annotations:
        apply_semantic_annotations(func_comparison, annotations)
        for class_items in class_comparisons.values():
            apply_semantic_annotations(class_items, annotations)

    out = {
        "source": "gap_analysis_pyspark_repo",
        "spark_version": py_data.get("spark_version"),
        "branch": py_data.get("branch"),
        "functions": func_comparison,
        "classes": class_comparisons,
        "summary": {
            "functions": counts,
            "function_total_pyspark": len(build_pyspark_function_map(py_data)),
            "function_total_robin": len(build_robin_function_map(robin_data)),
        },
    }

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Wrote {out_path}")

    if not args.no_write_md and args.write_md:
        md_path = Path(args.write_md)
        write_gap_analysis_md(out, md_path, robin_source)
        print(f"Wrote {md_path}")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
