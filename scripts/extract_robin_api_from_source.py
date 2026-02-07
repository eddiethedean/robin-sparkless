#!/usr/bin/env python3
"""
Extract robin-sparkless API from Rust source (no build required).
Parses src/python/mod.rs, column.rs, dataframe.rs, session.rs for
module-level functions and class methods exposed via PyO3.
Output: docs/robin_api_from_source.json (same structure as signatures_robin_sparkless.json).

Usage:
  python scripts/extract_robin_api_from_source.py [--output docs/robin_api_from_source.json]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


SRC = Path(__file__).resolve().parent.parent / "src"


def extract_module_functions(mod_rs: Path) -> list[dict[str, Any]]:
    """Extract m.add("name", wrap_pyfunction!(...)) from mod.rs."""
    text = mod_rs.read_text(encoding="utf-8", errors="replace")
    # m.add("func_name", wrap_pyfunction!(py_func, m)
    pattern = re.compile(r'm\.add\s*\(\s*["\']([^"\']+)["\']\s*,')
    names = set()
    for m in pattern.finditer(text):
        name = m.group(1)
        if not name.startswith("_"):
            names.add(name)
    return [{"name": n, "args": [], "kind": "function"} for n in sorted(names)]


def extract_impl_methods(file_path: Path, struct_name: str) -> list[dict[str, Any]]:
    """Extract fn name(...) from impl StructName {...} using brace counting."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    methods = []
    # Find "impl StructName {" (with optional #[pymethods] before it)
    impl_re = re.compile(rf"(?:#\[pymethods\]\s*\n\s*)?impl\s+{re.escape(struct_name)}\s*\{{")
    for impl_match in impl_re.finditer(text):
        start = impl_match.end()
        depth = 1
        i = start
        while i < len(text) and depth > 0:
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
            i += 1
        block = text[start : i - 1]  # exclude closing }
        # Match fn name(...) - handle #[pyo3(name = "py_name")] override
        fn_pattern = re.compile(
            r'(?:#\[pyo3\s*\(\s*name\s*=\s*["\']([^"\']+)["\']\s*\)\]\s*\n\s*)?'
            r"fn\s+(\w+)\s*\(",
        )
        for m in fn_pattern.finditer(block):
            py_name = m.group(1) or m.group(2)
            if py_name.startswith("__") and py_name.endswith("__"):
                continue
            methods.append({"name": py_name, "args": [], "class": struct_name.replace("Py", "")})
    return methods


def extract_class_methods_from_file(
    file_path: Path, class_map: list[tuple[str, str]]
) -> dict[str, list[dict[str, Any]]]:
    """Extract methods for multiple classes from one file.
    class_map: [(rust_struct_name, output_class_name), ...]
    """
    result: dict[str, list[dict[str, Any]]] = {}
    for rust_name, out_name in class_map:
        methods = extract_impl_methods(file_path, rust_name)
        # Normalize class name for output (PyColumn -> Column, etc.)
        result[out_name] = [{"name": m["name"], "args": m.get("args", [])} for m in methods]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract robin-sparkless API from Rust source"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("docs/robin_api_from_source.json"),
        help="Output JSON path",
    )
    args = parser.parse_args()

    if not (SRC / "python" / "mod.rs").exists():
        print("Source not found. Run from repo root.", file=sys.stderr)
        return 1

    result: dict[str, Any] = {
        "source": "robin_source",
        "functions": [],
        "classes": {},
    }

    # Module-level functions from mod.rs
    mod_rs = SRC / "python" / "mod.rs"
    result["functions"] = extract_module_functions(mod_rs)

    # Column methods
    col_rs = SRC / "python" / "column.rs"
    if col_rs.exists():
        result["classes"]["Column"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(col_rs, "PyColumn")
        ]

    # DataFrame methods
    df_rs = SRC / "python" / "dataframe.rs"
    if df_rs.exists():
        result["classes"]["DataFrame"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(df_rs, "PyDataFrame")
        ]
        result["classes"]["GroupedData"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(df_rs, "PyGroupedData")
        ]
        result["classes"]["DataFrameStat"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(df_rs, "PyDataFrameStat")
        ]
        result["classes"]["DataFrameNa"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(df_rs, "PyDataFrameNa")
        ]

    # SparkSession, SparkSessionBuilder from session.rs
    session_rs = SRC / "python" / "session.rs"
    if session_rs.exists():
        result["classes"]["SparkSession"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(session_rs, "PySparkSession")
        ]
        result["classes"]["SparkSessionBuilder"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(session_rs, "PySparkSessionBuilder")
        ]

    out_path = args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)

    n_funcs = len(result["functions"])
    n_classes = sum(len(v) for v in result["classes"].values())
    print(f"Wrote {out_path} ({n_funcs} functions, {n_classes} class methods)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
