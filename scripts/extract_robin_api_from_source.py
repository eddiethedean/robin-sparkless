#!/usr/bin/env python3
"""
Extract robin-sparkless API from Rust source (no build required).
Parses src/python/mod.rs, column.rs, dataframe.rs, session.rs for
module-level functions and class methods exposed via PyO3.
Extracts #[pyo3(signature = (...))] and fn params for accurate arg lists.
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


def parse_pyo3_signature(sig_str: str) -> list[dict[str, Any]]:
    """Parse #[pyo3(signature = (a, b=None, c=1))] inner content to args list."""
    sig_str = sig_str.strip()
    if not sig_str or sig_str == "()":
        return []
    args: list[dict[str, Any]] = []
    # Split by comma, respecting nested parens
    depth = 0
    start = 0
    for i, c in enumerate(sig_str):
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        elif c == "," and depth == 0:
            part = sig_str[start:i].strip()
            if part:
                args.append(_parse_sig_arg(part))
            start = i + 1
    if start < len(sig_str):
        part = sig_str[start:].strip()
        if part:
            args.append(_parse_sig_arg(part))
    return args


def _parse_sig_arg(part: str) -> dict[str, Any]:
    """Parse single arg: 'col' or 'format=None' or 'round_off=true'."""
    if "=" in part:
        name, default = part.split("=", 1)
        name = name.strip()
        default = default.strip()
        # Normalize Python None/True/False for comparison
        if default in ("None", "None)"):
            default = "None"
        elif default in ("True", "true"):
            default = "True"
        elif default in ("False", "false"):
            default = "False"
        return {"name": name, "default": default, "kind": "POSITIONAL_OR_KEYWORD"}
    return {"name": part.strip(), "default": None, "kind": "POSITIONAL_OR_KEYWORD"}


def parse_rust_fn_params(fn_sig: str) -> list[dict[str, Any]]:
    """Extract param names from fn name(arg1: Type, arg2: Type) excluding self."""
    # Match fn xxx ( ... ) - get content inside parens
    m = re.search(r"fn\s+\w+\s*\(\s*(.*?)\s*\)", fn_sig, re.DOTALL)
    if not m:
        return []
    params_str = m.group(1).strip()
    if not params_str:
        return []
    args: list[dict[str, Any]] = []
    # Split by comma, respect < > and [ ] for generics
    depth = 0
    in_lt = 0
    start = 0
    for i, c in enumerate(params_str):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "<":
            in_lt += 1
        elif c == ">":
            in_lt -= 1
        elif c == "," and depth == 0 and in_lt == 0:
            part = params_str[start:i].strip()
            if part and not part.startswith("&self") and part != "self":
                name = part.split(":")[0].strip().lstrip("&")
                if name.startswith("mut "):
                    name = name[4:]
                if name != "self":
                    args.append(
                        {"name": name, "default": None, "kind": "POSITIONAL_OR_KEYWORD"}
                    )
            start = i + 1
    if start < len(params_str):
        part = params_str[start:].strip()
        if part and not part.startswith("&self") and part != "self":
            name = part.split(":")[0].strip().lstrip("&")
            if name.startswith("mut "):
                name = name[4:]
            if name != "self":
                args.append(
                    {"name": name, "default": None, "kind": "POSITIONAL_OR_KEYWORD"}
                )
    return args


def extract_pyo3_signature_and_fn(
    text: str, match_start: int, match_end: int
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Extract signature from #[pyo3(signature=(...))] and fn params.
    match_start/end: span of the outer match (may include #[pyo3] above fn).
    """
    sig_re = re.compile(
        r"#\[\s*pyo3\s*\(\s*signature\s*=\s*\((.*?)\)\s*\)\s*\]",
        re.DOTALL,
    )
    # Search in the matched region (includes #[pyo3] if present) and 200 chars before
    search_start = max(0, match_start - 250)
    chunk = text[search_start:match_end]
    sig_m = sig_re.search(chunk)
    # Only use signature if it's within 100 chars of fn start (avoid prev fn's signature)
    args: list[dict[str, Any]] = []
    fn_offset_in_chunk = match_start - search_start
    if sig_m and (fn_offset_in_chunk - sig_m.end()) <= 100:
        inner = sig_m.group(1).strip()
        inner = re.sub(r"\s+", " ", inner)
        args = parse_pyo3_signature(inner)

    fn_re = re.compile(r"fn\s+(\w+)\s*\([^)]*\)")
    fn_m = fn_re.search(text, match_start)
    fn_name = fn_m.group(1) if fn_m else None

    if not args and fn_m:
        full_sig = fn_m.group(0)
        args = parse_rust_fn_params(full_sig)

    return (args, fn_name)


def extract_module_functions(mod_rs: Path) -> list[dict[str, Any]]:
    """Extract m.add("name", wrap_pyfunction!(py_xxx, m)) and resolve signatures."""
    text = mod_rs.read_text(encoding="utf-8", errors="replace")

    # Build map: py_func_name -> args by scanning for fn py_xxx and #[pyo3(signature)]
    func_sigs: dict[str, list[dict[str, Any]]] = {}
    for m in re.finditer(r"(?:#\[pyo3[^\]]*\]\s*\n\s*)*fn\s+(py_\w+)\s*\(", text):
        fn_name = m.group(1)
        args, _ = extract_pyo3_signature_and_fn(text, m.start(), m.end())
        func_sigs[fn_name] = args

    for m in re.finditer(r"fn\s+(py_\w+)\s*\(", text):
        fn_name = m.group(1)
        if fn_name not in func_sigs:
            args, _ = extract_pyo3_signature_and_fn(text, m.start(), m.end())
            func_sigs[fn_name] = args

    # Extract m.add("name", wrap_pyfunction!(py_xxx, m))
    add_re = re.compile(
        r'm\.add\s*\(\s*["\']([^"\']+)["\']\s*,\s*wrap_pyfunction!\s*\(\s*(\w+)\s*,'
    )
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for m in add_re.finditer(text):
        name = m.group(1)
        if name.startswith("_"):
            continue
        py_func = m.group(2)
        args = func_sigs.get(py_func, [])
        key = name
        if key not in seen:
            seen.add(key)
            result.append({"name": name, "args": args, "kind": "function"})

    result.sort(key=lambda x: x["name"])
    return result


def extract_impl_methods(file_path: Path, struct_name: str) -> list[dict[str, Any]]:
    """Extract fn name(...) from impl StructName with #[pyo3(signature)] and args."""
    text = file_path.read_text(encoding="utf-8", errors="replace")
    methods: list[dict[str, Any]] = []
    impl_re = re.compile(
        rf"(?:#\[pymethods\]\s*\n\s*)?impl\s+{re.escape(struct_name)}\s*\{{"
    )
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
        block = text[start : i - 1]

        # Match #[pyo3(name = "py_name")] and/or #[pyo3(signature = (...))] and fn name(...)
        # Pattern: optional name, optional signature, then fn
        fn_block_re = re.compile(
            r'(?:#\[pyo3\s*\(\s*name\s*=\s*["\']([^"\']+)["\']\s*\)\]\s*\n\s*)?'
            r"(?:#\[pyo3\s*\(\s*signature\s*=\s*\((.*?)\)\s*\)\s*\]\s*\n\s*)?"
            r"fn\s+(\w+)\s*\(",
            re.DOTALL,
        )
        for m in fn_block_re.finditer(block):
            py_name = m.group(1) or m.group(3)  # name override or Rust fn name
            if py_name.startswith("__") and py_name.endswith("__"):
                continue
            sig_inner = m.group(2)
            if sig_inner is not None:
                sig_inner = re.sub(r"\s+", " ", sig_inner.strip())
                args = parse_pyo3_signature(sig_inner)
            else:
                # Parse fn params
                fn_sig = m.group(0) + "..."  # partial; get full fn line
                end = block.find(")", m.end()) + 1
                if end > m.end():
                    fn_sig = block[m.start() : end]
                args = parse_rust_fn_params(fn_sig)
            methods.append(
                {"name": py_name, "args": args, "class": struct_name.replace("Py", "")}
            )
    return methods


def extract_class_methods_from_file(
    file_path: Path, class_map: list[tuple[str, str]]
) -> dict[str, list[dict[str, Any]]]:
    """Extract methods for multiple classes from one file."""
    result: dict[str, list[dict[str, Any]]] = {}
    for rust_name, out_name in class_map:
        methods = extract_impl_methods(file_path, rust_name)
        result[out_name] = [
            {"name": m["name"], "args": m.get("args", [])} for m in methods
        ]
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

    mod_rs = SRC / "python" / "mod.rs"
    result["functions"] = extract_module_functions(mod_rs)

    col_rs = SRC / "python" / "column.rs"
    if col_rs.exists():
        result["classes"]["Column"] = [
            {"name": m["name"], "args": m.get("args", [])}
            for m in extract_impl_methods(col_rs, "PyColumn")
        ]

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
    n_with_args = sum(1 for f in result["functions"] if f.get("args"))
    n_classes = sum(len(v) for v in result["classes"].values())
    print(
        f"Wrote {out_path} ({n_funcs} functions, {n_with_args} with args; {n_classes} class methods)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
