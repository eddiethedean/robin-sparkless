#!/usr/bin/env python3
"""Extract PySpark sql API signatures from an installed PySpark or a Spark git checkout.

Usage:
  python scripts/extract_pyspark_api_from_repo.py --output docs/pyspark_api_4.x.json
  python scripts/extract_pyspark_api_from_repo.py --branch v4.1.1 --clone --output docs/pyspark_api_4.x.json

When --clone is omitted, uses the currently installed ``pyspark`` package (pip install pyspark).
"""

from __future__ import annotations

import argparse
import ast
import inspect
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


def _arg_info(param: inspect.Parameter) -> dict[str, Any]:
    default = param.default if param.default is not inspect._empty else None
    ann = param.annotation if param.annotation is not inspect._empty else None
    return {
        "name": param.name,
        "default": repr(default) if default is not None else None,
        "annotation": str(ann) if ann is not None else None,
        "kind": param.kind.name,
    }


def extract_functions(module: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, obj in sorted(inspect.getmembers(module, inspect.isfunction)):
        if name.startswith("_"):
            continue
        try:
            sig = inspect.signature(obj)
        except (TypeError, ValueError):
            continue
        doc = inspect.getdoc(obj) or ""
        out.append(
            {
                "name": name,
                "args": [_arg_info(p) for p in sig.parameters.values()],
                "doc_preview": doc.split("\n")[0][:200] if doc else "",
            }
        )
    return out


def extract_class_methods(cls: type) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for name, obj in sorted(inspect.getmembers(cls)):
        if name.startswith("_"):
            continue
        if not (inspect.isfunction(obj) or inspect.ismethoddescriptor(obj)):
            continue
        try:
            sig = inspect.signature(obj)
        except (TypeError, ValueError):
            continue
        out.append(
            {"name": name, "args": [_arg_info(p) for p in sig.parameters.values()]}
        )
    return out


def extract_from_installed_pyspark() -> dict[str, Any]:
    import pyspark
    from pyspark.sql import DataFrame, SparkSession, functions as F
    from pyspark.sql.column import Column
    from pyspark.sql.group import GroupedData

    version = getattr(pyspark, "__version__", "unknown")
    return {
        "source": "pyspark_installed",
        "spark_version": version,
        "branch": version,
        "functions": extract_functions(F),
        "classes": {
            "SparkSession": extract_class_methods(SparkSession),
            "DataFrame": extract_class_methods(DataFrame),
            "Column": extract_class_methods(Column),
            "GroupedData": extract_class_methods(GroupedData),
        },
    }


def clone_spark_repo(branch: str, dest: Path) -> Path:
    if dest.exists():
        return dest / "python" / "pyspark"
    subprocess.check_call(
        [
            "git",
            "clone",
            "--depth",
            "1",
            "--branch",
            branch,
            "https://github.com/apache/spark.git",
            str(dest),
        ]
    )
    return dest / "python" / "pyspark"


def extract_from_repo_path(pyspark_root: Path) -> dict[str, Any]:
    """Best-effort AST extraction of function defs under pyspark/sql/functions."""
    functions_file = pyspark_root / "sql" / "functions" / "__init__.py"
    if not functions_file.exists():
        functions_file = pyspark_root / "sql" / "functions.py"
    functions: list[dict[str, Any]] = []
    if functions_file.exists():
        tree = ast.parse(functions_file.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and not node.name.startswith("_"):
                args = [
                    {
                        "name": a.arg,
                        "default": None,
                        "annotation": ast.unparse(a.annotation)
                        if a.annotation
                        else None,
                        "kind": "POSITIONAL_OR_KEYWORD",
                    }
                    for a in node.args.args
                ]
                functions.append({"name": node.name, "args": args, "doc_preview": ""})
    return {
        "source": "pyspark_repo",
        "spark_version": pyspark_root.parent.parent.name,
        "branch": pyspark_root.name,
        "functions": sorted(functions, key=lambda x: x["name"]),
        "classes": {},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/pyspark_api_4.x.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--branch",
        default="v4.1.1",
        help="Spark git branch/tag when using --clone",
    )
    parser.add_argument(
        "--clone",
        action="store_true",
        help="Clone apache/spark at --branch instead of using installed pyspark",
    )
    args = parser.parse_args()

    if args.clone:
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp) / "spark"
            pyspark_root = clone_spark_repo(args.branch, repo)
            payload = extract_from_repo_path(pyspark_root)
            payload["branch"] = args.branch
    else:
        try:
            payload = extract_from_installed_pyspark()
        except ImportError:
            print(
                "pyspark not installed; use --clone or pip install pyspark",
                file=sys.stderr,
            )
            sys.exit(1)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output} ({len(payload.get('functions', []))} functions)")


if __name__ == "__main__":
    main()
