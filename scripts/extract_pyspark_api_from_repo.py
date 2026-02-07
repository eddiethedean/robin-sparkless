#!/usr/bin/env python3
"""
Extract PySpark pyspark.sql API from Apache Spark source repository.
Uses AST parsing to extract functions and methods directly from source.
Does not require PySpark to be installed.

Usage:
  python scripts/extract_pyspark_api_from_repo.py --clone --branch v3.5.0
  python scripts/extract_pyspark_api_from_repo.py --repo-path /path/to/spark
  python scripts/extract_pyspark_api_from_repo.py --clone --output docs/pyspark_api_3.5.0.json
"""

from __future__ import annotations

import argparse
import ast
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


SPARK_REPO_URL = "https://github.com/apache/spark.git"
PYSPARK_BASE = "python/pyspark/sql"

# Source files to parse: (path_suffix, extraction_mode)
# extraction_mode: "functions" = module-level defs, "class:ClassName" = methods of that class
FILES_TO_PARSE: list[tuple[str, str]] = [
    ("functions.py", "functions"),
    ("dataframe.py", "class:DataFrame"),
    ("column.py", "class:Column"),
    ("group.py", "class:GroupedData"),
    ("session.py", "class:SparkSession"),
    ("readwriter.py", "class:DataFrameReader"),
    ("readwriter.py", "class:DataFrameWriter"),
    ("readwriter.py", "class:DataFrameWriterV2"),
    ("window.py", "class:Window"),
    ("window.py", "class:WindowSpec"),
    ("catalog.py", "class:Catalog"),
]

# Re-exports from typing / other modules to skip in functions.py
SKIP_FUNCTION_NAMES = frozenset(
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
        "no_type_check",
        "get_args",
        "get_origin",
        "get_type_hints",
        "warnings",
        "attr_name",
        "attr_value",
        "Functions",
    }
)


def clone_repo(branch: str, target_dir: Path) -> bool:
    """Clone Apache Spark repo and checkout branch. Returns True on success."""
    try:
        subprocess.run(
            [
                "git",
                "clone",
                "--depth",
                "1",
                "--branch",
                branch,
                SPARK_REPO_URL,
                str(target_dir),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"git clone failed: {e.stderr}", file=sys.stderr)
        return False
    except FileNotFoundError:
        print("git not found. Install git or use --repo-path.", file=sys.stderr)
        return False


def get_version_from_repo(repo_path: Path, branch: str) -> str:
    """Extract version from branch name (e.g. v3.5.0 -> 3.5.0) or pom.xml."""
    # Prefer branch/tag when it looks like a version
    if branch.startswith("v") and branch[1:2].isdigit():
        return branch[1:]
    pom = repo_path / "pom.xml"
    if pom.exists():
        try:
            import re

            content = pom.read_text()
            # Look for <version>3.5.0</version> in project (not parent)
            m = re.search(
                r"<project[^>]*>.*?<version>([\d.]+(?:-SNAPSHOT)?)</version>",
                content,
                re.DOTALL,
            )
            if m:
                return m.group(1)
        except Exception:
            pass
    return "unknown"


def param_to_dict(node: ast.arg) -> dict[str, Any]:
    """Convert ast.arg to dict."""
    return {
        "name": node.arg,
        "default": None,
        "annotation": ast.unparse(node.annotation) if node.annotation else None,  # type: ignore[attr-defined]
        "kind": "POSITIONAL_OR_KEYWORD",
    }


def extract_args_from_func(node: ast.FunctionDef) -> list[dict[str, Any]]:
    """Extract argument list from FunctionDef, excluding self/cls."""
    args = []
    # positional only
    for arg in node.args.posonlyargs:
        if arg.arg in ("self", "cls"):
            continue
        d = param_to_dict(arg)
        args.append(d)
    # regular args
    defaults = node.args.defaults
    num_required = len(node.args.args) - len(defaults)
    for i, arg in enumerate(node.args.args):
        if arg.arg in ("self", "cls"):
            continue
        d = param_to_dict(arg)
        if i >= num_required:
            idx = i - num_required
            if idx < len(defaults):
                try:
                    d["default"] = ast.unparse(defaults[idx])  # type: ignore[attr-defined]
                except Exception:
                    d["default"] = "<expr>"
        args.append(d)
    # *args, **kwargs - simplified
    if node.args.vararg:
        args.append(
            {
                "name": node.args.vararg.arg,
                "default": None,
                "annotation": None,
                "kind": "VAR_POSITIONAL",
            }
        )
    if node.args.kwarg:
        args.append(
            {
                "name": node.args.kwarg.arg,
                "default": None,
                "annotation": None,
                "kind": "VAR_KEYWORD",
            }
        )
    return args


def extract_doc_preview(node: ast.FunctionDef) -> str:
    """First line of docstring or empty."""
    doc = ast.get_docstring(node)
    if doc:
        return doc.split("\n")[0].strip()[:120]
    return ""


def extract_module_functions(tree: ast.Module) -> list[dict[str, Any]]:
    """Extract module-level function definitions."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            if node.name.startswith("_"):
                continue
            if node.name in SKIP_FUNCTION_NAMES:
                continue
            # Only top-level (parent is Module)
            for child in ast.iter_child_nodes(tree):
                if child is node:
                    out.append(
                        {
                            "name": node.name,
                            "args": extract_args_from_func(node),
                            "doc_preview": extract_doc_preview(node),
                        }
                    )
                    break
    # ast.walk doesn't preserve hierarchy; collect top-level defs manually
    out = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            if node.name.startswith("_") or node.name in SKIP_FUNCTION_NAMES:
                continue
            out.append(
                {
                    "name": node.name,
                    "args": extract_args_from_func(node),
                    "doc_preview": extract_doc_preview(node),
                }
            )
    return out


def extract_class_methods(tree: ast.Module, class_name: str) -> list[dict[str, Any]]:
    """Extract methods of a specific class."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    if item.name.startswith("_"):
                        continue
                    out.append(
                        {
                            "name": item.name,
                            "args": extract_args_from_func(item),
                            "doc_preview": extract_doc_preview(item),
                            "class": class_name,
                        }
                    )
            break
    return out


def parse_file(repo_path: Path, rel_path: str) -> ast.Module | None:
    """Parse a Python file and return AST."""
    full_path = repo_path / rel_path
    if not full_path.exists():
        return None
    try:
        source = full_path.read_text(encoding="utf-8", errors="replace")
        return ast.parse(source)
    except SyntaxError as e:
        print(f"Syntax error in {rel_path}: {e}", file=sys.stderr)
        return None


def extract_from_repo(repo_path: Path, branch: str) -> dict[str, Any]:
    """Extract full API from repo."""
    base = repo_path / PYSPARK_BASE
    if not base.exists():
        raise FileNotFoundError(f"PySpark path not found: {base}")

    version = get_version_from_repo(repo_path, branch)
    result: dict[str, Any] = {
        "source": "pyspark_repo",
        "spark_version": version,
        "branch": branch,
        "functions": [],
        "DataFrame": [],
        "Column": [],
        "GroupedData": [],
        "SparkSession": [],
        "DataFrameReader": [],
        "DataFrameWriter": [],
        "DataFrameWriterV2": [],
        "Window": [],
        "WindowSpec": [],
        "Catalog": [],
    }

    # Track which files we've parsed (readwriter appears multiple times)
    parsed_files: dict[str, ast.Module] = {}

    for rel_suffix, mode in FILES_TO_PARSE:
        rel_path = f"{PYSPARK_BASE}/{rel_suffix}"
        if rel_path not in parsed_files:
            tree = parse_file(repo_path, rel_path)
            if tree is None:
                continue
            parsed_files[rel_path] = tree
        tree = parsed_files[rel_path]

        if mode == "functions":
            result["functions"] = extract_module_functions(tree)
        elif mode.startswith("class:"):
            class_name = mode.split(":", 1)[1]
            methods = extract_class_methods(tree, class_name)
            key = class_name
            if key in result and isinstance(result[key], list):
                result[key].extend(methods)
            else:
                result[key] = methods

    # Sort functions by name
    result["functions"] = sorted(result["functions"], key=lambda x: x["name"])
    for key in [
        "DataFrame",
        "Column",
        "GroupedData",
        "SparkSession",
        "DataFrameReader",
        "DataFrameWriter",
        "DataFrameWriterV2",
        "Window",
        "WindowSpec",
        "Catalog",
    ]:
        if key in result and isinstance(result[key], list):
            result[key] = sorted(result[key], key=lambda x: x["name"])

    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract PySpark API from Apache Spark source repository"
    )
    parser.add_argument(
        "--repo-path",
        type=Path,
        help="Path to cloned Spark repository",
    )
    parser.add_argument(
        "--clone",
        action="store_true",
        help="Clone Spark repo to temp dir (requires git)",
    )
    parser.add_argument(
        "--branch",
        default="v3.5.0",
        help="Branch or tag to use (default: v3.5.0)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("docs/pyspark_api_from_repo.json"),
        help="Output JSON path",
    )
    args = parser.parse_args()

    repo_path: Path | None = args.repo_path
    temp_dir: Path | None = None

    if args.clone:
        if repo_path is not None:
            print("Ignoring --repo-path when --clone is used", file=sys.stderr)
        temp_dir = Path(tempfile.mkdtemp(prefix="spark_repo_"))
        try:
            if not clone_repo(args.branch, temp_dir):
                return 1
            repo_path = temp_dir
        except Exception as e:
            print(f"Clone failed: {e}", file=sys.stderr)
            if temp_dir and temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
            return 1
    elif repo_path is None:
        print("Must specify --repo-path or --clone", file=sys.stderr)
        return 1

    try:
        result = extract_from_repo(repo_path, args.branch)
        out_path = args.output
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)
        print(
            f"Wrote {out_path} (Spark {result.get('spark_version', '?')}, branch {args.branch})"
        )
        print(f"  functions: {len(result.get('functions', []))}")
        for cls in ["DataFrame", "Column", "GroupedData", "SparkSession"]:
            count = len(result.get(cls, []))
            if count:
                print(f"  {cls}: {count}")
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        raise
    finally:
        if temp_dir is not None and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
