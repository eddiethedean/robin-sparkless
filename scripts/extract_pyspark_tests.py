#!/usr/bin/env python3
"""
Extract Apache PySpark SQL tests and produce (a) JSON fixtures for behavioral tests,
(b) Python pytest stubs for error-handling/API tests.

Parse strategy: AST parse each target file; for each test_* method, identify patterns
(createDataFrame, spark.range, df.filter/select/withColumn/groupBy, collect,
assertDataFrameEqual, assertRaises) and classify as:
- fixture-candidate: createDataFrame + ops + collect + assert (emit minimal JSON fixture)
- python-test-candidate: error handling, API checks, assertRaises (emit pytest stub)

Output:
- tests/fixtures/pyspark_extracted/*.json — minimal fixtures (run regenerate_expected_from_pyspark.py)
- tests/python/test_pyspark_port_*.py — pytest stubs for error/API tests

Usage:
  python scripts/extract_pyspark_tests.py --spark-repo /path/to/spark
  python scripts/extract_pyspark_tests.py --clone --branch v3.5.0
  python scripts/extract_pyspark_tests.py --spark-repo /path/to/spark --dry-run
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

# Target files in python/pyspark/sql/tests/
TARGET_FILES = [
    "test_functions.py",
    "test_dataframe.py",
    "test_column.py",
    "test_group.py",
    "test_readwriter.py",
    "test_session.py",
    "test_sql.py",
]

# Exclude patterns (don't process)
EXCLUDE_PATTERNS = (
    "test_udf",
    "test_pandas",
    "test_streaming",
    "test_connect",
    "test_arrow",
    "test_plot",
    "coercion",
    "typing",
)


def should_skip_test(name: str) -> bool:
    """Return True if test should be skipped (UDF, pandas, streaming, etc.)."""
    name_lower = name.lower()
    return any(pat in name_lower for pat in EXCLUDE_PATTERNS)


class TestClassifier(ast.NodeVisitor):
    """AST visitor to classify a test method as fixture-candidate or python-test-candidate."""

    def __init__(self) -> None:
        self.has_create_dataframe = False
        self.has_spark_range = False
        self.has_read = False
        self.has_df_ops = False  # filter, select, withColumn, groupBy, etc.
        self.has_collect = False
        self.has_assert_dataframe_equal = False
        self.has_assert_raises = False
        self.has_self_sc = False  # RDD / SparkContext
        self.has_assert_equal = False
        self.has_assert_close = False
        self.test_source: str | None = None

    def visit_Call(self, node: ast.Call) -> None:
        name = self._call_name(node)
        if name:
            if "createDataFrame" in name or "create_dataframe" in name:
                self.has_create_dataframe = True
            elif "range" in name and ("spark" in name or "session" in name):
                self.has_spark_range = True
            elif "read" in name and ("spark" in name or "session" in name):
                self.has_read = True
            elif name in (
                "filter",
                "select",
                "withColumn",
                "groupBy",
                "orderBy",
                "agg",
                "join",
                "limit",
                "distinct",
                "drop",
                "dropna",
                "fillna",
            ):
                self.has_df_ops = True
            elif "collect" in name:
                self.has_collect = True
            elif "assertDataFrameEqual" in name or "assert_dataframe_equal" in name:
                self.has_assert_dataframe_equal = True
            elif "assertRaises" in name or "assert_raises" in name:
                self.has_assert_raises = True
            elif "assertEqual" in name or "assert_equal" in name:
                self.has_assert_equal = True
            elif "assert_close" in name or "assertClose" in name:
                self.has_assert_close = True
        # Check for self.sc (SparkContext / RDD)
        if isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Attribute):
                if node.func.value.attr == "sc":
                    self.has_self_sc = True
        self.generic_visit(node)

    def _call_name(self, node: ast.Call) -> str:
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        if isinstance(node.func, ast.Name):
            return node.func.id
        return ""

    def classify(self) -> tuple[str, str]:
        """Return (classification, reason)."""
        if self.has_self_sc:
            return "skip", "uses self.sc (RDD)"
        if self.has_assert_raises:
            return "python-test", "assertRaises / error handling"
        if self.has_create_dataframe and self.has_df_ops and self.has_collect:
            if self.has_assert_dataframe_equal or self.has_assert_equal or self.has_assert_close:
                return "fixture", "createDataFrame + ops + collect + assert"
            return "fixture", "createDataFrame + ops + collect"
        if self.has_spark_range and self.has_df_ops and self.has_collect:
            return "fixture", "spark.range + ops + collect"
        if self.has_read and self.has_df_ops and self.has_collect:
            return "fixture", "spark.read + ops + collect"
        if self.has_df_ops and (self.has_assert_equal or self.has_assert_close):
            return "python-test", "API/metadata assertion"
        return "skip", "no fixture pattern matched"


def extract_tests_from_file(
    path: Path, source: str
) -> list[dict[str, Any]]:
    """Parse file and extract test info for each test_* method."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    results: list[dict[str, Any]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            if should_skip_test(node.name):
                results.append({
                    "name": node.name,
                    "file": str(path),
                    "classification": "skip",
                    "reason": "excluded pattern",
                })
                continue
            classifier = TestClassifier()
            classifier.visit(node)
            class_name = _find_class_for_method(tree, node)
            classification, reason = classifier.classify()
            results.append({
                "name": node.name,
                "class": class_name,
                "file": str(path),
                "classification": classification,
                "reason": reason,
            })
    return results


def _find_class_for_method(tree: ast.AST, method: ast.FunctionDef) -> str | None:
    """Find the class containing the given method."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for stmt in node.body:
                if stmt is method:
                    return node.name
    return None


def clone_spark_repo(branch: str, dest: Path) -> Path:
    """Clone Apache Spark repo. Returns path to repo."""
    dest.mkdir(parents=True, exist_ok=True)
    spark_path = dest / "spark"
    if spark_path.exists():
        return spark_path
    subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", branch, "https://github.com/apache/spark.git", str(spark_path)],
        check=True,
        capture_output=True,
    )
    return spark_path


def emit_fixture_stub(
    test_name: str,
    source_file: str,
    output_dir: Path,
    dry_run: bool,
) -> Path | None:
    """Emit minimal fixture JSON stub. Expected is placeholder; run regenerate_expected_from_pyspark.py."""
    fixture_name = test_name.replace("test_", "", 1)
    out_path = output_dir / f"{fixture_name}.json"
    stub = {
        "name": fixture_name,
        "pyspark_version": "3.5",
        "source": f"extracted from {source_file}",
        "input": {
            "schema": [{"name": "id", "type": "bigint"}, {"name": "value", "type": "double"}],
            "rows": [[1, 1.0], [2, 2.0], [3, 3.0]],
        },
        "operations": [],
        "expected": {"schema": [], "rows": []},
        "skip": True,
        "skip_reason": "Placeholder from extractor; add operations and run regenerate_expected_from_pyspark.py --include-skipped",
    }
    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(stub, f, indent=2)
    return out_path


def emit_pytest_stub(
    test_name: str,
    class_name: str | None,
    source_file: str,
    output_path: Path,
    dry_run: bool,
) -> None:
    """Append pytest stub for python-test-candidate."""
    stub = f'''
@pytest.mark.skip(reason="Ported from {source_file}; implement with robin_sparkless")
def {test_name}() -> None:
    """Ported from PySpark {class_name or '?'}.{test_name}."""
    # with pytest.raises(...): ...
    pass
'''
    if not dry_run:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "a") as f:
            f.write(stub)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract PySpark SQL tests to fixtures and pytest stubs"
    )
    parser.add_argument(
        "--spark-repo",
        type=Path,
        help="Path to Apache Spark repo (python/pyspark/sql/tests/ must exist)",
    )
    parser.add_argument(
        "--clone",
        action="store_true",
        help="Clone Spark repo (use with --branch)",
    )
    parser.add_argument(
        "--branch",
        default="v3.5.0",
        help="Spark branch/tag for --clone (default: v3.5.0)",
    )
    parser.add_argument(
        "--output-fixtures",
        type=Path,
        default=Path("tests/fixtures/pyspark_extracted"),
        help="Output dir for fixture JSON (default: tests/fixtures/pyspark_extracted)",
    )
    parser.add_argument(
        "--output-pytest",
        type=Path,
        default=Path("tests/python/test_pyspark_port_extracted.py"),
        help="Output file for pytest stubs (default: tests/python/test_pyspark_port_extracted.py)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only classify; do not write files",
    )
    args = parser.parse_args()

    spark_repo: Path | None = None
    if args.spark_repo:
        spark_repo = args.spark_repo.resolve()
        tests_dir = spark_repo / "python" / "pyspark" / "sql" / "tests"
        if not tests_dir.exists():
            print(f"Error: {tests_dir} does not exist", file=sys.stderr)
            return 1
    elif args.clone:
        dest = Path.cwd() / ".spark_clone"
        spark_repo = clone_spark_repo(args.branch, dest)
        tests_dir = spark_repo / "python" / "pyspark" / "sql" / "tests"
    else:
        print("Error: provide --spark-repo PATH or --clone", file=sys.stderr)
        return 1

    assert spark_repo is not None
    tests_dir = spark_repo / "python" / "pyspark" / "sql" / "tests"
    if not tests_dir.exists():
        print(f"Error: {tests_dir} does not exist", file=sys.stderr)
        return 1

    all_results: list[dict[str, Any]] = []
    for fname in TARGET_FILES:
        fpath = tests_dir / fname
        if not fpath.exists():
            continue
        source = fpath.read_text(encoding="utf-8", errors="replace")
        results = extract_tests_from_file(fpath, source)
        all_results.extend(results)

    fixture_count = 0
    pytest_count = 0
    pytest_path = args.output_pytest
    if not args.dry_run and any(r["classification"] == "python-test" for r in all_results):
        pytest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(pytest_path, "w") as f:
            f.write('"""Ported PySpark error/API tests (extracted). Use robin_sparkless."""\n\n')
            f.write("import pytest\n\n\n")

    for r in all_results:
        if r["classification"] == "fixture":
            p = emit_fixture_stub(
                r["name"], r["file"], args.output_fixtures, args.dry_run
            )
            if p:
                fixture_count += 1
                if args.dry_run:
                    print(f"  [fixture] {r['name']} -> {p.name}")
        elif r["classification"] == "python-test":
            emit_pytest_stub(
                r["name"], r.get("class"), r["file"], pytest_path, args.dry_run
            )
            pytest_count += 1
            if args.dry_run:
                print(f"  [pytest]   {r['name']}")
        else:
            if args.dry_run:
                print(f"  [skip]    {r['name']}: {r['reason']}")

    print(f"Classified: {len(all_results)} tests")
    print(f"  fixture-candidates: {sum(1 for r in all_results if r['classification'] == 'fixture')}")
    print(f"  python-test-candidates: {sum(1 for r in all_results if r['classification'] == 'python-test')}")
    print(f"  skipped: {sum(1 for r in all_results if r['classification'] == 'skip')}")
    if not args.dry_run:
        print(f"Emitted: {fixture_count} fixtures -> {args.output_fixtures}")
        print(f"         {pytest_count} pytest stubs -> {args.output_pytest}")
        if fixture_count:
            print("Run: python tests/regenerate_expected_from_pyspark.py tests/fixtures/pyspark_extracted --include-skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
