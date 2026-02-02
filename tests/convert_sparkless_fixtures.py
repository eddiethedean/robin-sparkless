#!/usr/bin/env python3
"""
Convert Sparkless expected_outputs JSON to robin-sparkless fixture format.

Sparkless format (tests/expected_outputs/*.json):
  - input_data: list of dicts (row-wise)
  - operation: string e.g. "filter_operations", "groupby", "join"
  - expected_output: schema (field_names, field_types, fields) + data (list of dicts)

Robin-sparkless format (tests/fixtures/*.json):
  - input: { schema: [{name, type}], rows: [[...], ...] }
  - operations: [{ op, ... }] (filter, select, orderBy, groupBy, agg, join, window, withColumn)
  - expected: { schema, rows }

Usage:
  python tests/convert_sparkless_fixtures.py <sparkless_json_path> [output_dir]
  python tests/convert_sparkless_fixtures.py --batch <sparkless_expected_outputs_dir> [output_dir]

When Sparkless repo is available, run from robin-sparkless root:
  python tests/convert_sparkless_fixtures.py --batch /path/to/sparkless/tests/expected_outputs tests/fixtures

See docs/SPARKLESS_INTEGRATION_ANALYSIS.md §4 for format details.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


# Map Sparkless type names to robin-sparkless fixture type names
SPARKLESS_TYPE_TO_ROBIN = {
    "long": "bigint",
    "int": "int",
    "integer": "int",
    "bigint": "bigint",
    "double": "double",
    "float": "float",
    "string": "string",
    "str": "string",
    "boolean": "boolean",
    "bool": "boolean",
}


def sparkless_schema_to_robin(schema: dict) -> list[dict[str, str]]:
    """Convert Sparkless schema to robin-sparkless schema array."""
    if "fields" in schema and schema["fields"]:
        return [
            {"name": f.get("name", ""), "type": SPARKLESS_TYPE_TO_ROBIN.get(f.get("type", "string").lower(), f.get("type", "string").lower())}
            for f in schema["fields"]
        ]
    if "field_names" in schema and "field_types" in schema:
        names = schema["field_names"]
        types = schema["field_types"]
        return [
            {"name": names[i], "type": SPARKLESS_TYPE_TO_ROBIN.get(str(types[i]).lower(), str(types[i]))}
            for i in range(len(names))
        ]
    return []


def dict_rows_to_column_rows(data: list[dict], schema: list[dict]) -> list[list[Any]]:
    """Convert list of dicts (row-wise) to list of column arrays using schema order."""
    if not schema:
        return []
    names = [s["name"] for s in schema]
    return [[row.get(n) for n in names] for row in data]


def column_rows_to_dict_rows(rows: list[list[Any]], schema: list[dict]) -> list[dict]:
    """Convert column arrays back to list of dicts."""
    if not schema or not rows:
        return []
    names = [s["name"] for s in schema]
    return [dict(zip(names, row)) for row in rows]


def convert_sparkless_to_robin(
    sparkless: dict,
    fixture_name: str | None = None,
    operation_hint: str | None = None,
) -> dict:
    """
    Convert one Sparkless expected_output JSON to robin-sparkless fixture format.

    operation_hint: if Sparkless "operation" is not enough to build operations list,
    caller can pass a hint. We map known operation types to robin-sparkless operations.
    """
    input_data = sparkless.get("input_data", [])
    operation = operation_hint or sparkless.get("operation", "")
    expected_output = sparkless.get("expected_output", {})

    if not input_data and not expected_output:
        raise ValueError("Sparkless JSON must have input_data or expected_output")

    # Infer input schema from first row of input_data if not provided
    if input_data:
        input_schema_names = list(input_data[0].keys()) if input_data else []
        # Sparkless may not give input types; use string as default
        input_schema = [{"name": n, "type": "string"} for n in input_schema_names]
    else:
        input_schema = []
        input_schema_names = []

    input_rows = dict_rows_to_column_rows(input_data, input_schema) if input_schema else []

    # Expected schema and rows
    exp_schema_raw = expected_output.get("schema", {})
    exp_schema = sparkless_schema_to_robin(exp_schema_raw) if isinstance(exp_schema_raw, dict) else (exp_schema_raw or [])
    exp_data = expected_output.get("data", [])
    if exp_data and isinstance(exp_data[0], dict):
        expected_rows = dict_rows_to_column_rows(exp_data, exp_schema) if exp_schema else [list(r.values()) for r in exp_data]
    else:
        expected_rows = exp_data if isinstance(exp_data, list) else []

    # Build operations list from Sparkless operation type
    operations = _map_operation_to_robin(operation, sparkless, input_schema_names, exp_schema)

    name = fixture_name or sparkless.get("name") or operation or "converted"

    return {
        "name": name.replace(" ", "_").lower(),
        "pyspark_version": sparkless.get("pyspark_version", "3.5.0"),
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": operations,
        "expected": {"schema": exp_schema, "rows": expected_rows},
    }


def _map_operation_to_robin(
    operation: str,
    sparkless: dict,
    input_schema_names: list[str],
    exp_schema: list[dict],
) -> list[dict]:
    """Map Sparkless operation string and context to robin-sparkless operations list."""
    op_lower = operation.lower().strip()
    operations: list[dict] = []

    if "filter" in op_lower:
        # Default filter: use first numeric column > 0 or similar; caller can override via metadata
        filter_expr = sparkless.get("filter_expr") or "col('" + (input_schema_names[1] if len(input_schema_names) > 1 else input_schema_names[0]) + "') > 0"
        operations.append({"op": "filter", "expr": filter_expr})
    if "select" in op_lower:
        cols = sparkless.get("select_columns") or [s["name"] for s in exp_schema]
        operations.append({"op": "select", "columns": cols})
    if "groupby" in op_lower or "group_by" in op_lower:
        group_cols = sparkless.get("group_by_columns") or (input_schema_names[:1] if input_schema_names else [])
        agg_col = sparkless.get("agg_column")
        agg_func = sparkless.get("agg_func", "count")
        operations.append({"op": "groupBy", "columns": group_cols})
        if agg_func == "count":
            operations.append({"op": "agg", "aggregations": [{"func": "count", "alias": "count"}]})
        else:
            operations.append({"op": "agg", "aggregations": [{"func": agg_func, "alias": agg_func, "column": agg_col or input_schema_names[-1] if input_schema_names else ""}]})
    if "order" in op_lower or "orderby" in op_lower:
        order_cols = sparkless.get("order_by_columns") or [exp_schema[0]["name"]] if exp_schema else []
        operations.append({"op": "orderBy", "columns": order_cols, "ascending": [True] * len(order_cols)})

    if not operations:
        # Minimal: no op (just input -> expected)
        pass

    return operations


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert Sparkless expected_outputs to robin-sparkless fixtures")
    parser.add_argument("input_path", nargs="?", help="Path to a Sparkless JSON file")
    parser.add_argument("output_dir", nargs="?", default="tests/fixtures", help="Output directory for robin-sparkless fixtures")
    parser.add_argument("--batch", metavar="DIR", help="Convert all JSON files in DIR (Sparkless expected_outputs dir)")
    parser.add_argument("--name", help="Fixture name (default: from file or operation)")
    args = parser.parse_args()

    if args.batch:
        in_dir = Path(args.batch)
        if not in_dir.is_dir():
            print(f"Error: not a directory: {in_dir}", file=sys.stderr)
            return 1
        out_dir = Path(args.output_dir or "tests/fixtures")
        out_dir.mkdir(parents=True, exist_ok=True)
        count = 0
        for path in sorted(in_dir.glob("*.json")):
            try:
                with open(path) as f:
                    data = json.load(f)
                name = args.name or path.stem
                out = convert_sparkless_to_robin(data, fixture_name=name)
                out_path = out_dir / f"{out['name']}.json"
                with open(out_path, "w") as f:
                    json.dump(out, f, indent=2)
                count += 1
                print(f"Converted: {path.name} -> {out_path}")
            except Exception as e:
                print(f"Skip {path.name}: {e}", file=sys.stderr)
        print(f"Done: {count} fixtures written to {out_dir}")
        return 0

    if not args.input_path:
        parser.print_help()
        return 0

    in_path = Path(args.input_path)
    if not in_path.exists():
        print(f"Error: file not found: {in_path}", file=sys.stderr)
        return 1

    with open(in_path) as f:
        data = json.load(f)

    out = convert_sparkless_to_robin(data, fixture_name=args.name)
    out_dir = Path(args.output_dir or "tests/fixtures")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{out['name']}.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
