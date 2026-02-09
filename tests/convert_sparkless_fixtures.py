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
  python tests/convert_sparkless_fixtures.py --batch <dir> tests/fixtures --output-subdir converted --dedupe

When Sparkless repo is available, run from robin-sparkless root:
  python tests/convert_sparkless_fixtures.py --batch /path/to/sparkless/tests/expected_outputs tests/fixtures --output-subdir converted
  Use --dedupe to skip converting Sparkless files whose fixture name already exists in tests/fixtures/*.json (avoid duplicate scenarios).

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
    "date": "date",
    "timestamp": "timestamp",
    "timestamp_ntz": "timestamp",
    "datetime": "timestamp",
}


def sparkless_schema_to_robin(schema: dict) -> list[dict[str, str]]:
    """Convert Sparkless schema to robin-sparkless schema array."""
    if "fields" in schema and schema["fields"]:
        return [
            {
                "name": f.get("name", ""),
                "type": SPARKLESS_TYPE_TO_ROBIN.get(
                    f.get("type", "string").lower(), f.get("type", "string").lower()
                ),
            }
            for f in schema["fields"]
        ]
    if "field_names" in schema and "field_types" in schema:
        names = schema["field_names"]
        types = schema["field_types"]
        return [
            {
                "name": names[i],
                "type": SPARKLESS_TYPE_TO_ROBIN.get(
                    str(types[i]).lower(), str(types[i])
                ),
            }
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

    input_rows = (
        dict_rows_to_column_rows(input_data, input_schema) if input_schema else []
    )

    # Expected schema and rows
    exp_schema_raw = expected_output.get("schema", {})
    exp_schema = (
        sparkless_schema_to_robin(exp_schema_raw)
        if isinstance(exp_schema_raw, dict)
        else (exp_schema_raw or [])
    )
    exp_data = expected_output.get("data", [])
    if exp_data and isinstance(exp_data[0], dict):
        expected_rows = (
            dict_rows_to_column_rows(exp_data, exp_schema)
            if exp_schema
            else [list(r.values()) for r in exp_data]
        )
    else:
        expected_rows = exp_data if isinstance(exp_data, list) else []

    # Build operations list from Sparkless operation type
    operations = _map_operation_to_robin(
        operation, sparkless, input_schema_names, exp_schema
    )

    # Optional right_input for join / union / unionByName
    right_input = _build_right_input(sparkless)

    name = fixture_name or sparkless.get("name") or operation or "converted"

    out: dict[str, Any] = {
        "name": name.replace(" ", "_").lower(),
        "pyspark_version": sparkless.get("pyspark_version", "3.5.0"),
        "input": {"schema": input_schema, "rows": input_rows},
        "operations": operations,
        "expected": {"schema": exp_schema, "rows": expected_rows},
    }
    if right_input is not None:
        out["right_input"] = right_input
    return out


def _build_right_input(sparkless: dict) -> dict | None:
    """Build right_input section from Sparkless right_input_data / second_input_data."""
    right_data = (
        sparkless.get("right_input_data")
        or sparkless.get("second_input_data")
        or sparkless.get("right_input")
    )
    if not right_data or not isinstance(right_data, list):
        return None
    if not right_data:
        return {"schema": [], "rows": []}
    names = list(right_data[0].keys()) if right_data else []
    right_schema = [{"name": n, "type": "string"} for n in names]
    right_rows = dict_rows_to_column_rows(right_data, right_schema)
    return {"schema": right_schema, "rows": right_rows}


# Column names typically used for numeric comparisons (avoid col('name')>0 when name is string)
NUMERIC_COLUMN_NAMES = frozenset(
    {"id", "age", "salary", "amount", "value", "count", "score", "num", "qty"}
)

# Alias mapping: output column -> input column (for select_with_alias)
ALIAS_MAPPING = {"user_id": "id", "full_name": "name"}


def _pick_numeric_filter_column(input_schema_names: list[str]) -> str:
    """Pick first column that looks numeric for filter expr (avoid string columns)."""
    for n in input_schema_names:
        if n.lower() in NUMERIC_COLUMN_NAMES:
            return n
    return input_schema_names[0] if input_schema_names else "id"


def _map_operation_to_robin(
    operation: str,
    sparkless: dict,
    input_schema_names: list[str],
    exp_schema: list[dict],
) -> list[dict]:
    """Map Sparkless operation string and context to robin-sparkless operations list.
    Order: filter -> groupBy -> agg -> select -> orderBy (never select before groupBy).
    """
    op_lower = operation.lower().strip().replace("-", "_").replace(" ", "_")
    filter_op: dict | None = None
    groupby_op: dict | None = None
    agg_op: dict | None = None
    select_op: dict | None = None
    orderby_op: dict | None = None
    join_op: dict | None = None
    window_op: dict | None = None
    withcolumn_ops: list[dict] = []
    other_ops: list[dict] = []

    if "filter" in op_lower:
        filter_expr = sparkless.get("filter_expr")
        if not filter_expr:
            col_name = _pick_numeric_filter_column(input_schema_names)
            filter_expr = f"col('{col_name}') > 0"
        filter_op = {"op": "filter", "expr": filter_expr}
    if "select" in op_lower:
        cols = sparkless.get("select_columns") or [s["name"] for s in exp_schema]
        select_op = {"op": "select", "columns": cols}
    if "groupby" in op_lower or "group_by" in op_lower:
        group_cols = sparkless.get("group_by_columns") or (
            input_schema_names[:1] if input_schema_names else []
        )
        agg_col = sparkless.get("agg_column")
        agg_func = sparkless.get("agg_func", "count")
        groupby_op = {"op": "groupBy", "columns": group_cols}
        if agg_func == "count":
            agg_op = {
                "op": "agg",
                "aggregations": [{"func": "count", "alias": "count"}],
            }
        else:
            agg_op = {
                "op": "agg",
                "aggregations": [
                    {
                        "func": agg_func,
                        "alias": agg_func,
                        "column": agg_col
                        or (input_schema_names[-1] if input_schema_names else ""),
                    }
                ],
            }
    if "order" in op_lower or "orderby" in op_lower:
        order_cols = (
            sparkless.get("order_by_columns") or [exp_schema[0]["name"]]
            if exp_schema
            else []
        )
        orderby_op = {
            "op": "orderBy",
            "columns": order_cols,
            "ascending": [True] * len(order_cols),
        }

    # Select_with_alias: output cols (e.g. user_id, full_name) not in input -> add withColumn aliases
    if select_op and groupby_op is None:
        input_set = set(input_schema_names)
        for out_col in select_op.get("columns", []):
            if out_col not in input_set and out_col in ALIAS_MAPPING:
                src = ALIAS_MAPPING[out_col]
                if src in input_set:
                    withcolumn_ops.append(
                        {"op": "withColumn", "column": out_col, "expr": f"col('{src}')"}
                    )

    # Join: needs right_input from _build_right_input; emit join op
    if "join" in op_lower:
        on = (
            sparkless.get("join_on")
            or sparkless.get("on")
            or (input_schema_names[:1] if input_schema_names else [])
        )
        how = (sparkless.get("join_how") or sparkless.get("how") or "inner").lower()
        if how not in ("inner", "left", "right", "outer"):
            how = "inner"
        join_op = {"op": "join", "on": on if isinstance(on, list) else [on], "how": how}

    # Window: partition_by, order_by, func, value_column (for lag/lead)
    if "window" in op_lower:
        part = (
            sparkless.get("partition_by")
            or sparkless.get("partition_cols")
            or (input_schema_names[:1] if input_schema_names else [])
        )
        order = sparkless.get("order_by") or sparkless.get("order_cols")
        if isinstance(order, list) and order and isinstance(order[0], dict):
            order_specs = [
                {"col": o.get("col", o.get("column", "")), "asc": o.get("asc", True)}
                for o in order
            ]
        elif isinstance(order, list):
            order_specs = [{"col": c, "asc": True} for c in order]
        else:
            order_specs = []
        func = (
            sparkless.get("window_func") or sparkless.get("func") or "row_number"
        ).lower()
        value_col = sparkless.get("value_column") or (
            input_schema_names[-1] if input_schema_names else None
        )
        op_payload: dict[str, Any] = {
            "op": "window",
            "column": sparkless.get("output_column") or "rn",
            "func": func,
            "partition_by": part if isinstance(part, list) else [part],
            "order_by": order_specs,
        }
        if value_col is not None:
            op_payload["value_column"] = value_col
        window_op = op_payload

    # WithColumn / transformations
    if (
        "with_column" in op_lower
        or "withcolumn" in op_lower
        or "transformation" in op_lower
    ):
        col_name = (
            sparkless.get("with_column_name")
            or sparkless.get("column_name")
            or "computed"
        )
        expr = (
            sparkless.get("with_column_expr")
            or sparkless.get("expr")
            or "col('" + (input_schema_names[0] if input_schema_names else "id") + "')"
        )
        withcolumn_ops.append({"op": "withColumn", "column": col_name, "expr": expr})

    # Union / unionAll
    if "union" in op_lower and "name" not in op_lower:
        other_ops.append({"op": "union"})
    if "union_by_name" in op_lower or "unionbyname" in op_lower:
        other_ops.append({"op": "unionByName"})

    # Distinct
    if "distinct" in op_lower or "drop_duplicate" in op_lower:
        subset = sparkless.get("subset") or sparkless.get("columns")
        other_ops.append(
            {"op": "distinct", "subset": subset if isinstance(subset, list) else None}
        )

    # Drop
    if (
        "drop" in op_lower
        and "dropna" not in op_lower
        and "drop_duplicate" not in op_lower
    ):
        cols = sparkless.get("columns") or sparkless.get("drop_columns") or []
        other_ops.append(
            {"op": "drop", "columns": cols if isinstance(cols, list) else [cols]}
        )

    # Dropna
    if "dropna" in op_lower or "drop_null" in op_lower:
        subset = sparkless.get("subset") or sparkless.get("columns")
        other_ops.append(
            {"op": "dropna", "subset": subset if isinstance(subset, list) else None}
        )

    # Fillna
    if "fillna" in op_lower or "fill_null" in op_lower:
        value = sparkless.get("value") or sparkless.get("fill_value") or 0
        other_ops.append({"op": "fillna", "value": value})

    # Limit
    if "limit" in op_lower or "head" in op_lower:
        n = sparkless.get("n") or sparkless.get("limit") or 10
        other_ops.append({"op": "limit", "n": int(n)})

    # WithColumnRenamed
    if (
        "with_column_renamed" in op_lower
        or "withcolumnrenamed" in op_lower
        or "rename" in op_lower
    ):
        existing = (
            sparkless.get("existing")
            or sparkless.get("old_name")
            or (input_schema_names[0] if input_schema_names else "old")
        )
        new = sparkless.get("new") or sparkless.get("new_name") or "new"
        other_ops.append({"op": "withColumnRenamed", "existing": existing, "new": new})

    # Build final operations list: filter -> withColumn (aliases) -> groupBy -> agg -> select -> orderBy -> join -> window -> other
    operations: list[dict] = []
    if filter_op:
        operations.append(filter_op)
    operations.extend(withcolumn_ops)
    if groupby_op:
        operations.append(groupby_op)
    if agg_op:
        operations.append(agg_op)
    # Select after groupBy when both present; never emit select before groupBy
    if select_op:
        operations.append(select_op)
    if orderby_op:
        operations.append(orderby_op)
    if join_op:
        operations.append(join_op)
    if window_op:
        operations.append(window_op)
    operations.extend(other_ops)

    return operations


def main() -> int:
    """CLI: convert one Sparkless JSON or --batch a directory; write robin-sparkless fixtures. Return 0 on success."""
    parser = argparse.ArgumentParser(
        description="Convert Sparkless expected_outputs to robin-sparkless fixtures"
    )
    parser.add_argument("input_path", nargs="?", help="Path to a Sparkless JSON file")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="tests/fixtures",
        help="Output directory for robin-sparkless fixtures",
    )
    parser.add_argument(
        "--batch",
        metavar="DIR",
        help="Convert all JSON files in DIR (Sparkless expected_outputs dir)",
    )
    parser.add_argument(
        "--output-subdir",
        metavar="DIR",
        help="When using --batch, write into output_dir/DIR (e.g. converted)",
    )
    parser.add_argument("--name", help="Fixture name (default: from file or operation)")
    parser.add_argument(
        "--dedupe",
        action="store_true",
        help="When using --batch: skip Sparkless files whose converted fixture name already exists in output_dir/*.json (avoid duplicating hand-written fixtures)",
    )
    args = parser.parse_args()

    if args.batch:
        in_dir = Path(args.batch)
        if not in_dir.is_dir():
            print(f"Error: not a directory: {in_dir}", file=sys.stderr)
            return 1
        base_out = Path(args.output_dir or "tests/fixtures")
        out_dir = base_out / args.output_subdir if args.output_subdir else base_out
        out_dir.mkdir(parents=True, exist_ok=True)

        # When --dedupe: existing names = all .json in base_out (hand-written fixtures), not in subdirs
        existing_names: set[str] = set()
        if args.dedupe and base_out.is_dir():
            for p in base_out.glob("*.json"):
                if p.is_file():
                    existing_names.add(p.stem)

        count = 0
        skipped_dup = 0
        for path in sorted(in_dir.glob("*.json")):
            try:
                with open(path) as f:
                    data = json.load(f)
                name = args.name or path.stem
                out = convert_sparkless_to_robin(data, fixture_name=name)
                fixture_name = out["name"]
                if args.dedupe and fixture_name in existing_names:
                    print(
                        f"Skipped (duplicate): {path.name} -> already have fixture '{fixture_name}'"
                    )
                    skipped_dup += 1
                    continue
                out_path = out_dir / f"{fixture_name}.json"
                with open(out_path, "w") as f:
                    json.dump(out, f, indent=2)
                count += 1
                existing_names.add(
                    fixture_name
                )  # avoid writing same name again in same batch
                print(f"Converted: {path.name} -> {out_path}")
            except Exception as e:
                print(f"Skip {path.name}: {e}", file=sys.stderr)
        print(
            f"Done: {count} fixtures written to {out_dir}"
            + (f"; {skipped_dup} skipped (duplicate)" if skipped_dup else "")
        )
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
