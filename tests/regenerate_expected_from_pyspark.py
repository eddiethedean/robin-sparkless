#!/usr/bin/env python3
"""
Regenerate the `expected` section of Robin Sparkless fixtures by running the same
input + operations in PySpark. Use this after converting Sparkless expected_outputs
so that parity is Robin vs PySpark (not Sparkless).

Usage:
  python tests/regenerate_expected_from_pyspark.py [tests/fixtures/converted]
  python tests/regenerate_expected_from_pyspark.py tests/fixtures/converted --dry-run

Requires: PySpark (pip install pyspark) and Java 17 or newer. PySpark uses the JVM;
if you see "UnsupportedClassVersionError" or "JAVA_GATEWAY_EXITED", set JAVA_HOME
to a JDK 17+ installation (e.g. export JAVA_HOME=/path/to/jdk-17).

Reads each JSON fixture in the given directory, builds a PySpark DataFrame from
input.schema + input.rows, applies operations in order, then overwrites the
fixture's expected.schema and expected.rows with PySpark's result.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    try:
        from pyspark.errors.exceptions.base import PySparkRuntimeError
    except ImportError:
        PySparkRuntimeError = Exception  # older PySpark
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError as e:
    print("Error: PySpark is required. Install with: pip install pyspark", file=sys.stderr)
    sys.exit(1)


# Map Robin fixture type names to PySpark types
TYPE_MAP = {
    "bigint": LongType(),
    "long": LongType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "float": DoubleType(),
    "string": StringType(),
    "boolean": BooleanType(),
    "bool": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamp_ntz": TimestampType(),
    "datetime": TimestampType(),
}


def schema_to_struct_type(schema: list[dict]) -> StructType:
    """Build PySpark StructType from fixture schema list [{name, type}, ...]."""
    fields = []
    for col_spec in schema:
        name = col_spec.get("name", "")
        type_str = (col_spec.get("type") or "string").lower()
        dtype = TYPE_MAP.get(type_str, StringType())
        fields.append(StructField(name, dtype, True))
    return StructType(fields)


def rows_to_pyspark_df(spark: SparkSession, schema: list[dict], rows: list[list[Any]]) -> Any:
    """Create PySpark DataFrame from fixture schema + rows."""
    if not schema or not rows:
        return spark.createDataFrame([], schema_to_struct_type(schema or []))
    struct = schema_to_struct_type(schema)
    return spark.createDataFrame(rows, struct)


def schema_from_df(df: Any) -> list[dict]:
    """Extract fixture-format schema from PySpark DataFrame."""
    return [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]


def rows_from_df(df: Any) -> list[list[Any]]:
    """Collect DataFrame rows as list of lists (column order)."""
    return [list(r) for r in df.collect()]


def apply_operations(
    spark: SparkSession,
    fixture: dict,
    df: Any,
) -> Any:
    """Apply fixture operations in order; return resulting DataFrame."""
    operations = fixture.get("operations", [])
    right_input = fixture.get("right_input")

    for op_spec in operations:
        op = op_spec.get("op", "")
        if op == "filter":
            expr = op_spec.get("expr", "true")
            try:
                # Fixture expr is often Python-style: col('age') > 30
                cond = eval(expr, {"F": F, "col": F.col, "lit": F.lit})
                df = df.filter(cond)
            except Exception:
                try:
                    df = df.filter(F.expr(_expr_to_sql(expr)))
                except Exception:
                    pass  # skip filter if we can't parse
        elif op == "select":
            cols = op_spec.get("columns", [])
            if cols:
                df = df.select(*cols)
        elif op == "orderBy":
            columns = op_spec.get("columns", [])
            ascending = op_spec.get("ascending", [True] * len(columns))
            if columns:
                order_cols = [F.col(c).asc() if asc else F.col(c).desc() for c, asc in zip(columns, ascending)]
                df = df.orderBy(*order_cols)
        elif op == "groupBy":
            cols = op_spec.get("columns", [])
            df = df.groupBy(*cols) if cols else df
        elif op == "agg":
            aggs = op_spec.get("aggregations", [])
            agg_exprs = []
            for a in aggs:
                func = (a.get("func") or "count").lower()
                alias = a.get("alias", func)
                col_name = a.get("column")
                if func == "count":
                    agg_exprs.append(F.count(F.lit(1)).alias(alias))
                elif func == "sum" and col_name:
                    agg_exprs.append(F.sum(col_name).alias(alias))
                elif func == "avg" and col_name:
                    agg_exprs.append(F.avg(col_name).alias(alias))
                elif func == "min" and col_name:
                    agg_exprs.append(F.min(col_name).alias(alias))
                elif func == "max" and col_name:
                    agg_exprs.append(F.max(col_name).alias(alias))
                else:
                    agg_exprs.append(F.count(F.lit(1)).alias(alias))
            if agg_exprs:
                df = df.agg(*agg_exprs)
        elif op == "join":
            if right_input is None:
                continue
            right_df = rows_to_pyspark_df(
                spark,
                right_input.get("schema", []),
                right_input.get("rows", []),
            )
            on_cols = op_spec.get("on", [])
            how = (op_spec.get("how") or "inner").lower()
            if on_cols:
                df = df.join(right_df, on_cols, how)
            else:
                df = df.join(right_df, how=how)
        elif op == "withColumn":
            col_name = op_spec.get("column", "computed")
            expr = op_spec.get("expr", "lit(0)")
            try:
                df = df.withColumn(col_name, eval(expr, {"F": F, "col": F.col, "lit": F.lit}))
            except Exception:
                df = df.withColumn(col_name, F.expr(_expr_to_sql(expr)))
        elif op == "union":
            if right_input:
                right_df = rows_to_pyspark_df(spark, right_input.get("schema", []), right_input.get("rows", []))
                df = df.union(right_df)
        elif op == "unionByName":
            if right_input:
                right_df = rows_to_pyspark_df(spark, right_input.get("schema", []), right_input.get("rows", []))
                df = df.unionByName(right_df)
        elif op == "distinct":
            subset = op_spec.get("subset")
            df = df.distinct() if not subset else df.dropDuplicates(subset)
        elif op == "drop":
            cols = op_spec.get("columns", [])
            if isinstance(cols, str):
                cols = [cols]
            if cols:
                df = df.drop(*cols)
        elif op == "dropna":
            subset = op_spec.get("subset")
            df = df.dropna(subset=subset) if subset else df.dropna()
        elif op == "fillna":
            value = op_spec.get("value", 0)
            df = df.fillna(value)
        elif op == "limit":
            n = int(op_spec.get("n", 10))
            df = df.limit(n)
        elif op == "withColumnRenamed":
            existing = op_spec.get("existing", "")
            new = op_spec.get("new", "")
            if existing and new:
                df = df.withColumnRenamed(existing, new)
        elif op == "replace":
            col_name = op_spec.get("column", "")
            old_val = op_spec.get("old_value", "")
            new_val = op_spec.get("new_value", "")
            if col_name:
                df = df.replace(old_val, new_val, subset=[col_name])
        elif op == "crossJoin":
            if right_input:
                right_df = rows_to_pyspark_df(spark, right_input.get("schema", []), right_input.get("rows", []))
                df = df.crossJoin(right_df)
        elif op == "describe":
            df = df.describe()
        elif op == "offset":
            n = int(op_spec.get("n", 0))
            if n > 0:
                from pyspark.sql.window import Window
                w = Window.orderBy(F.monotonically_increasing_id())
                df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") > n).drop("_rn")
        elif op == "head":
            n = int(op_spec.get("n", 1))
            df = df.limit(n)
        # first, summary, subtract, intersect: optional to add
    return df


def _expr_to_sql(expr: str) -> str:
    """Heuristic: turn col('x') > 30 into SQL-like "x > 30" for F.expr."""
    import re
    s = expr.replace("col('", "").replace("')", "").replace('col("', "").replace('")', "")
    s = re.sub(r"\.gt\s*\(\s*lit\s*\(\s*(\d+)\s*\)\s*\)", r" > \1", s)
    s = re.sub(r"\.gt\s*\(\s*(\d+)\s*\)", r" > \1", s)
    s = re.sub(r"\.lt\s*\(\s*(\d+)\s*\)", r" < \1", s)
    s = re.sub(r"\.eq\s*\(\s*lit\s*", r" = ", s)
    return s if s.strip() else "1=1"


def process_fixture(spark: SparkSession, path: Path, dry_run: bool) -> bool:
    """Load fixture, run in PySpark, write updated expected. Return True on success."""
    with open(path) as f:
        fixture = json.load(f)
    input_section = fixture.get("input", {})
    schema = input_section.get("schema", [])
    rows = input_section.get("rows", [])

    try:
        df = rows_to_pyspark_df(spark, schema, rows)
        df = apply_operations(spark, fixture, df)
        new_schema = schema_from_df(df)
        new_rows = rows_from_df(df)
    except Exception as e:
        print(f"  Error running PySpark for {path.name}: {e}", file=sys.stderr)
        return False

    fixture["expected"] = {"schema": new_schema, "rows": new_rows}
    if not dry_run:
        with open(path, "w") as f:
            json.dump(fixture, f, indent=2)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate fixture expected sections from PySpark"
    )
    parser.add_argument(
        "dir",
        nargs="?",
        default="tests/fixtures/converted",
        help="Directory containing Robin-format JSON fixtures",
    )
    parser.add_argument("--dry-run", action="store_true", help="Do not write files")
    args = parser.parse_args()

    dir_path = Path(args.dir)
    if not dir_path.is_dir():
        print(f"Error: not a directory: {dir_path}", file=sys.stderr)
        return 1

    try:
        spark = SparkSession.builder.appName("regenerate_expected").getOrCreate()
    except PySparkRuntimeError as e:
        err_msg = str(e)
        if "JAVA_GATEWAY" in err_msg or "UnsupportedClassVersionError" in err_msg or "class file version" in err_msg.lower():
            print(
                "Error: PySpark could not start the JVM. PySpark requires Java 17 or newer.",
                file=sys.stderr,
            )
            print(
                "Set JAVA_HOME to a JDK 17+ installation, e.g.:",
                file=sys.stderr,
            )
            print("  export JAVA_HOME=/path/to/jdk-17", file=sys.stderr)
            print("Then run this script again.", file=sys.stderr)
        else:
            print(f"Error starting PySpark: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        err_msg = str(e)
        if "Java" in err_msg or "JAVA_GATEWAY" in err_msg or "class file version" in err_msg.lower():
            print(
                "Error: PySpark could not start the JVM. PySpark requires Java 17 or newer.",
                file=sys.stderr,
            )
            print("  Set JAVA_HOME to a JDK 17+ installation (e.g. export JAVA_HOME=/path/to/jdk-17).", file=sys.stderr)
        else:
            print(f"Error starting PySpark: {e}", file=sys.stderr)
        return 1

    count = 0
    for path in sorted(dir_path.glob("*.json")):
        if path.is_file():
            if process_fixture(spark, path, args.dry_run):
                count += 1
                print(f"  {'Would update' if args.dry_run else 'Updated'}: {path.name}")
    spark.stop()
    print(f"Done: {count} fixtures {'would be updated' if args.dry_run else 'updated'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
