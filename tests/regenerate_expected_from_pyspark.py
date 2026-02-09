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

Reads each JSON fixture in the given directory (or list of paths), builds a
PySpark DataFrame from input.schema + input.rows, applies operations in order,
then overwrites the fixture's expected.schema and expected.rows with PySpark's
result. Hand-written fixtures under tests/fixtures/ (same operation format as
converted) are supported; use tests/fixtures to process only top-level JSON
files (converted/ and plans/ are not recursed). Fixtures with "skip": true are
skipped. Use --dry-run to print diffs without writing.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    try:
        from pyspark.errors.exceptions.base import PySparkRuntimeError
    except ImportError:
        PySparkRuntimeError = Exception  # type: ignore[assignment,misc]  # older PySpark
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
except ImportError:
    print(
        "Error: PySpark is required. Install with: pip install pyspark", file=sys.stderr
    )
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


def _cast_cell(value: Any, type_str: str) -> Any:
    """Cast a fixture value (e.g. date/timestamp string) for PySpark createDataFrame.
    Timestamps without explicit timezone are treated as UTC when session timeZone is UTC.
    """
    if value is None:
        return None
    t = (type_str or "string").lower()
    if t == "date" and isinstance(value, str):
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    if t in ("timestamp", "timestamp_ntz", "datetime") and isinstance(value, str):
        s = value.replace("Z", "+00:00")
        if "T" in s:
            if "+" in s or "-" in s.split("T")[-1][:1] == "-":
                return datetime.fromisoformat(s)
            dt = datetime.fromisoformat(s.replace("T", " "))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return value


def rows_to_pyspark_df(
    spark: SparkSession, schema: list[dict], rows: list[list[Any]]
) -> Any:
    """Create PySpark DataFrame from fixture schema + rows."""
    if not schema or not rows:
        return spark.createDataFrame([], schema_to_struct_type(schema or []))
    struct = schema_to_struct_type(schema)
    type_strs = [(c.get("type") or "string").lower() for c in schema]
    cast_rows = [
        [_cast_cell(v, type_strs[i]) for i, v in enumerate(row)] for row in rows
    ]
    return spark.createDataFrame(cast_rows, struct)


# Normalize PySpark simpleString() to fixture type names (e.g. long -> bigint)
SCHEMA_TYPE_NORMALIZE = {
    "long": "bigint",
    "integer": "int",
}


def schema_from_df(df: Any) -> list[dict]:
    """Extract fixture-format schema from PySpark DataFrame."""
    out = []
    for f in df.schema.fields:
        t = f.dataType.simpleString()
        out.append({"name": f.name, "type": SCHEMA_TYPE_NORMALIZE.get(t, t)})
    return out


def rows_from_df(df: Any) -> list[list[Any]]:
    """Collect DataFrame rows as list of lists (column order)."""
    return [list(r) for r in df.collect()]


def _json_serializable(obj: Any) -> Any:
    """Convert date/datetime in collected rows to strings for JSON."""
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_json_serializable(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _json_serializable(v) for k, v in obj.items()}
    return obj


def _withcolumn_globals() -> dict[str, Any]:
    """Namespace for eval() of withColumn/filter exprs in fixtures (hand-written and converted)."""
    return {
        "F": F,
        "col": F.col,
        "lit": F.lit,
        "split": F.split,
        "date_add": F.date_add,
        "date_sub": F.date_sub,
        "array_distinct": F.array_distinct,
        "size": F.size,
        "length": F.length,
        "lower": F.lower,
        "upper": F.upper,
        "trim": F.trim,
        "ltrim": F.ltrim,
        "rtrim": F.rtrim,
        "when": F.when,
        "coalesce": F.coalesce,
        "element_at": F.element_at,
        "array_contains": F.array_contains,
        "struct": F.struct,
        "to_date": F.to_date,
        "to_timestamp": F.to_timestamp,
        "date_format": F.date_format,
        "year": F.year,
        "month": F.month,
        "dayofmonth": F.dayofmonth,
        "dayofweek": F.dayofweek,
        "current_date": F.current_date,
        "current_timestamp": F.current_timestamp,
        "crc32": F.crc32,
        "concat_ws": F.concat_ws,
        "regexp_replace": F.regexp_replace,
        "substring": F.substring,
        "hash": F.hash,
        "rand": F.rand,
        "randn": F.randn,
        "bit_length": F.bit_length,
        "octet_length": F.octet_length,
        "levenshtein": F.levenshtein,
        "slice": F.slice,
        "explode": F.explode,
        "abs": F.abs,
        "sqrt": F.sqrt,
    }


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
                cond = eval(expr, _withcolumn_globals())
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
                order_cols = [
                    F.col(c).asc() if asc else F.col(c).desc()
                    for c, asc in zip(columns, ascending)
                ]
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
                elif func == "any_value" and col_name:
                    agg_exprs.append(F.first(col_name).alias(alias))
                elif func == "first" and col_name:
                    agg_exprs.append(F.first(col_name).alias(alias))
                elif func == "last" and col_name:
                    agg_exprs.append(F.last(col_name).alias(alias))
                elif func == "median" and col_name:
                    try:
                        agg_exprs.append(F.median(col_name).alias(alias))
                    except AttributeError:
                        agg_exprs.append(
                            F.expr(f"percentile_approx({col_name}, 0.5)").alias(alias)
                        )
                elif func == "product" and col_name:
                    agg_exprs.append(F.product(col_name).alias(alias))
                elif func in ("stddev", "stddev_samp") and col_name:
                    agg_exprs.append(F.stddev(col_name).alias(alias))
                elif func == "count_distinct" and col_name:
                    agg_exprs.append(F.countDistinct(col_name).alias(alias))
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
                df = df.withColumn(col_name, eval(expr, _withcolumn_globals()))
            except Exception:
                df = df.withColumn(col_name, F.expr(_expr_to_sql(expr)))
        elif op == "window":
            col_name = op_spec.get("column", "win_col")
            func = (op_spec.get("func") or "row_number").lower()
            partition_by = op_spec.get("partition_by", [])
            order_by_specs = op_spec.get("order_by") or []
            value_column = op_spec.get("value_column")
            n = op_spec.get("n")
            w = (
                Window.partitionBy(*partition_by)
                if partition_by
                else Window.partitionBy()
            )
            if order_by_specs:
                order_exprs = [
                    F.col(x["col"]).asc()
                    if x.get("asc", True)
                    else F.col(x["col"]).desc()
                    for x in order_by_specs
                ]
                w = w.orderBy(*order_exprs)
            else:
                w = w.orderBy(F.lit(0))
            if func == "row_number":
                df = df.withColumn(col_name, F.row_number().over(w))
            elif func == "rank":
                df = df.withColumn(col_name, F.rank().over(w))
            elif func == "dense_rank":
                df = df.withColumn(col_name, F.dense_rank().over(w))
            elif func == "lag":
                vcol = value_column or (df.columns[0] if df.columns else None)
                df = df.withColumn(col_name, F.lag(F.col(vcol), n or 1).over(w))
            elif func == "lead":
                vcol = value_column or (df.columns[0] if df.columns else None)
                df = df.withColumn(col_name, F.lead(F.col(vcol), n or 1).over(w))
            elif func == "first_value":
                vcol = value_column or (df.columns[0] if df.columns else None)
                df = df.withColumn(col_name, F.first(F.col(vcol)).over(w))
            elif func == "last_value":
                vcol = value_column or (df.columns[0] if df.columns else None)
                df = df.withColumn(col_name, F.last(F.col(vcol)).over(w))
            elif func == "nth_value":
                vcol = value_column or (df.columns[0] if df.columns else None)
                idx = int(n or 1)
                df = df.withColumn(col_name, F.nth_value(F.col(vcol), idx).over(w))
            elif func == "percent_rank":
                df = df.withColumn(col_name, F.percent_rank().over(w))
            elif func == "cume_dist":
                df = df.withColumn(col_name, F.cume_dist().over(w))
            elif func == "ntile":
                df = df.withColumn(col_name, F.ntile(int(n or 2)).over(w))
            elif func in ("sum", "avg", "count", "min", "max"):
                agg_col = op_spec.get("value_column") or (
                    df.columns[0] if df.columns else None
                )
                if agg_col is None:
                    continue
                agg_expr = getattr(F, func)(F.col(agg_col))
                df = df.withColumn(col_name, agg_expr.over(w))
            # else: unsupported window func, skip
        elif op == "union":
            if right_input:
                right_df = rows_to_pyspark_df(
                    spark, right_input.get("schema", []), right_input.get("rows", [])
                )
                df = df.union(right_df)
        elif op == "unionByName":
            if right_input:
                right_df = rows_to_pyspark_df(
                    spark, right_input.get("schema", []), right_input.get("rows", [])
                )
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
            # Strip surrounding quotes so 'pending' -> pending (match parity literal parsing)
            if (
                isinstance(old_val, str)
                and len(old_val) >= 2
                and (
                    (old_val.startswith("'") and old_val.endswith("'"))
                    or (old_val.startswith('"') and old_val.endswith('"'))
                )
            ):
                old_val = old_val[1:-1]
            if (
                isinstance(new_val, str)
                and len(new_val) >= 2
                and (
                    (new_val.startswith("'") and new_val.endswith("'"))
                    or (new_val.startswith('"') and new_val.endswith('"'))
                )
            ):
                new_val = new_val[1:-1]
            if col_name:
                df = df.replace(old_val, new_val, subset=[col_name])
        elif op == "crossJoin":
            if right_input:
                right_df = rows_to_pyspark_df(
                    spark, right_input.get("schema", []), right_input.get("rows", [])
                )
                df = df.crossJoin(right_df)
        elif op == "describe":
            df = df.describe()
        elif op == "summary":
            df = df.summary()
        elif op == "offset":
            n = int(op_spec.get("n", 0))
            if n > 0:
                w = Window.orderBy(F.monotonically_increasing_id())
                df = (
                    df.withColumn("_rn", F.row_number().over(w))
                    .filter(F.col("_rn") > n)
                    .drop("_rn")
                )
        elif op == "head":
            n = int(op_spec.get("n", 1))
            df = df.limit(n)
        elif op == "first":
            df = df.limit(1)
        # subtract, intersect: optional to add
    return df


def _expr_to_sql(expr: str) -> str:
    """Heuristic: turn col('x') > 30 into SQL-like "x > 30" for F.expr."""
    import re

    s = (
        expr.replace("col('", "")
        .replace("')", "")
        .replace('col("', "")
        .replace('")', "")
    )
    s = re.sub(r"\.gt\s*\(\s*lit\s*\(\s*(\d+)\s*\)\s*\)", r" > \1", s)
    s = re.sub(r"\.gt\s*\(\s*(\d+)\s*\)", r" > \1", s)
    s = re.sub(r"\.lt\s*\(\s*(\d+)\s*\)", r" < \1", s)
    s = re.sub(r"\.eq\s*\(\s*lit\s*", r" = ", s)
    return s if s.strip() else "1=1"


def process_fixture(
    spark: SparkSession, path: Path, dry_run: bool, include_skipped: bool = False
) -> bool:
    """Load fixture, run in PySpark, write updated expected. Return True on success."""
    with open(path) as f:
        fixture = json.load(f)
    if fixture.get("skip") and not include_skipped:
        return False
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

    old_expected = fixture.get("expected", {})
    if dry_run and old_expected:
        old_rows = old_expected.get("rows", [])
        old_schema = old_expected.get("schema", [])
        if old_schema != new_schema or old_rows != new_rows:
            print(
                f"  Would change {path.name}: schema {len(old_schema)}->{len(new_schema)} fields, "
                f"rows {len(old_rows)}->{len(new_rows)}"
            )
    fixture["expected"] = {
        "schema": new_schema,
        "rows": _json_serializable(new_rows),
    }
    if include_skipped and fixture.get("skip"):
        fixture.pop("skip", None)
        fixture.pop("skip_reason", None)
    if not dry_run:
        with open(path, "w") as f:
            json.dump(fixture, f, indent=2)
    return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Regenerate fixture expected sections from PySpark"
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["tests/fixtures/converted"],
        help="Directories (glob *.json) or JSON files. Default: tests/fixtures/converted. Use tests/fixtures for hand-written.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not write files; print diffs"
    )
    parser.add_argument(
        "--include-skipped",
        action="store_true",
        help="Process fixtures with skip:true (e.g. converted); remove skip on success.",
    )
    args = parser.parse_args()

    to_process: list[Path] = []
    for p in args.paths:
        path = Path(p)
        if path.is_file() and path.suffix.lower() == ".json":
            to_process.append(path)
        elif path.is_dir():
            # Only top-level *.json (no recurse into converted/ or plans/)
            for f in sorted(path.glob("*.json")):
                if f.is_file():
                    to_process.append(f)
        else:
            print(
                f"Warning: skipping (not a dir or .json file): {path}", file=sys.stderr
            )
    if not to_process:
        print("Error: no JSON files to process.", file=sys.stderr)
        return 1

    try:
        spark = (
            SparkSession.builder.appName("regenerate_expected")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except PySparkRuntimeError as e:
        err_msg = str(e)
        if (
            "JAVA_GATEWAY" in err_msg
            or "UnsupportedClassVersionError" in err_msg
            or "class file version" in err_msg.lower()
        ):
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
        if (
            "Java" in err_msg
            or "JAVA_GATEWAY" in err_msg
            or "class file version" in err_msg.lower()
        ):
            print(
                "Error: PySpark could not start the JVM. PySpark requires Java 17 or newer.",
                file=sys.stderr,
            )
            print(
                "  Set JAVA_HOME to a JDK 17+ installation (e.g. export JAVA_HOME=/path/to/jdk-17).",
                file=sys.stderr,
            )
        else:
            print(f"Error starting PySpark: {e}", file=sys.stderr)
        return 1

    count = 0
    for path in to_process:
        if process_fixture(
            spark, path, args.dry_run, include_skipped=args.include_skipped
        ):
            count += 1
            if not args.dry_run:
                print(f"  Updated: {path.name}")
    spark.stop()
    print(f"Done: {count} fixtures {'would be updated' if args.dry_run else 'updated'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
