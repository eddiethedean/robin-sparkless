"""
Verify PySpark's behavior for to_timestamp(regexp_replace(col, r"\.\d+", "").cast("string"), format)
with both #168-style data (impression_date, many rows) and #153-style data (date_string, 3 rows).

Phase 1: Dump logical and optimized plan for both data styles (same expr, same column name) before collect.
Phase 2: Trace runtime (regexp_replace output only, then full pipeline).
"""

from datetime import datetime, timedelta

# Use PySpark directly so we're not tied to test fixtures
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def dump_plans(spark):
    """Dump logical and optimized plan for #168-style and #153-style DFs (same column 'value', same expr). No collect."""
    fmt = "yyyy-MM-dd'T'HH:mm:ss"
    expr = F.to_timestamp(
        F.regexp_replace(F.col("value"), r"\.\d+", "").cast("string"),
        fmt,
    )

    # #168-style: 3 rows, dynamic data
    data_dynamic = [
        {"id": i, "value": (datetime.now() - timedelta(hours=i)).isoformat()}
        for i in range(3)
    ]
    df_dynamic = spark.createDataFrame(data_dynamic, ["id", "value"]).withColumn(
        "parsed", expr
    )

    # #153-style: 3 rows, fixed strings
    data_fixed = [
        (1, "2024-01-15T10:30:45.123456"),
        (2, "2024-01-16T14:20:30.789012"),
        (3, "2024-01-17T09:15:22.456789"),
    ]
    df_fixed = spark.createDataFrame(data_fixed, ["id", "value"]).withColumn(
        "parsed", expr
    )

    # Trigger planning (no collect); get plan strings
    def plan_strings(df):
        qe = df._jdf.queryExecution()
        logical = qe.logical().toString()
        optimized = qe.optimizedPlan().toString()
        return logical, optimized

    logical_dyn, optimized_dyn = plan_strings(df_dynamic)
    logical_fix, optimized_fix = plan_strings(df_fixed)

    print("=== Phase 1: PySpark plans (before collect) ===\n")
    print("--- #168-style (dynamic data), logical plan ---")
    print(logical_dyn)
    print("\n--- #168-style (dynamic data), optimized plan ---")
    print(optimized_dyn)
    print("\n--- #153-style (fixed data), logical plan ---")
    print(logical_fix)
    print("\n--- #153-style (fixed data), optimized plan ---")
    print(optimized_fix)
    print("\n--- Plan comparison ---")
    print(f"  Logical plans identical: {logical_dyn == logical_fix}")
    print(f"  Optimized plans identical: {optimized_dyn == optimized_fix}")
    return logical_dyn == logical_fix, optimized_dyn == optimized_fix


def trace_runtime(spark):
    """Trace runtime: regexp_replace output only, then full to_timestamp. Compare dynamic vs fixed data."""
    fmt = "yyyy-MM-dd'T'HH:mm:ss"
    expr_replace_only = F.regexp_replace(F.col("value"), r"\.\d+", "").cast("string")
    expr_full = F.to_timestamp(expr_replace_only, fmt)

    # Dynamic data
    data_dynamic = [
        {"value": (datetime.now() - timedelta(hours=i)).isoformat()} for i in range(3)
    ]
    df_dyn = spark.createDataFrame(data_dynamic, ["value"])
    df_dyn_replaced = df_dyn.withColumn("after_regex", expr_replace_only)
    df_dyn_full = df_dyn.withColumn("parsed", expr_full)

    # Fixed data
    data_fixed = [
        {"value": "2024-01-15T10:30:45.123456"},
        {"value": "2024-01-16T14:20:30.789012"},
    ]
    df_fix = spark.createDataFrame(data_fixed, ["value"])
    df_fix_replaced = df_fix.withColumn("after_regex", expr_replace_only)
    df_fix_full = df_fix.withColumn("parsed", expr_full)

    print("\n=== Phase 2: Runtime trace ===\n")
    print("--- After regexp_replace only (dynamic) ---")
    for row in df_dyn_replaced.collect():
        v = row["after_regex"]
        print(f"  {v!r} (len={len(v)}, has_dot={'.' in v})")
    print("--- After regexp_replace only (fixed) ---")
    for row in df_fix_replaced.collect():
        v = row["after_regex"]
        print(f"  {v!r} (len={len(v)}, has_dot={'.' in v})")
    print("--- After to_timestamp (dynamic) ---")
    for row in df_dyn_full.collect():
        print(f"  parsed={row['parsed']!r}")
    print("--- After to_timestamp (fixed) ---")
    for row in df_fix_full.collect():
        print(f"  parsed={row['parsed']!r}")


def main():
    spark = SparkSession.builder.appName("verify_to_timestamp").getOrCreate()
    try:
        dump_plans(spark)
        trace_runtime(spark)

        fmt = "yyyy-MM-dd'T'HH:mm:ss"

        # --- #168 style: 150 rows, column "impression_date" (exact test setup) ---
        data_168 = [
            {
                "impression_id": f"IMP-{i:08d}",
                "impression_date": (
                    datetime.now() - timedelta(hours=i % 720)
                ).isoformat(),
            }
            for i in range(150)
        ]
        df_168 = spark.createDataFrame(data_168, ["impression_id", "impression_date"])
        expr_168 = F.to_timestamp(
            F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
            fmt,
        )
        df_168 = df_168.withColumn("parsed", expr_168)
        null_168 = df_168.filter(F.col("parsed").isNull()).count()
        non_null_168 = df_168.filter(F.col("parsed").isNotNull()).count()
        print("#168 style (impression_date, 150 rows, now()-timedelta):")
        print(f"  parsed null: {null_168}, non-null: {non_null_168}")

        # --- Same as #168 but only 3 rows to see if row count matters ---
        data_168_small = [
            {
                "impression_id": f"IMP-{i:08d}",
                "impression_date": (datetime.now() - timedelta(hours=i)).isoformat(),
            }
            for i in range(3)
        ]
        df_168_small = spark.createDataFrame(
            data_168_small, ["impression_id", "impression_date"]
        )
        df_168_small = df_168_small.withColumn("parsed", expr_168)
        null_168_small = df_168_small.filter(F.col("parsed").isNull()).count()
        non_null_168_small = df_168_small.filter(F.col("parsed").isNotNull()).count()
        print("#168 style but 3 rows only (impression_date, now()-timedelta):")
        print(f"  parsed null: {null_168_small}, non-null: {non_null_168_small}")

        # --- Swap: 3 rows, column "date_string" but now()-timedelta data (like #168) ---
        expr_date_string = F.to_timestamp(
            F.regexp_replace(F.col("date_string"), r"\.\d+", "").cast("string"),
            fmt,
        )
        data_swap = [
            {
                "id": f"imp_{i:03d}",
                "date_string": (datetime.now() - timedelta(hours=i)).isoformat(),
            }
            for i in range(3)
        ]
        df_swap = spark.createDataFrame(data_swap, ["id", "date_string"])
        df_swap = df_swap.withColumn("parsed", expr_date_string)
        null_swap = df_swap.filter(F.col("parsed").isNull()).count()
        non_null_swap = df_swap.filter(F.col("parsed").isNotNull()).count()
        print("Swap (3 rows, date_string column, now()-timedelta data):")
        print(f"  parsed null: {null_swap}, non-null: {non_null_swap}")

        # --- #153 style: 3 rows, column "date_string" (exact test setup) ---
        data_153 = [
            ("imp_001", "2024-01-15T10:30:45.123456"),
            ("imp_002", "2024-01-16T14:20:30.789012"),
            ("imp_003", "2024-01-17T09:15:22.456789"),
        ]
        df_153 = spark.createDataFrame(data_153, ["id", "date_string"])
        df_153 = df_153.withColumn("parsed", expr_date_string)
        null_153 = df_153.filter(F.col("parsed").isNull()).count()
        non_null_153 = df_153.filter(F.col("parsed").isNotNull()).count()
        print("#153 style (date_string-like, 3 fixed strings):")
        print(f"  parsed null: {null_153}, non-null: {non_null_153}")

        # Summary
        print("\nConclusion:")
        print(
            f"  #168 (150 rows, impression_date): null={null_168}, non_null={non_null_168}"
        )
        print(
            f"  #168 small (3 rows, impression_date): null={null_168_small}, non_null={non_null_168_small}"
        )
        print(
            f"  swap (3 rows, date_string, now()-timedelta): null={null_swap}, non_null={non_null_swap}"
        )
        print(
            f"  #153 (3 rows, date_string, fixed strings): null={null_153}, non_null={non_null_153}"
        )
        if null_swap == 0 and non_null_swap == 3:
            print(
                "  -> Column name drives behavior (impression_date => null, date_string => non-null)."
            )
        elif null_swap == 3:
            print(
                "  -> Data drives behavior (now()-timedelta => null, fixed strings => non-null)."
            )

        # Sample strings to compare
        import re

        pat = r"\.\d+"
        sample_dynamic = (datetime.now() - timedelta(hours=0)).isoformat()
        sample_fixed = "2024-01-15T10:30:45.123456"
        print("\nSample string comparison:")
        print(f"  now()-timedelta: {sample_dynamic!r} (len={len(sample_dynamic)})")
        print(f"  fixed:           {sample_fixed!r} (len={len(sample_fixed)})")
        print(
            f"  after regex strip: dynamic -> {repr(re.sub(pat, '', sample_dynamic))}; fixed -> {repr(re.sub(pat, '', sample_fixed))}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
