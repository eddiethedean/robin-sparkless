#!/usr/bin/env python3
"""Run documentation examples and print their real output.

Use this to verify doc output blocks match actual execution.
Run from repo root: python scripts/run_doc_examples.py
"""

import sys

try:
    import robin_sparkless as rs
except ImportError:
    print("Install robin-sparkless first: maturin develop --features pyo3,sql,delta")
    sys.exit(1)


def main() -> None:
    print("=== README / README-Python: Python quickstart ===\n")
    spark = rs.SparkSession.builder().app_name("demo").get_or_create()
    df = spark.createDataFrame(
        [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
        ["id", "age", "name"],
    )
    filtered = df.filter(rs.col("age") > rs.lit(26))
    result = filtered.collect()
    print("print(filtered.collect())")
    print(result)
    print()

    print("=== USER_GUIDE: filter (age > 25) ===\n")
    adults = df.filter(rs.col("age") > rs.lit(25))
    rows = adults.collect()
    print("adults.collect() ->", rows)
    print()

    print("=== USER_GUIDE: when/then/otherwise ===\n")
    df2 = spark.createDataFrame(
        [(1, 10, "a"), (2, 25, "b"), (3, 70, "c")], ["id", "age", "name"]
    )
    df2 = df2.with_column(
        "category",
        rs.when(rs.col("age") >= 65)
        .then(rs.lit("senior"))
        .otherwise(
            rs.when(rs.col("age") >= 18)
            .then(rs.lit("adult"))
            .otherwise(rs.lit("minor"))
        ),
    )
    rows2 = df2.collect()
    print("category column:", [r["category"] for r in rows2])
    print()

    print("All Python doc examples ran successfully.")


if __name__ == "__main__":
    main()
