"""
Tests for #412: DataFrame.cube() multiple columns (PySpark parity).

PySpark df.cube("dept", "year") and df.cube(*cols) accept multiple column names.
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_412").get_or_create()


def test_cube_two_columns_variadic() -> None:
    """df.cube("dept", "year").count() works with variadic args."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"dept": "A", "year": 2023}, {"dept": "B", "year": 2023}],
        [("dept", "str"), ("year", "int")],
    )
    out = (
        df.cube("dept", "year").agg([rs.count(rs.col("dept")).alias("count")]).collect()
    )
    assert len(out) >= 1
    assert all("count" in r for r in out)


def test_cube_single_list() -> None:
    """df.cube(["dept", "year"]) still works."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"dept": "A", "year": 2023}],
        [("dept", "str"), ("year", "int")],
    )
    out = (
        df.cube(["dept", "year"])
        .agg([rs.count(rs.col("dept")).alias("count")])
        .collect()
    )
    assert len(out) >= 1
