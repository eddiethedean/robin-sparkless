"""
Tests for #366: Comparison coercion string/numeric and date/string (column-to-column).

PySpark allows col("id") == col("label") when id is int and label is string;
it coerces (string parsed to number). Robin-sparkless previously raised
"cannot compare string with numeric type". Now column-to-column comparisons
in filter use coerce_for_pyspark_comparison when types differ.
"""

from __future__ import annotations


def test_col_col_string_numeric_issue_repro() -> None:
    """col('id') == col('label') where id is int, label is string (issue repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_366").get_or_create()
    df = spark.createDataFrame(
        [{"id": 1, "label": "1"}],
        [("id", "int"), ("label", "string")],
    )
    rows = df.filter(rs.col("id") == rs.col("label")).collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["label"] == "1"


def test_col_col_string_numeric_no_match() -> None:
    """col('n') == col('s') when s is non-numeric string: no match."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_366").get_or_create()
    df = spark.createDataFrame(
        [{"n": 42, "s": "abc"}, {"n": 1, "s": "1"}],
        [("n", "int"), ("s", "string")],
    )
    rows = df.filter(rs.col("n") == rs.col("s")).collect()
    assert len(rows) == 1
    assert rows[0]["n"] == 1
    assert rows[0]["s"] == "1"


def test_col_col_numeric_string_reversed() -> None:
    """col('label') == col('id') (string col first) also coerces."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_366").get_or_create()
    df = spark.createDataFrame(
        [{"id": 99, "label": "99"}],
        [("id", "int"), ("label", "string")],
    )
    rows = df.filter(rs.col("label") == rs.col("id")).collect()
    assert len(rows) == 1
