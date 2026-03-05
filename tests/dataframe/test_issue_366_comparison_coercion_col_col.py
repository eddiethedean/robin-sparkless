"""
Tests for #366: Comparison coercion string/numeric and date/string (column-to-column).

PySpark allows col("id") == col("label") when id is int and label is string;
it coerces (string parsed to number). Robin-sparkless previously raised
"cannot compare string with numeric type". Now column-to-column comparisons
in filter use coerce_for_pyspark_comparison when types differ.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark

F = get_functions()


def test_col_col_string_numeric_issue_repro() -> None:
    """col('id') == col('label') where id is int, label is string (issue repro)."""
    spark = get_spark("issue_366")
    df = spark.createDataFrame(
        [{"id": 1, "label": "1"}],
        ["id", "label"],
    )
    rows = df.filter(F.col("id") == F.col("label")).collect()
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert rows[0]["label"] == "1"


def test_col_col_string_numeric_no_match() -> None:
    """col('n') == col('s') when s is non-numeric string: no match."""
    spark = get_spark("issue_366")
    df = spark.createDataFrame(
        [{"n": 42, "s": "abc"}, {"n": 1, "s": "1"}],
        ["n", "s"],
    )
    rows = df.filter(F.col("n") == F.col("s")).collect()
    assert len(rows) == 1
    assert rows[0]["n"] == 1
    assert rows[0]["s"] == "1"


def test_col_col_numeric_string_reversed() -> None:
    """col('label') == col('id') (string col first) also coerces."""
    spark = get_spark("issue_366")
    df = spark.createDataFrame(
        [{"id": 99, "label": "99"}],
        ["id", "label"],
    )
    rows = df.filter(F.col("label") == F.col("id")).collect()
    assert len(rows) == 1
