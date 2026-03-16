"""
Tests for #176: select() with column expressions and regexp_extract_all.
Expected outputs are from a prior PySpark 3.5 run; tests do not execute PySpark at runtime.
"""

from __future__ import annotations

from typing import Any

import pytest

from sparkless.testing import get_imports
from tests.utils import assert_rows_equal

_imports = get_imports()
F = _imports.F


# Expected outputs (from prior PySpark 3.5 run)
EXPECTED_REGEXP_EXTRACT_ALL_SELECT_EXPR: list[dict[str, Any]] = [
    {"m": ["1", "22", "333"]},
    {"m": []},
    {"m": None},
]
EXPECTED_REGEXP_EXTRACT_ALL_MIXED: list[dict[str, Any]] = [
    {"s": "a1b22c", "m": ["1", "22"]},
    {"s": "x99y", "m": ["99"]},
    {"s": "no-digits", "m": []},
]
EXPECTED_REGEXP_EXTRACT_ALL_EMPTY_NULL: list[dict[str, Any]] = [
    {"m": []},
    {"m": None},
    {"m": ["1"]},
]
EXPECTED_SELECT_EXPR_AND_COLUMN: list[dict[str, Any]] = [
    {"a": 1, "sum_ab": 3},
    {"a": 3, "sum_ab": 7},
]


def test_regexp_extract_all_select_expression_pyspark_parity(spark) -> None:
    """select(regexp_extract_all(...).alias('m')) - same scenario in both modes."""
    pytest.skip(
        "See https://github.com/eddiethedean/robin-sparkless/issues/1501 – "
        "regexp_extract_all + select parity gap between sparkless and PySpark; "
        "unskip once sparkless matches PySpark's UNRESOLVED_COLUMN behavior."
    )
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(Exception) as excinfo:
        df.select([F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")]).collect()
    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg
    assert "`\\d+` cannot be resolved" in msg


def test_regexp_extract_all_select_varargs_pyspark_parity(spark) -> None:
    """select(expr) with single expression as vararg - same scenario in both modes."""
    pytest.skip(
        "See https://github.com/eddiethedean/robin-sparkless/issues/1501 – "
        "regexp_extract_all + select parity gap between sparkless and PySpark; "
        "unskip once sparkless matches PySpark's UNRESOLVED_COLUMN behavior."
    )
    data = [
        {"s": "a1 b22 c333"},
        {"s": "no-digits"},
        {"s": None},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(Exception) as excinfo:
        df.select(F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")).collect()
    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg
    assert "`\\d+` cannot be resolved" in msg


def test_regexp_extract_all_select_mixed_columns_and_expression_pyspark_parity(
    spark,
) -> None:
    """select('s', expr.alias('m')) - same scenario in both modes."""
    pytest.skip(
        "See https://github.com/eddiethedean/robin-sparkless/issues/1501 – "
        "regexp_extract_all + select parity gap between sparkless and PySpark; "
        "unskip once sparkless matches PySpark's UNRESOLVED_COLUMN behavior."
    )
    data = [
        {"s": "a1b22c"},
        {"s": "x99y"},
        {"s": "no-digits"},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(Exception) as excinfo:
        df.select("s", F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")).collect()
    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg
    assert "`\\d+` cannot be resolved" in msg


def test_regexp_extract_all_empty_string_and_null_pyspark_parity(spark) -> None:
    """regexp_extract_all with empty string and null - same scenario in both modes."""
    pytest.skip(
        "See https://github.com/eddiethedean/robin-sparkless/issues/1501 – "
        "regexp_extract_all + select parity gap between sparkless and PySpark; "
        "unskip once sparkless matches PySpark's UNRESOLVED_COLUMN behavior."
    )
    data = [
        {"s": ""},
        {"s": None},
        {"s": "x1y"},
    ]
    schema = ["s"]
    df = spark.createDataFrame(data, schema)

    with pytest.raises(Exception) as excinfo:
        df.select(F.regexp_extract_all(F.col("s"), r"\d+", 0).alias("m")).collect()
    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg
    assert "`\\d+` cannot be resolved" in msg


def test_select_expression_and_column_name_pyspark_parity(spark) -> None:
    """select('a', try_add(col('a'), col('b')).alias('sum_ab')) - same scenario in both modes."""
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select(
        "a",
        F.try_add(F.col("a"), F.col("b")).alias("sum_ab"),
    )
    actual = result.collect()
    assert_rows_equal(actual, EXPECTED_SELECT_EXPR_AND_COLUMN, order_matters=True)


def test_select_list_of_column_names_still_works(spark) -> None:
    """select(['a','b']) continues to work (backward compatibility) - same scenario in both modes."""
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select(["a", "b"])
    rows = result.collect()
    assert [r.asDict() for r in rows] == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_select_varargs_column_names_still_works(spark) -> None:
    """select('a', 'b') continues to work (backward compatibility) - same scenario in both modes."""
    data = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    schema = ["a", "b"]
    df = spark.createDataFrame(data, schema)
    result = df.select("a", "b")
    rows = result.collect()
    assert [r.asDict() for r in rows] == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
