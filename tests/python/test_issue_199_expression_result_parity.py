"""Regression tests for issue #199: Other expression or result parity (Sparkless).

Parent issue for astype/cast and expression parity; child issues #211, #214–#220
address specific failure categories. This file covers the representative example
from #199 and related basic cast/collect behavior.
"""

import robin_sparkless as rs


def test_astype_cast_returns_expected_value_not_none() -> None:
    """Exact scenario from #199: cast to string in with_column must return '1', not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"num": 1}], [("num", "bigint")])
    result = df.with_column("num_str", rs.col("num").cast("string"))
    rows = result.collect()
    assert rows[0]["num_str"] == "1"
    assert rows[0]["num"] == 1


def test_basic_astype_int_in_collect() -> None:
    """#199 affected: cast to int in with_column; collect must return int, not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"s": "123"}], [("s", "string")])
    result = df.with_column("n", rs.col("s").cast("int"))
    rows = result.collect()
    assert rows[0]["n"] == 123
    assert rows[0]["s"] == "123"


def test_basic_astype_multiple_chained() -> None:
    """#199 affected: chained cast (e.g. int -> string); result must be string, not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"x": 123}], [("x", "bigint")])
    result = df.with_column("s", rs.col("x").cast("string"))
    rows = result.collect()
    assert rows[0]["s"] == "123"
