"""Repro for issue #211: astype/cast returns None instead of expected value."""

import robin_sparkless as rs


def test_cast_int_to_string_in_with_column() -> None:
    """Basic cast: int to string in with_column; collected value should be '1', not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame([{"num": 1}], [("num", "bigint")])
    result = df.with_column("num_str", rs.col("num").cast("string"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"


def test_cast_int_to_string_in_select() -> None:
    """Cast in select; collected value should be '1', not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame([{"num": 1}], [("num", "bigint")])
    result = df.select(rs.col("num").cast("string").alias("num_str"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"
