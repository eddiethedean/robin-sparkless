"""Tests for issue #213: duplicate column names in select.

Polars raises 'duplicate: the name X is duplicate' when select produces columns
with the same name. We disambiguate with _1, _2, ... (PySpark/Sparkless parity).
"""

import robin_sparkless as rs


def test_select_same_column_cast_twice() -> None:
    """Select same column cast to different types (duplicate output names) should not error."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"num": 1}, {"num": 2}],
        [("num", "bigint")],
    )
    result = df.select(
        rs.col("num").cast("string"),
        rs.col("num").cast("int"),
    )
    rows = result.collect()
    assert len(rows) == 2
    # First column keeps name "num" (string), second gets "num_1" (int)
    assert "num" in rows[0]
    assert "num_1" in rows[0]
    assert rows[0]["num"] == "1"
    assert rows[0]["num_1"] == 1
    assert rows[1]["num"] == "2"
    assert rows[1]["num_1"] == 2


def test_select_three_same_name() -> None:
    """Three expressions with same output name become name, name_1, name_2."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"x": 10}],
        [("x", "bigint")],
    )
    result = df.select(
        rs.col("x"),
        rs.col("x") + rs.lit(0),
        rs.col("x") * 1,
    )
    rows = result.collect()
    assert len(rows) == 1
    assert "x" in rows[0]
    assert "x_1" in rows[0]
    assert "x_2" in rows[0]
    assert rows[0]["x"] == 10
    assert rows[0]["x_1"] == 10
    assert rows[0]["x_2"] == 10
