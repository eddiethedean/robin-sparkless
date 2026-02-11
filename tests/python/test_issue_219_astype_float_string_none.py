"""Tests for issue #219: TypeError NoneType in astype float/string conversions.

Float/string cast with nulls must not raise TypeError. Nulls are returned as None
in collect(); callers should check 'value is None' before using 'x in value'.
"""

import robin_sparkless as rs


def test_float_to_string_with_nulls() -> None:
    """Exact scenario from #219: float column with None, cast to string, collect."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"f": 1.5}, {"f": None}, {"f": 2.0}],
        [("f", "double")],
    )
    result = df.with_column("s", rs.col("f").cast("string"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["f"] == 1.5 and rows[0]["s"] == "1.5"
    assert rows[1]["f"] is None and rows[1]["s"] is None
    assert rows[2]["f"] == 2.0 and rows[2]["s"] == "2.0"


def test_string_to_float_with_nulls() -> None:
    """String column with nulls cast to double; nulls stay None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"s": "1.5"}, {"s": None}, {"s": "2.0"}],
        [("s", "string")],
    )
    result = df.with_column("f", rs.col("s").cast("double"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["s"] == "1.5" and rows[0]["f"] == 1.5
    assert rows[1]["s"] is None and rows[1]["f"] is None
    assert rows[2]["s"] == "2.0" and rows[2]["f"] == 2.0


def test_collect_null_safe_iteration() -> None:
    """Iterating over rows and keys/values must not raise when values are None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"f": 1.0}, {"f": None}],
        [("f", "double")],
    )
    result = df.with_column("s", rs.col("f").cast("string"))
    rows = result.collect()
    for row in rows:
        for key in row:
            val = row[key]
            # Safe: do not use 'x in val' when val may be None
            if val is not None:
                assert isinstance(val, (str, float, int))
            assert key in ("f", "s")
