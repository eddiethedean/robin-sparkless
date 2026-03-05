"""Tests for issue #219: TypeError NoneType in astype float/string conversions (PySpark parity).

Float/string cast with nulls must not raise TypeError. Nulls are returned as None
in collect(); callers should check 'value is None' before using 'x in value'.
"""

from tests.utils import get_functions

F = get_functions()


def test_float_to_string_with_nulls(spark) -> None:
    """Exact scenario from #219: float column with None, cast to string, collect."""
    df = spark.createDataFrame(
        [{"f": 1.5}, {"f": None}, {"f": 2.0}],
        schema=["f"],
    )
    result = df.withColumn("s", F.col("f").cast("string"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["f"] == 1.5 and rows[0]["s"] == "1.5"
    assert rows[1]["f"] is None and rows[1]["s"] is None
    assert rows[2]["f"] == 2.0 and rows[2]["s"] == "2.0"


def test_string_to_float_with_nulls(spark) -> None:
    """String column with nulls cast to double; nulls stay None."""
    df = spark.createDataFrame(
        [{"s": "1.5"}, {"s": None}, {"s": "2.0"}],
        schema=["s"],
    )
    result = df.withColumn("f", F.col("s").cast("double"))
    rows = result.collect()
    assert len(rows) == 3
    assert rows[0]["s"] == "1.5" and rows[0]["f"] == 1.5
    assert rows[1]["s"] is None and rows[1]["f"] is None
    assert rows[2]["s"] == "2.0" and rows[2]["f"] == 2.0


def test_collect_null_safe_iteration(spark) -> None:
    """Iterating over rows and keys/values must not raise when values are None."""
    df = spark.createDataFrame(
        [{"f": 1.0}, {"f": None}],
        schema=["f"],
    )
    result = df.withColumn("s", F.col("f").cast("string"))
    rows = result.collect()
    for row in rows:
        for key, val in row.asDict().items():
            # Safe: do not use 'x in val' when val may be None
            if val is not None:
                assert isinstance(val, (str, float, int))
            assert key in ("f", "s")
