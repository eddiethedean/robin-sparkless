"""Regression tests for issue #199: Other expression or result parity (Sparkless).

Parent issue for astype/cast and expression parity; child issues #211, #214–#220
address specific failure categories. This file covers the representative example
from #199 and related basic cast/collect behavior.
"""

from tests.python.utils import get_functions, get_spark

F = get_functions()


def test_astype_cast_returns_expected_value_not_none() -> None:
    """Exact scenario from #199: cast to string in with_column must return '1', not None."""
    spark = get_spark("issue_199")
    df = spark.createDataFrame([{"num": 1}], ["num"])
    result = df.withColumn("num_str", F.col("num").cast("string"))
    rows = result.collect()
    assert rows[0]["num_str"] == "1"
    assert rows[0]["num"] == 1


def test_basic_astype_int_in_collect() -> None:
    """#199 affected: cast to int in with_column; collect must return int, not None."""
    spark = get_spark("issue_199")
    df = spark.createDataFrame([{"s": "123"}], ["s"])
    result = df.withColumn("n", F.col("s").cast("int"))
    rows = result.collect()
    assert rows[0]["n"] == 123
    assert rows[0]["s"] == "123"


def test_basic_astype_multiple_chained() -> None:
    """#199 affected: chained cast (e.g. int -> string); result must be string, not None."""
    spark = get_spark("issue_199")
    df = spark.createDataFrame([{"x": 123}], ["x"])
    result = df.withColumn("s", F.col("x").cast("string"))
    rows = result.collect()
    assert rows[0]["s"] == "123"
