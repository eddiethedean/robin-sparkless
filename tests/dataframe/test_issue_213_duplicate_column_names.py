"""Tests for issue #213: duplicate column names in select.

Polars raises 'duplicate: the name X is duplicate' when select produces columns
with the same name. We disambiguate with _1, _2, ... (PySpark/Sparkless parity).
"""

from tests.utils import _row_to_dict
import pytest
from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F


@pytest.mark.skip(reason="Issue #1187: unskip when fixing")
def test_select_same_column_cast_twice(spark) -> None:
    """Select same column cast to different types (duplicate output names) should not error."""
    df = spark.createDataFrame(
        [{"num": 1}, {"num": 2}],
        ["num"],
    )
    result = df.select(
        F.col("num").cast("string").alias("num"),
        F.col("num").cast("int").alias("num_1"),
    )
    rows = [_row_to_dict(r) for r in result.collect()]
    assert len(rows) == 2
    assert "num" in rows[0]
    assert "num_1" in rows[0]
    assert rows[0]["num"] == "1"
    assert rows[0]["num_1"] == 1
    assert rows[1]["num"] == "2"
    assert rows[1]["num_1"] == 2


def test_select_three_same_name(spark) -> None:
    """Three expressions with same output name become name, name_1, name_2."""
    df = spark.createDataFrame(
        [{"x": 10}],
        ["x"],
    )
    result = df.select(
        F.col("x").alias("x"),
        (F.col("x") + F.lit(0)).alias("x_1"),
        (F.col("x") * 1).alias("x_2"),
    )
    rows = [_row_to_dict(r) for r in result.collect()]
    assert len(rows) == 1
    assert rows[0]["x"] == 10
    assert rows[0]["x_1"] == 10
    assert rows[0]["x_2"] == 10
