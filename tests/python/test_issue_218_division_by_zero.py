"""Tests for issue #218: Division by zero returns null (PySpark parity).

PySpark returns null for division by zero; these tests assert that behavior
directly against a real PySpark session via the ``spark`` fixture.
"""

from tests.python.utils import get_functions

F = get_functions()


def test_division_by_zero_literal_over_column(spark) -> None:
    """Exact scenario from #218: lit(1) / col('x') with x=0 -> null."""
    df = spark.createDataFrame(
        [{"x": 1}, {"x": 0}],
        schema=["x"],
    )
    result = df.withColumn("q", F.lit(1) / F.col("x"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["q"] == 1.0
    assert rows[1]["q"] is None


def test_division_by_zero_column_over_literal(spark) -> None:
    """col('x') / lit(0) -> null."""
    df = spark.createDataFrame(
        [{"x": 10}, {"x": 20}],
        schema=["x"],
    )
    result = df.withColumn("q", F.col("x") / F.lit(0))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["q"] is None
    assert rows[1]["q"] is None


def test_division_by_zero_column_over_column(spark) -> None:
    """col('a') / col('b') with b=0 -> null."""
    df = spark.createDataFrame(
        [{"a": 5, "b": 1}, {"a": 5, "b": 0}],
        schema=["a", "b"],
    )
    result = df.withColumn("q", F.col("a") / F.col("b"))
    rows = result.collect()
    assert rows[0]["q"] == 5.0
    assert rows[1]["q"] is None
