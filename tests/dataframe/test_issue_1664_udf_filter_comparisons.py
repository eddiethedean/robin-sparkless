"""Tests for issue #1664: UDF filter with ==, >=, <=, !=, < operators."""

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F
T = _imports
udf = F.udf


def test_udf_filter_gt(spark) -> None:
    """UDF filter with > (regression)."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) > 10).collect()
    assert sorted(r["x"] for r in rows) == [10]


def test_udf_filter_eq(spark) -> None:
    """UDF filter with == must evaluate UDF before comparing."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) == 10).collect()
    assert sorted(r["x"] for r in rows) == [5]


def test_udf_filter_ge(spark) -> None:
    """UDF filter with >= must evaluate UDF before comparing."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) >= 10).collect()
    assert sorted(r["x"] for r in rows) == [5, 10]


def test_udf_filter_le(spark) -> None:
    """UDF filter with <= must evaluate UDF before comparing."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) <= 4).collect()
    assert sorted(r["x"] for r in rows) == [1]


def test_udf_filter_lt(spark) -> None:
    """UDF filter with < must evaluate UDF before comparing."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) < 10).collect()
    assert sorted(r["x"] for r in rows) == [1]


def test_udf_filter_ne(spark) -> None:
    """UDF filter with != must evaluate UDF before comparing."""
    df = spark.createDataFrame([{"x": 1}, {"x": 5}, {"x": 10}])

    @udf(T.IntegerType())
    def double(x):
        return x * 2

    rows = df.filter(double(F.col("x")) != 10).collect()
    assert sorted(r["x"] for r in rows) == [1, 10]
