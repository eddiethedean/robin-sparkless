"""Issue #1573: F.when(cond, None).otherwise(...) must chain like PySpark."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_when_none_otherwise_col() -> None:
    spark = SparkSession.builder.appName("issue_1573").get_or_create()
    df = spark.createDataFrame([{"A": "-"}])
    out = df.withColumn(
        "A",
        F.when(F.col("A") == "-", None).otherwise(F.col("A")),
    )
    row = out.collect()[0]
    assert row["A"] is None
