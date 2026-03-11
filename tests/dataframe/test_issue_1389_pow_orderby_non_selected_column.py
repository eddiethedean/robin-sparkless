"""Regression test for issue #1389: column.pow_operator parity.

PySpark scenario (from the issue body):

    def scenario_pow_operator(session):
        if _backend_is_pyspark(session):
            from pyspark.sql import functions as F  # type: ignore
        else:
            from sparkless.sql import functions as F  # type: ignore

        df = session.createDataFrame([(3,), (5,)], ["x"])
        return df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")

Sparkless previously raised an unresolved_column error when ordering by "x"
after selecting only the derived "sq" column. This test locks in the
expected behavior that the operation should succeed without error and
produce the squared values.
"""

from __future__ import annotations

from sparkless.sql import SparkSession, functions as F


def test_issue_1389_pow_orderby_non_selected_column() -> None:
    spark = SparkSession.builder.appName("issue_1389_pow_orderby").getOrCreate()
    try:
        df = spark.createDataFrame([(3,), (5,)], ["x"])
        out = df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")

        rows = list(out.collect())
        assert [r["sq"] for r in rows] == [9, 25]
    finally:
        spark.stop()

