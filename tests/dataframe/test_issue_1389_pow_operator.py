"""
Regression test for issue #1389: column.pow_operator should not error when used
in a projection followed by orderBy on the original column name.

PySpark scenario:

    df = session.createDataFrame([(3,), (5,)], ["x"])
    df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")

Sparkless previously raised:
    SparklessError: unresolved_column: cannot resolve: column 'x' not found. Available columns: [sq].
"""

from sparkless.sql import SparkSession, functions as F


def test_pow_operator_order_by_original_column_name() -> None:
    """column.pow_operator: select(col(\"x\") ** lit(2)).orderBy(\"x\") must succeed (issue #1389)."""
    spark = SparkSession.builder.appName("issue_1389").getOrCreate()
    try:
        df = spark.createDataFrame([(3,), (5,)], ["x"])
        result = df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")

        rows = result.collect()
        # Expect two rows, ordered by x (3, 5). We only have \"sq\" in schema; values should be 9 and 25.
        assert len(rows) == 2
        assert rows[0]["sq"] == 9
        assert rows[1]["sq"] == 25
    finally:
        spark.stop()

