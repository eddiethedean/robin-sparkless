from __future__ import annotations


def test_issue_1533_drop_nonexistent_column_is_idempotent(spark, spark_imports) -> None:
    df = spark.createDataFrame([("A", "B", "C")])

    out = df.drop("D")

    assert out.columns == df.columns
