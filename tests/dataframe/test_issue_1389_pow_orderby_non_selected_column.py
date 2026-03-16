"""Regression test for issue #1389: column.pow_operator parity.

Same scenario in both modes via spark + spark_imports: pow and orderBy non-selected column.

Sparkless previously raised an unresolved_column error when ordering by "x"
after selecting only the derived "sq" column. This test locks in the
expected behavior that the operation should succeed without error and
produce the squared values.
"""

from __future__ import annotations


def test_issue_1389_pow_orderby_non_selected_column(spark, spark_imports) -> None:
    F = spark_imports.F
    df = spark.createDataFrame([(3,), (5,)], ["x"])
    out = df.select((F.col("x") ** F.lit(2)).alias("sq")).orderBy("x")

    rows = list(out.collect())
    assert [r["sq"] for r in rows] == [9, 25]
