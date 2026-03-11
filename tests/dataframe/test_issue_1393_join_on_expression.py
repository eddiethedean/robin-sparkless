"""Regression test for issue #1393: join.on_expression parity.

Scenario from the issue:

    def scenario_join_on_expression(session):
        df1 = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
        df2 = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
        return df1.join(df2, on=df1["id"] == df2["id"], how="inner").orderBy("id")

PySpark raises an AnalysisException for the unqualified/ambiguous ORDER BY
column `id` after the join. Sparkless should not silently succeed; this test
locks in the expectation that Sparkless raises a SparklessError for the
ambiguous column reference.
"""

from __future__ import annotations

import pytest

from sparkless.errors import SparklessError
from sparkless.sql import SparkSession


def _scenario_join_on_expression(session: SparkSession):
    df1 = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    df2 = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
    return df1.join(df2, on=df1["id"] == df2["id"], how="inner").orderBy("id")


def test_issue_1393_join_on_expression_ambiguous_order_by_raises() -> None:
    """join(on expression) followed by orderBy(\"id\") should raise SparklessError for ambiguity."""
    spark = SparkSession.builder.appName("issue_1393_join_on_expression").getOrCreate()
    try:
        with pytest.raises(SparklessError) as excinfo:
            df = _scenario_join_on_expression(spark)
            # Trigger execution (the error may surface on collect).
            _ = df.collect()

        msg = str(excinfo.value)
        # Lock in current ambiguous/column-not-found error shape so future
        # changes are intentional and visible in tests.
        assert "unresolved_column" in msg or "AMBIGUOUS_REFERENCE" in msg
        assert "id" in msg
    finally:
        spark.stop()
