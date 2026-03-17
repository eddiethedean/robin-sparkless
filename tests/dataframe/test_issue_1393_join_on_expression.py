"""Regression test for issue #1393: join.on_expression parity.

Scenario from the issue:

    def scenario_join_on_expression(session):
        df1 = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
        df2 = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
        return df1.join(df2, on=df1["id"] == df2["id"], how="inner").orderBy("id")

PySpark raises AnalysisException and Sparkless raises SparklessError for the
unqualified/ambiguous ORDER BY column `id` after the join. This test asserts
both backends raise with UNRESOLVED_COLUMN/AMBIGUOUS_REFERENCE and mention of `id`.
"""

from __future__ import annotations

import pytest


def _scenario_join_on_expression(session):
    df1 = session.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    df2 = session.createDataFrame([(1, "x"), (3, "y")], ["id", "w"])
    return df1.join(df2, on=df1["id"] == df2["id"], how="inner").orderBy("id")


def test_issue_1393_join_on_expression_ambiguous_order_by_raises(spark) -> None:
    """join(on expression) followed by orderBy(\"id\") raises for ambiguous column (PySpark #1510)."""
    with pytest.raises(Exception) as excinfo:
        df = _scenario_join_on_expression(spark)
        # Trigger execution (the error may surface on collect).
        _ = df.collect()

    msg = str(excinfo.value)
    assert "UNRESOLVED_COLUMN" in msg or "AMBIGUOUS_REFERENCE" in msg
    assert "id" in msg
