"""
Regression test for issue #1401: date.to_date should not trigger an
unresolved-column error when applied to a simple string column.

PySpark scenario (from the issue):

    lambda session: session.createDataFrame(
        [("2020-01-02",), ("invalid",), (None,)],
        ["s"],
    )

The parity harness then applies date.to_date to column ``s``; previously
Sparkless raised:

    SparklessError: unresolved_column: cannot resolve: column 's' not found.
    Available columns: [d].
"""

from datetime import date

from sparkless.sql import SparkSession, functions as F


def test_issue_1401_date_to_date_no_unresolved_column() -> None:
    """date.to_date on a single string column must not raise (issue #1401)."""
    spark = SparkSession.builder.appName("issue_1401").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("2020-01-02",), ("invalid",), (None,)],
            ["s"],
        )

        # Apply to_date to column "s" and collect the result. The important
        # behavior for this regression is that Sparkless does NOT raise an
        # unresolved-column error for 's'.
        result = df.select(F.to_date("s").alias("d"))
        rows = result.collect()

        assert len(rows) == 3
        values = [row["d"] for row in rows]
        assert values[0] == date(2020, 1, 2)
        assert values[1] is None
        assert values[2] is None
    finally:
        spark.stop()
