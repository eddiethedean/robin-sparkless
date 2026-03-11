"""Regression test for issue #1397: string.split_basic parity.

Scenario (from the issue, reconstructed):

    df = session.createDataFrame([("a,b,c",), ("a",), (None,)], ["s"])
    df.select(F.split("s", ",").alias("arr")).orderBy("s")

Previously, Sparkless raised a SparklessError:

    unresolved_column: cannot be resolved: not found: cannot resolve: column 's' not found.
    Available columns: [arr].

PySpark succeeds for this scenario. This test locks in the expectation that
Sparkless also succeeds (no unresolved_column error) and returns a 3-row
DataFrame with an array<string> column.
"""

from __future__ import annotations

from sparkless.sql import SparkSession, functions as F


def test_issue_1397_string_split_basic_no_unresolved_column_error() -> None:
    spark = SparkSession.builder.appName("issue_1397_string_split_basic").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("a,b,c",), ("a",), (None,)],
            ["s"],
        )
        out = df.select(F.split("s", ",").alias("arr")).orderBy("s")

        rows = list(out.collect())
        assert len(rows) == 3
        assert out.schema.simpleString() == "struct<arr:array<string>>"
    finally:
        spark.stop()
