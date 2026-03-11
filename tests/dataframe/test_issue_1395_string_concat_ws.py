"""Regression test for issue #1395: string.concat_ws parity.

PySpark scenario (from the issue body, paraphrased):

    lambda session: session.createDataFrame(
        [("a", "b"), ("a", None), (None, "c")],
        ["a", "b"],
    )

Expected PySpark behavior for:

    df.select(F.concat_ws("-", "a", "b").alias("out"))

is:

    [{'out': 'a-b'}, {'out': 'a'}, {'out': 'c'}]

Sparkless previously produced:

    [{'out': 'a-b'}, {'out': None}, {'out': None}]

This test locks in the PySpark semantics:
- nulls are ignored (dropped) from concat_ws arguments;
- rows with all-null inputs yield null.
"""

from __future__ import annotations

from sparkless.sql import SparkSession, functions as F


def test_issue_1395_string_concat_ws_null_handling() -> None:
    spark = SparkSession.builder.appName("issue_1395_string_concat_ws").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("a", "b"), ("a", None), (None, "c")],
            ["a", "b"],
        )
        out = df.select(F.concat_ws("-", "a", "b").alias("out"))

        rows = [{k: v for k, v in r.asDict().items()} for r in out.collect()]
        assert rows == [
            {"out": "a-b"},
            {"out": "a"},
            {"out": "c"},
        ]
    finally:
        spark.stop()
