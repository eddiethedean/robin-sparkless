"""Regression test for issue #1396: string.regexp_extract parity.

Scenario (paraphrased from the issue):

    df = session.createDataFrame([("abc123",), ("zzz",), (None,)], ["s"])
    df.select(F.regexp_extract("s", r"(\d+)$", 1).alias("out"))

PySpark semantics for regexp_extract:
- null input -> null
- non-null input with no match -> empty string
- non-null input with match -> captured substring

This test locks in that behavior for Sparkless.
"""

from __future__ import annotations

from sparkless.sql import SparkSession, functions as F


def test_issue_1396_string_regexp_extract_null_and_no_match() -> None:
    spark = SparkSession.builder.appName("issue_1396_string_regexp_extract").getOrCreate()
    try:
        df = spark.createDataFrame(
            [("abc123",), ("zzz",), (None,)],
            ["s"],
        )
        out = df.select(F.regexp_extract("s", r"(\d+)$", 1).alias("out"))

        # Preserve input order to make expectations clear.
        rows = [r["out"] for r in out.collect()]
        # Match expected PySpark behavior for this pattern: non-matching input
        # yields empty string; null input yields null.
        assert rows == ["123", "", None]
    finally:
        spark.stop()

