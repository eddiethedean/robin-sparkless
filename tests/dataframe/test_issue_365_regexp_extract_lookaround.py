"""
Tests for #365: regexp_extract support lookahead/lookbehind (PySpark parity).

PySpark regexp_extract supports lookahead/lookbehind in regex patterns.
Robin-sparkless now supports them via fancy-regex when the pattern contains lookaround.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports


_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


def test_regexp_extract_lookbehind_issue_repro() -> None:
    """Lookbehind (?<=hello )\\w+ extracts 'world' from 'hello world' (issue repro)."""
    spark = SparkSession.builder.appName("issue_365").getOrCreate()
    df = spark.createDataFrame([{"s": "hello world"}], ["s"])
    rows = df.select(
        F.regexp_extract(F.col("s"), r"(?<=hello )\w+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "world"


def test_regexp_extract_lookahead() -> None:
    """Lookahead: digits followed by 'y' -> capture group 1 gives '42'."""
    spark = SparkSession.builder.appName("issue_365").getOrCreate()
    df = spark.createDataFrame([{"s": "x42y"}], ["s"])
    # (\d+)(?=y) = digits followed by 'y' (positive lookahead); group 1 = "42"
    rows = df.select(
        F.regexp_extract(F.col("s"), r"(\d+)(?=y)", 1).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "42"


def test_regexp_extract_lookbehind_digits() -> None:
    """Lookbehind: digits after space (?<=\\s)\\d+."""
    spark = SparkSession.builder.appName("issue_365").getOrCreate()
    df = spark.createDataFrame([{"s": "price 99"}], ["s"])
    rows = df.select(
        F.regexp_extract(F.col("s"), r"(?<=\s)\d+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "99"


def test_regexp_extract_without_lookaround_unchanged() -> None:
    """Patterns without lookaround still use Polars path (no regression)."""
    spark = SparkSession.builder.appName("issue_365").getOrCreate()
    df = spark.createDataFrame([{"s": "a1b2c3"}], ["s"])
    rows = df.select(
        F.regexp_extract(F.col("s"), r"\d+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "1"
