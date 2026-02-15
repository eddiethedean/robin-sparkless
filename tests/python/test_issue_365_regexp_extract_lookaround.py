"""
Tests for #365: regexp_extract support lookahead/lookbehind (PySpark parity).

PySpark regexp_extract supports lookahead/lookbehind in regex patterns.
Robin-sparkless now supports them via fancy-regex when the pattern contains lookaround.
"""

from __future__ import annotations


def test_regexp_extract_lookbehind_issue_repro() -> None:
    """Lookbehind (?<=hello )\\w+ extracts 'world' from 'hello world' (issue repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_365").get_or_create()
    df = spark.createDataFrame([{"s": "hello world"}], [("s", "string")])
    rows = df.select(
        rs.regexp_extract(rs.col("s"), r"(?<=hello )\w+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "world"


def test_regexp_extract_lookahead() -> None:
    """Lookahead: digits followed by 'y' -> capture group 1 gives '42'."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_365").get_or_create()
    df = spark.createDataFrame([{"s": "x42y"}], [("s", "string")])
    # (\d+)(?=y) = digits followed by 'y' (positive lookahead); group 1 = "42"
    rows = df.select(
        rs.regexp_extract(rs.col("s"), r"(\d+)(?=y)", 1).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "42"


def test_regexp_extract_lookbehind_digits() -> None:
    """Lookbehind: digits after space (?<=\\s)\\d+."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_365").get_or_create()
    df = spark.createDataFrame([{"s": "price 99"}], [("s", "string")])
    rows = df.select(
        rs.regexp_extract(rs.col("s"), r"(?<=\s)\d+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "99"


def test_regexp_extract_without_lookaround_unchanged() -> None:
    """Patterns without lookaround still use Polars path (no regression)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_365").get_or_create()
    df = spark.createDataFrame([{"s": "a1b2c3"}], [("s", "string")])
    rows = df.select(
        rs.regexp_extract(rs.col("s"), r"\d+", 0).alias("extracted")
    ).collect()
    assert rows[0]["extracted"] == "1"
