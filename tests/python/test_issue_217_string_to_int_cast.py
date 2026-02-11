"""Tests for issue #217: String-to-int cast empty/invalid strings.

Casting empty or invalid strings to int should yield null (Spark/Sparkless parity),
not raise RuntimeError. Polars strict conversion is replaced by custom parsing.
"""

import robin_sparkless as rs


def test_cast_empty_and_whitespace_string_to_int() -> None:
    """Exact scenario from #217: cast('') and cast(' ') -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"text": ""}, {"text": " "}],
        [("text", "string")],
    )
    result = df.with_column("n", rs.col("text").cast("int"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["text"] == ""
    assert rows[0]["n"] is None
    assert rows[1]["text"] == " "
    assert rows[1]["n"] is None


def test_cast_invalid_strings_to_int_null() -> None:
    """#217 affected: hello, abc123, '' -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"s": "hello"}, {"s": "abc123"}, {"s": ""}, {"s": "42"}],
        [("s", "string")],
    )
    result = df.with_column("n", rs.col("s").cast("int"))
    rows = result.collect()
    assert len(rows) == 4
    assert rows[0]["n"] is None
    assert rows[1]["n"] is None
    assert rows[2]["n"] is None
    assert rows[3]["n"] == 42


def test_try_cast_invalid_to_int_null() -> None:
    """try_cast also yields null for invalid."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"s": "1"}, {"s": "x"}],
        [("s", "string")],
    )
    result = df.with_column("n", rs.try_cast(rs.col("s"), "int"))
    rows = result.collect()
    assert rows[0]["n"] == 1
    assert rows[1]["n"] is None


def test_cast_valid_string_to_long() -> None:
    """cast to long/bigint works; invalid -> null."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"s": "9999999999"}, {"s": "bad"}],
        [("s", "string")],
    )
    result = df.with_column("n", rs.col("s").cast("long"))
    rows = result.collect()
    assert rows[0]["n"] == 9999999999
    assert rows[1]["n"] is None
