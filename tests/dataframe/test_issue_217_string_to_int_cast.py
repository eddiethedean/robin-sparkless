from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

"""Tests for issue #217: String-to-int cast empty/invalid strings (PySpark parity).

Casting empty or invalid strings to int should yield null, not raise.
"""


def test_cast_empty_and_whitespace_string_to_int(spark) -> None:
    """Exact scenario from #217: cast('') and cast(' ') -> null."""
    df = spark.createDataFrame(
        [{"text": ""}, {"text": " "}],
        schema=["text"],
    )
    result = df.withColumn("n", F.col("text").cast("int"))
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["text"] == ""
    assert rows[0]["n"] is None
    assert rows[1]["text"] == " "
    assert rows[1]["n"] is None


def test_cast_invalid_strings_to_int_null(spark) -> None:
    """#217 affected: hello, abc123, '' -> null."""
    df = spark.createDataFrame(
        [{"s": "hello"}, {"s": "abc123"}, {"s": ""}, {"s": "42"}],
        schema=["s"],
    )
    result = df.withColumn("n", F.col("s").cast("int"))
    rows = result.collect()
    assert len(rows) == 4
    assert rows[0]["n"] is None
    assert rows[1]["n"] is None
    assert rows[2]["n"] is None
    assert rows[3]["n"] == 42


def test_try_cast_invalid_to_int_null(spark) -> None:
    """try_cast also yields null for invalid."""
    df = spark.createDataFrame(
        [{"s": "1"}, {"s": "x"}],
        schema=["s"],
    )
    result = df.withColumn("n", F.col("s").cast("int"))
    rows = result.collect()
    assert rows[0]["n"] == 1
    assert rows[1]["n"] is None


def test_cast_valid_string_to_long(spark) -> None:
    """cast to long/bigint works; invalid -> null."""
    df = spark.createDataFrame(
        [{"s": "9999999999"}, {"s": "bad"}],
        schema=["s"],
    )
    result = df.withColumn("n", F.col("s").cast("long"))
    rows = result.collect()
    assert rows[0]["n"] == 9999999999
    assert rows[1]["n"] is None
