"""
Subset of Sparkless string arithmetic tests, run directly against robin-sparkless.

These focus on verifying that string columns participate in arithmetic operations
and that behaviour matches PySpark-style semantics for common cases (invalid
strings -> null). Robin-sparkless requires explicit cast for string-to-numeric coercion.
"""

from __future__ import annotations


def _get_session():
    import robin_sparkless as rs

    return rs.SparkSession.builder().app_name("test").get_or_create()


def test_string_division_by_numeric_literal_robin() -> None:
    """col('string_1').cast('double') / 5 where string_1 is a string column."""
    spark = _get_session()
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(
        [{"string_1": "10.0"}, {"string_1": "20"}],
        [("string_1", "string")],
    )

    result = df.with_column("result", rs.col("string_1").cast("double") / 5)
    rows = result.collect()

    assert len(rows) == 2
    assert rows[0]["result"] == 2.0
    assert rows[1]["result"] == 4.0

    # Schema-level check: result column is Double-like
    schema_str = result.print_schema()
    assert "result" in schema_str and (
        "double" in schema_str.lower() or "float" in schema_str.lower()
    )


def test_numeric_literal_divided_by_string_robin() -> None:
    """100 / col('string_1').cast('double') where string_1 is a string column."""
    spark = _get_session()
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(
        [{"string_1": "10.0"}, {"string_1": "5"}],
        [("string_1", "string")],
    )

    result = df.with_column("result", rs.lit(100) / rs.col("string_1").cast("double"))
    rows = result.collect()

    assert len(rows) == 2
    assert rows[0]["result"] == 10.0
    assert rows[1]["result"] == 20.0


def test_string_arithmetic_with_invalid_strings_robin() -> None:
    """Invalid numeric strings become null when used in arithmetic (via cast)."""
    spark = _get_session()
    import robin_sparkless as rs

    df = spark._create_dataframe_from_rows(
        [{"string_1": "10.0"}, {"string_1": "invalid"}, {"string_1": "20"}],
        [("string_1", "string")],
    )

    result = df.with_column("result", rs.col("string_1").try_cast("double") / 5)
    rows = result.collect()

    assert len(rows) == 3
    assert rows[0]["result"] == 2.0
    assert rows[1]["result"] is None
    assert rows[2]["result"] == 4.0
