"""
Tests for issue #1603: createDataFrame with DDL schema and None values.

PySpark uses the provided DDL schema and does not attempt type inference when
a column is all null; sparkless previously raised ValueError.
"""

from __future__ import annotations


def test_ddl_schema_with_all_null_column(spark) -> None:
    """Exact scenario from issue #1603."""
    schema = "col1 string, col2 string"
    df = spark.createDataFrame([("A", None), ("B", None)], schema)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["col1"] == "A"
    assert rows[0]["col2"] is None
    assert rows[1]["col1"] == "B"
    assert rows[1]["col2"] is None


def test_ddl_schema_colon_syntax_with_nulls(spark) -> None:
    """DDL with colon separator should also respect explicit types."""
    schema = "col1: string, col2: int"
    df = spark.createDataFrame([("A", None), ("B", None)], schema)
    rows = df.collect()
    assert rows[0]["col1"] == "A"
    assert rows[0]["col2"] is None


def test_explicit_name_type_pairs_with_nulls(spark) -> None:
    """List of (name, type) pairs is explicit schema, not names-only inference."""
    schema = [("col1", "string"), ("col2", "string")]
    df = spark.createDataFrame([("A", None), ("B", None)], schema)
    rows = df.collect()
    assert rows[0]["col2"] is None
