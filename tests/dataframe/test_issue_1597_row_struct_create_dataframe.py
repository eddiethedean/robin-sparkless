"""
Tests for issue #1597: Row as struct value in createDataFrame.

PySpark accepts Row, dict, tuple, or list for nested struct columns; sparkless
previously stringified Row and failed struct parsing.
"""

from __future__ import annotations

from sparkless.testing import get_imports

Row = get_imports().Row
DoubleType = get_imports().DoubleType
StringType = get_imports().StringType
StructField = get_imports().StructField
StructType = get_imports().StructType


def test_create_dataframe_struct_value_from_row(spark) -> None:
    """Exact scenario from issue #1597."""
    inner_schema = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", DoubleType()),
        ]
    )
    schema = StructType(
        [
            StructField("col_a", StringType()),
            StructField("col_b", inner_schema),
        ]
    )

    nested = Row(col1="X", col2=0.95)
    row = spark.createDataFrame([("A", nested)], schema).collect()[0]
    assert row["col_a"] == "A"
    assert row["col_b"]["col1"] == "X"
    assert row["col_b"]["col2"] == 0.95


def test_create_dataframe_struct_value_from_dict(spark) -> None:
    """Dict struct values should continue to work."""
    inner_schema = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", DoubleType()),
        ]
    )
    schema = StructType(
        [
            StructField("col_a", StringType()),
            StructField("col_b", inner_schema),
        ]
    )

    row = spark.createDataFrame(
        [("A", {"col1": "X", "col2": 0.95})],
        schema,
    ).collect()[0]
    assert row["col_b"]["col1"] == "X"
    assert row["col_b"]["col2"] == 0.95


def test_create_dataframe_struct_value_from_tuple(spark) -> None:
    """Tuple struct values map by position."""
    inner_schema = StructType(
        [
            StructField("col1", StringType()),
            StructField("col2", DoubleType()),
        ]
    )
    schema = StructType(
        [
            StructField("col_a", StringType()),
            StructField("col_b", inner_schema),
        ]
    )

    row = spark.createDataFrame([("A", ("X", 0.95))], schema).collect()[0]
    assert row["col_b"]["col1"] == "X"
    assert row["col_b"]["col2"] == 0.95
