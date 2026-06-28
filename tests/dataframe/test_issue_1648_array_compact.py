"""Regression test for issue #1648: F.array_compact()."""

from __future__ import annotations

from sparkless.testing import get_imports

F = get_imports().F
ArrayType = get_imports().ArrayType
StringType = get_imports().StringType
StructField = get_imports().StructField
StructType = get_imports().StructType


def _row_val(row, key):
    if hasattr(row, "asDict"):
        return row.asDict().get(key)
    return row[key]


class TestIssue1648ArrayCompact:
    """PySpark array_compact removes null elements from arrays."""

    def test_array_compact_issue_example(self, spark) -> None:
        """Exact scenario from issue #1648."""
        schema = StructType([StructField("col1", ArrayType(StringType()))])
        df = spark.createDataFrame([([" A", None, "B"],)], schema)
        result = df.withColumn("col2", F.array_compact("col1"))
        row = result.collect()[0]
        assert _row_val(row, "col1") == [" A", None, "B"]
        assert _row_val(row, "col2") == [" A", "B"]

    def test_array_compact_all_nulls(self, spark) -> None:
        schema = StructType([StructField("col1", ArrayType(StringType()))])
        df = spark.createDataFrame([( [None, None], )], schema)
        row = df.withColumn("col2", F.array_compact("col1")).collect()[0]
        assert _row_val(row, "col2") == []

    def test_array_compact_no_nulls_unchanged(self, spark) -> None:
        schema = StructType([StructField("col1", ArrayType(StringType()))])
        df = spark.createDataFrame([( ["x", "y"], )], schema)
        row = df.withColumn("col2", F.array_compact("col1")).collect()[0]
        assert _row_val(row, "col2") == ["x", "y"]
