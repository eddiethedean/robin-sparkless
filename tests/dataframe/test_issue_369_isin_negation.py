"""
Tests for issue #369: ~F.col("Values").isin([20, 30]) with string column.

PySpark allows is_in checks with negation when column type and list type differ
(string column vs int list); Polars raises InvalidOperationError without coercion.
Sparkless now coerces the list to the column type so ~col.isin([...]) works.
"""


class TestIssue369IsinNegation:
    """Test ~col.isin([...]) with string column and int list (issue #369)."""

    def test_negation_isin_string_column_int_list(self, spark):
        """Exact scenario from issue #369: filter with ~col.isin([int, int]) on string column."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df = df.filter(~F.col("Values").isin([20, 30]))
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice" and rows[0]["Values"] == "10"

    def test_negation_isin_show(self, spark):
        """Exact issue scenario: filter + show() (issue #369)."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df = df.filter(~F.col("Values").isin([20, 30]))
        df.show()
        rows = df.collect()
        assert len(rows) == 1 and rows[0]["Name"] == "Alice"

    def test_isin_without_negation_string_column_int_list(self, spark):
        """Positive isin (no ~) with string column and int list also coerces."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df = df.filter(F.col("Values").isin([20, 30]))
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Bob" and rows[0]["Values"] == "20"

    def test_negation_isin_string_to_string(self, spark):
        """~col.isin([str, str]) on string column (types match) still works."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": "10"},
                {"Name": "Bob", "Values": "20"},
            ]
        )
        df = df.filter(~F.col("Values").isin(["20", "30"]))
        rows = df.collect()
        assert len(rows) == 1 and rows[0]["Name"] == "Alice"
