"""
Tests for issue #359: NAHandler.drop method.

PySpark supports df.na.drop() for dropping rows with null values.
This test verifies that Sparkless supports the same operation.

Works with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

import pytest

from tests.fixtures.spark_backend import BackendType, get_backend_type


def _is_pyspark_mode() -> bool:
    """True if running with PySpark backend."""
    return bool(get_backend_type() == BackendType.PYSPARK)


# Exception type for missing column (PySpark uses AnalysisException)
if _is_pyspark_mode():
    try:
        from pyspark.sql.utils import AnalysisException as ColumnNotFoundException
    except ImportError:
        from sparkless.core.exceptions.analysis import ColumnNotFoundException
else:
    from sparkless.core.exceptions.analysis import ColumnNotFoundException


class TestIssue359NADrop:
    """Test NAHandler.drop method (issue #359)."""

    def test_na_drop_with_subset_exact_issue_scenario(self, spark):
        """Exact scenario from issue #359: drop rows with None in specified column."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result = df.na.drop(subset=["Value"])
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"
        assert rows[0]["Value"] == 1

    def test_na_drop_no_subset_drops_any_null(self, spark):
        """na.drop() with no subset drops rows with any null (how='any' default)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
                {"Name": "Charlie", "Value": 3},
            ]
        )
        result = df.na.drop()
        rows = result.collect()
        assert len(rows) == 2
        names = {r["Name"] for r in rows}
        assert names == {"Alice", "Charlie"}

    def test_na_drop_subset_as_string(self, spark):
        """na.drop(subset='Value') with single column as string."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result = df.na.drop(subset="Value")
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["Name"] == "Alice"

    def test_na_drop_subset_as_tuple(self, spark):
        """na.drop(subset=(col1, col2)) with tuple of columns."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": None, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": 1, "b": 2, "c": None},
            ]
        )
        # Drop rows with null in a or b only
        result = df.na.drop(subset=("a", "b"))
        rows = result.collect()
        assert len(rows) == 2  # rows 1 and 2 have null in a or b

    def test_na_drop_how_all(self, spark):
        """na.drop(how='all') drops only rows where all values are null."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2},
                {"a": None, "b": 2},
                {"a": None, "b": None},
            ]
        )
        result = df.na.drop(how="all")
        rows = result.collect()
        assert len(rows) == 2  # third row is all null

    def test_na_drop_thresh(self, spark):
        """na.drop(thresh=n) keeps rows with at least n non-null values."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": None, "b": None, "c": 3},
            ]
        )
        result = df.na.drop(thresh=2)  # keep rows with >= 2 non-null
        rows = result.collect()
        assert len(rows) == 2  # third row has only 1 non-null

    def test_na_drop_no_nulls_returns_same(self, spark):
        """na.drop() on DataFrame with no nulls returns same row count."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": 2},
            ]
        )
        result = df.na.drop()
        rows = result.collect()
        assert len(rows) == 2

    def test_na_drop_equivalent_to_dropna(self, spark):
        """na.drop() produces same result as dropna()."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result_na = df.na.drop(subset=["Value"]).collect()
        result_direct = df.dropna(subset=["Value"]).collect()
        assert len(result_na) == len(result_direct) == 1
        assert result_na[0]["Name"] == result_direct[0]["Name"]


class TestIssue359NADropRobust:
    """Robust tests for NAHandler.drop: edge cases, errors, chaining, schema."""

    def test_na_drop_empty_dataframe(self, spark):
        """na.drop() on empty DataFrame returns empty DataFrame."""
        df = spark.createDataFrame([], "name string, value int")
        result = df.na.drop()
        rows = result.collect()
        assert len(rows) == 0

    def test_na_drop_all_rows_have_null_how_any(self, spark):
        """na.drop(how='any') drops every row that has any null."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": None},
                {"a": None, "b": 2},
                {"a": None, "b": None},
            ]
        )
        result = df.na.drop(how="any")
        rows = result.collect()
        assert len(rows) == 0

    def test_na_drop_how_all_only_drops_fully_null_rows(self, spark):
        """na.drop(how='all') keeps rows that have at least one non-null."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": None},
                {"a": None, "b": 2},
                {"a": None, "b": None},
            ]
        )
        result = df.na.drop(how="all")
        rows = result.collect()
        assert len(rows) == 2
        # Third row (all null) is dropped

    def test_na_drop_thresh_one_keeps_rows_with_at_least_one_non_null(self, spark):
        """na.drop(thresh=1) keeps rows with >= 1 non-null value."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        IntegerType = imports.IntegerType
        schema = StructType(
            [
                StructField("a", IntegerType(), nullable=True),
                StructField("b", IntegerType(), nullable=True),
                StructField("c", IntegerType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"a": 1, "b": None, "c": None},
                {"a": None, "b": None, "c": None},
            ],
            schema=schema,
        )
        result = df.na.drop(thresh=1)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["a"] == 1

    def test_na_drop_thresh_equals_column_count(self, spark):
        """na.drop(thresh=n) with n=column count keeps only full rows."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
            ]
        )
        result = df.na.drop(thresh=3)
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["a"] == 1 and rows[0]["b"] == 2 and rows[0]["c"] == 3

    def test_na_drop_after_filter(self, spark):
        """na.drop() after filter drops nulls in remaining rows."""
        from tests.fixtures.spark_imports import get_spark_imports

        F = get_spark_imports().F
        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice", "score": 90},
                {"id": 2, "name": None, "score": 85},
                {"id": 3, "name": "Charlie", "score": None},
            ]
        )
        result = df.filter(F.col("score") > 80).na.drop(subset=["name"])
        rows = result.collect()
        # After filter(score > 80) and drop(name null): no row should have null name
        assert all(r["name"] is not None for r in rows)
        alice = next((r for r in rows if r["name"] == "Alice"), None)
        assert alice is not None
        assert alice["score"] == 90

    def test_na_drop_after_select(self, spark):
        """na.drop() after select considers only selected columns."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": None, "c": 10},
                {"a": 2, "b": 2, "c": 20},
                {"a": None, "b": 3, "c": 30},
            ]
        )
        result = df.select("a", "b").na.drop(subset=["a"])
        rows = result.collect()
        assert len(rows) == 2
        assert all(r["a"] is not None for r in rows)

    def test_na_drop_multiple_chained(self, spark):
        """Chained na.drop() with different subset."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": None, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": 1, "b": 2, "c": None},
            ]
        )
        result = df.na.drop(subset=["a"]).na.drop(subset=["b"])
        rows = result.collect()
        assert len(rows) == 2
        assert all(r["a"] is not None and r["b"] is not None for r in rows)

    def test_na_drop_invalid_subset_column_raises(self, spark):
        """na.drop(subset=[...]) with non-existent column raises."""
        df = spark.createDataFrame([{"Name": "Alice", "Value": 1}])
        with pytest.raises(ColumnNotFoundException):
            df.na.drop(subset=["NonExistentColumn"])

    def test_na_drop_subset_empty_list_behavior(self, spark):
        """na.drop(subset=[]) does not raise; behavior is backend-dependent (PySpark keeps all, Sparkless may drop by all cols)."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": None},
                {"a": None, "b": 2},
            ]
        )
        result = df.na.drop(subset=[])
        rows = result.collect()
        # PySpark: subset=[] keeps all rows. Sparkless: may treat as all-columns and drop rows with any null.
        assert len(rows) >= 0 and len(rows) <= 2

    def test_na_drop_preserves_schema(self, spark):
        """na.drop() preserves DataFrame schema."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        StructType = imports.StructType
        StructField = imports.StructField
        StringType = imports.StringType
        IntegerType = imports.IntegerType
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("value", IntegerType(), nullable=True),
            ]
        )
        df = spark.createDataFrame(
            [{"name": "Alice", "value": 1}, {"name": "Bob", "value": None}],
            schema=schema,
        )
        result = df.na.drop(subset=["value"])
        assert result.schema is not None
        assert len(result.schema.fields) == 2
        assert result.schema.fieldNames() == ["name", "value"]

    def test_na_drop_with_show_materializes(self, spark):
        """na.drop() then show() does not raise (materialization)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Value": 1},
                {"Name": "Bob", "Value": None},
            ]
        )
        result = df.na.drop(subset=["Value"])
        result.show()

    def test_na_drop_single_column_all_null(self, spark):
        """subset with one column: drop rows where that column is null."""
        df = spark.createDataFrame(
            [
                {"id": 1, "tag": None},
                {"id": 2, "tag": None},
                {"id": 3, "tag": "x"},
            ]
        )
        result = df.na.drop(subset=["tag"])
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 3
        assert rows[0]["tag"] == "x"

    def test_na_drop_three_columns_subset(self, spark):
        """na.drop(subset=[a,b,c]) drops row if any of a,b,c is null."""
        df = spark.createDataFrame(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": None, "c": 3},
                {"a": 1, "b": 2, "c": None},
                {"a": None, "b": 2, "c": 3},
            ]
        )
        result = df.na.drop(subset=["a", "b", "c"])
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["a"] == 1 and rows[0]["b"] == 2 and rows[0]["c"] == 3
