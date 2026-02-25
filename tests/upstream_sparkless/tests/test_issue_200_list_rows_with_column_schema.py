"""
Test for issue #200: createDataFrame accepts list rows (not just tuples) with column schema.

Issue #200 reports that sparkless raises "ValueError: Some of types cannot be determined after inferring"
when creating a DataFrame with data as a list of lists (not tuples) and schema as a list of column names.
PySpark accepts any sequence (list, tuple, etc.) as positional rows, so sparkless should too.

This test verifies that:
1. List rows work with column name schema (the exact issue reproduction)
2. Tuple rows still work (regression test)
3. Mixed list/tuple rows work
4. Edge cases are handled correctly
"""

from sparkless import SparkSession


class TestIssue200ListRowsWithColumnSchema:
    """Test cases for issue #200: list rows with column name schema."""

    def test_createDataFrame_list_rows_with_column_schema(self):
        """Test that createDataFrame accepts list rows (not just tuples) with column schema."""
        spark = SparkSession("test")

        # Exact reproduction from issue #200
        df = spark.createDataFrame(
            [
                ["value1A", "value2A", "value3A"],
                ["value1B", "value2B", "value3B"],
            ],
            ["column1", "column2", "column3"],
        )

        assert df.count() == 2
        assert df.columns == ["column1", "column2", "column3"]
        rows = df.collect()
        assert rows[0]["column1"] == "value1A"
        assert rows[0]["column2"] == "value2A"
        assert rows[0]["column3"] == "value3A"
        assert rows[1]["column1"] == "value1B"
        assert rows[1]["column2"] == "value2B"
        assert rows[1]["column3"] == "value3B"

        spark.stop()

    def test_createDataFrame_tuple_rows_still_work(self):
        """Regression test: ensure tuple rows still work as before."""
        spark = SparkSession("test")

        # Existing tuple-based code should continue to work
        df = spark.createDataFrame(
            [("value1A", "value2A", "value3A"), ("value1B", "value2B", "value3B")],
            ["column1", "column2", "column3"],
        )

        assert df.count() == 2
        assert df.columns == ["column1", "column2", "column3"]
        rows = df.collect()
        assert rows[0]["column1"] == "value1A"
        assert rows[1]["column1"] == "value1B"

        spark.stop()

    def test_createDataFrame_mixed_list_and_tuple_rows(self):
        """Test that mixed list and tuple rows work together."""
        spark = SparkSession("test")

        # Mix of lists and tuples should work
        df = spark.createDataFrame(
            [
                ["value1A", "value2A"],
                ("value1B", "value2B"),
                ["value1C", "value2C"],
            ],
            ["column1", "column2"],
        )

        assert df.count() == 3
        assert df.columns == ["column1", "column2"]
        rows = df.collect()
        assert rows[0]["column1"] == "value1A"
        assert rows[1]["column1"] == "value1B"
        assert rows[2]["column1"] == "value1C"

        spark.stop()

    def test_createDataFrame_list_rows_with_different_data_types(self):
        """Test list rows with various data types."""
        spark = SparkSession("test")

        df = spark.createDataFrame(
            [
                ["Alice", 25, 50000.5, True],
                ["Bob", 30, 60000.0, False],
            ],
            ["name", "age", "salary", "active"],
        )

        assert df.count() == 2
        rows = df.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25
        assert rows[0]["salary"] == 50000.5
        assert rows[0]["active"] is True
        assert rows[1]["name"] == "Bob"
        assert rows[1]["age"] == 30

        spark.stop()

    def test_createDataFrame_single_list_row(self):
        """Test with a single list row."""
        spark = SparkSession("test")

        df = spark.createDataFrame([["Alice", 25]], ["name", "age"])

        assert df.count() == 1
        rows = df.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25

        spark.stop()

    def test_createDataFrame_list_rows_with_none_values(self):
        """Test list rows with None values."""
        spark = SparkSession("test")

        df = spark.createDataFrame(
            [
                ["Alice", 25, None],
                ["Bob", None, 60000.0],
                [None, 30, 70000.0],
            ],
            ["name", "age", "salary"],
        )

        assert df.count() == 3
        rows = df.collect()
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 25
        assert rows[0]["salary"] is None
        assert rows[1]["name"] == "Bob"
        assert rows[1]["age"] is None
        assert rows[1]["salary"] == 60000.0
        assert rows[2]["name"] is None

        spark.stop()
