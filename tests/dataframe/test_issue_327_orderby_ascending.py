"""
Unit tests for Issue #327: orderBy() missing ascending parameter.

Tests the orderBy() method with ascending parameter support.
"""

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
F = _imports.F


class TestIssue327OrderByAscending:
    """Test orderBy() with ascending parameter."""

    def test_orderby_ascending_true(self):
        """Test orderBy with ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "AAA"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "ZZZ"
        finally:
            spark.stop()

    def test_orderby_ascending_false(self):
        """Test orderBy with ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "ZZZ"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "AAA"
        finally:
            spark.stop()

    def test_orderby_default_ascending(self):
        """Test orderBy without ascending parameter (defaults to True)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.orderBy("StringValue")
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "AAA"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "ZZZ"
        finally:
            spark.stop()

    def test_orderby_numeric_ascending(self):
        """Test orderBy with numeric column and ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 5
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()

    def test_orderby_numeric_descending(self):
        """Test orderBy with numeric column and ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy("Value", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_multiple_columns_ascending(self):
        """Test orderBy with multiple columns and ascending=True."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Category": "A", "Value": 10},
                    {"Name": "Bob", "Category": "A", "Value": 5},
                    {"Name": "Charlie", "Category": "B", "Value": 20},
                ]
            )

            result = df.orderBy("Category", "Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            # First by Category (A, A, B), then by Value within same Category
            assert rows[0]["Category"] == "A"
            assert rows[0]["Value"] == 5
            assert rows[1]["Category"] == "A"
            assert rows[1]["Value"] == 10
            assert rows[2]["Category"] == "B"
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()

    def test_orderby_multiple_columns_descending(self):
        """Test orderBy with multiple columns and ascending=False."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Category": "A", "Value": 10},
                    {"Name": "Bob", "Category": "A", "Value": 5},
                    {"Name": "Charlie", "Category": "B", "Value": 20},
                ]
            )

            result = df.orderBy("Category", "Value", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            # Both columns in descending order
            assert rows[0]["Category"] == "B"
            assert rows[0]["Value"] == 20
            assert rows[1]["Category"] == "A"
            assert rows[1]["Value"] == 10
            assert rows[2]["Category"] == "A"
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_with_column_object(self):
        """Test orderBy with Column object and ascending parameter."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy(F.col("Value"), ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_sort_with_ascending_parameter(self):
        """Test sort() alias with ascending parameter."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "StringValue": "AAA"},
                    {"Name": "Bob", "StringValue": "ZZZ"},
                    {"Name": "Charlie", "StringValue": "MMM"},
                ]
            )

            result = df.sort("StringValue", ascending=False)
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["StringValue"] == "ZZZ"
            assert rows[1]["StringValue"] == "MMM"
            assert rows[2]["StringValue"] == "AAA"
        finally:
            spark.stop()

    def test_orderby_with_null_values(self):
        """Test orderBy with null values (explicit nulls last for PySpark)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": None},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            result = df.orderBy(F.asc_nulls_last("Value"))
            rows = result.collect()

            assert len(rows) == 3
            values = [r["Value"] for r in rows]
            assert 10 in values and 20 in values and None in values
            assert values[-1] is None  # nulls last
        finally:
            spark.stop()

    def test_orderby_backward_compatibility(self):
        """Test that orderBy without ascending still works (backward compatibility)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            # Should work without ascending parameter (defaults to True)
            result = df.orderBy("Value")
            rows = result.collect()

            assert len(rows) == 3
            assert rows[0]["Value"] == 5
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 20
        finally:
            spark.stop()

    def test_orderby_empty_dataframe(self):
        """Test orderBy with empty DataFrame."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame([], schema="Name string, Value int")

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 0
        finally:
            spark.stop()

    def test_orderby_single_row(self):
        """Test orderBy with single row DataFrame."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame([{"Name": "Alice", "Value": 10}])

            result_asc = df.orderBy("Value", ascending=True)
            result_desc = df.orderBy("Value", ascending=False)
            rows_asc = result_asc.collect()
            rows_desc = result_desc.collect()

            assert len(rows_asc) == 1
            assert len(rows_desc) == 1
            assert rows_asc[0]["Value"] == 10
            assert rows_desc[0]["Value"] == 10
        finally:
            spark.stop()

    def test_orderby_all_null_values(self):
        """Test orderBy with all null values in column."""
        StructType = _imports.StructType
        StructField = _imports.StructField
        IntegerType = _imports.IntegerType
        StringType = _imports.StringType

        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            schema = StructType(
                [
                    StructField("Name", StringType(), True),
                    StructField("Value", IntegerType(), True),
                ]
            )
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Bob", "Value": None},
                    {"Name": "Charlie", "Value": None},
                ],
                schema,
            )

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 3
            # All nulls should remain in original order (or any order, but all should be None)
            assert all(row["Value"] is None for row in rows)
        finally:
            spark.stop()

    def test_orderby_mixed_nulls_and_values(self):
        """Test orderBy with mixed null and non-null values (explicit nulls last)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": None},
                    {"Name": "David", "Value": 10},
                ]
            )

            result_asc = df.orderBy(F.asc_nulls_last("Value"))
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            values_asc = [r["Value"] for r in rows_asc]
            assert values_asc[:2] == [5, 10]
            assert values_asc[2] is None and values_asc[3] is None

            result_desc = df.orderBy(F.desc_nulls_last("Value"))
            rows_desc = result_desc.collect()

            assert len(rows_desc) == 4
            values_desc = [r["Value"] for r in rows_desc]
            assert values_desc[:2] == [10, 5]
            assert values_desc[2] is None and values_desc[3] is None
        finally:
            spark.stop()

    def test_orderby_negative_numbers(self):
        """Test orderBy with negative numbers."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": -10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": -5},
                    {"Name": "David", "Value": 0},
                ]
            )

            result_asc = df.orderBy("Value", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            assert rows_asc[0]["Value"] == -10
            assert rows_asc[1]["Value"] == -5
            assert rows_asc[2]["Value"] == 0
            assert rows_asc[3]["Value"] == 5

            result_desc = df.orderBy("Value", ascending=False)
            rows_desc = result_desc.collect()

            assert len(rows_desc) == 4
            assert rows_desc[0]["Value"] == 5
            assert rows_desc[1]["Value"] == 0
            assert rows_desc[2]["Value"] == -5
            assert rows_desc[3]["Value"] == -10
        finally:
            spark.stop()

    def test_orderby_floating_point(self):
        """Test orderBy with floating point numbers."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10.5},
                    {"Name": "Bob", "Value": 5.25},
                    {"Name": "Charlie", "Value": 20.75},
                    {"Name": "David", "Value": 5.1},
                ]
            )

            result_asc = df.orderBy("Value", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            assert rows_asc[0]["Value"] == 5.1
            assert rows_asc[1]["Value"] == 5.25
            assert rows_asc[2]["Value"] == 10.5
            assert rows_asc[3]["Value"] == 20.75
        finally:
            spark.stop()

    def test_orderby_boolean_column(self):
        """Test orderBy with boolean column."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Active": True},
                    {"Name": "Bob", "Active": False},
                    {"Name": "Charlie", "Active": True},
                    {"Name": "David", "Active": False},
                ]
            )

            result_asc = df.orderBy("Active", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            # False comes before True in ascending order
            assert rows_asc[0]["Active"] is False
            assert rows_asc[1]["Active"] is False
            assert rows_asc[2]["Active"] is True
            assert rows_asc[3]["Active"] is True

            result_desc = df.orderBy("Active", ascending=False)
            rows_desc = result_desc.collect()

            assert len(rows_desc) == 4
            # True comes before False in descending order
            assert rows_desc[0]["Active"] is True
            assert rows_desc[1]["Active"] is True
            assert rows_desc[2]["Active"] is False
            assert rows_desc[3]["Active"] is False
        finally:
            spark.stop()

    def test_orderby_unicode_strings(self):
        """Test orderBy with Unicode strings."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "café"},
                    {"Name": "Bob", "Value": "zebra"},
                    {"Name": "Charlie", "Value": "αβγ"},
                    {"Name": "David", "Value": "café"},
                ]
            )

            result_asc = df.orderBy("Value", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            # Should sort Unicode correctly (actual order depends on Unicode code points)
            # Just verify all values are present and sorted
            values = [row["Value"] for row in rows_asc]
            assert "αβγ" in values
            assert "café" in values
            assert "zebra" in values
            # Verify it's actually sorted (each value should be <= next)
            for i in range(len(values) - 1):
                assert values[i] <= values[i + 1]
        finally:
            spark.stop()

    def test_orderby_special_characters(self):
        """Test orderBy with special characters in values."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "A-B"},
                    {"Name": "Bob", "Value": "A_B"},
                    {"Name": "Charlie", "Value": "A B"},
                    {"Name": "David", "Value": "A+B"},
                ]
            )

            result_asc = df.orderBy("Value", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 4
            # Should sort special characters correctly
            assert rows_asc[0]["Value"] == "A B"
            assert rows_asc[1]["Value"] == "A+B"
            assert rows_asc[2]["Value"] == "A-B"
            assert rows_asc[3]["Value"] == "A_B"
        finally:
            spark.stop()

    def test_orderby_very_long_strings(self):
        """Test orderBy with very long strings."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            long_string1 = "A" * 1000
            long_string2 = "B" * 1000
            long_string3 = "C" * 1000

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": long_string3},
                    {"Name": "Bob", "Value": long_string1},
                    {"Name": "Charlie", "Value": long_string2},
                ]
            )

            result_asc = df.orderBy("Value", ascending=True)
            rows_asc = result_asc.collect()

            assert len(rows_asc) == 3
            assert rows_asc[0]["Value"] == long_string1
            assert rows_asc[1]["Value"] == long_string2
            assert rows_asc[2]["Value"] == long_string3
        finally:
            spark.stop()

    def test_orderby_chained_operations(self):
        """Test orderBy chained with other operations."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10, "Category": "A"},
                    {"Name": "Bob", "Value": 5, "Category": "B"},
                    {"Name": "Charlie", "Value": 20, "Category": "A"},
                ]
            )

            # Chain filter, orderBy, and select
            result = (
                df.filter(F.col("Category") == "A")
                .orderBy("Value", ascending=False)
                .select("Name", "Value")
            )
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Name"] == "Charlie"
            assert rows[0]["Value"] == 20
            assert rows[1]["Name"] == "Alice"
            assert rows[1]["Value"] == 10
        finally:
            spark.stop()

    def test_orderby_multiple_orderby_calls(self):
        """Test multiple orderBy calls (last one wins)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            # Multiple orderBy calls - last one should win
            result = df.orderBy("Value", ascending=True).orderBy(
                "Value", ascending=False
            )
            rows = result.collect()

            assert len(rows) == 3
            # Should be descending (last orderBy wins)
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 10
            assert rows[2]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_with_limit(self):
        """Test orderBy combined with limit."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                    {"Name": "David", "Value": 15},
                ]
            )

            result = df.orderBy("Value", ascending=False).limit(2)
            rows = result.collect()

            assert len(rows) == 2
            assert rows[0]["Value"] == 20
            assert rows[1]["Value"] == 15
        finally:
            spark.stop()

    def test_orderby_case_insensitive_column_name(self):
        """Test orderBy with case-insensitive column name resolution."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 5},
                    {"Name": "Charlie", "Value": 20},
                ]
            )

            # Try different case variations
            result1 = df.orderBy("value", ascending=True)
            result2 = df.orderBy("VALUE", ascending=True)
            rows1 = result1.collect()
            rows2 = result2.collect()

            assert len(rows1) == 3
            assert len(rows2) == 3
            # Should work with case-insensitive column names
            assert rows1[0]["Value"] == 5
            assert rows2[0]["Value"] == 5
        finally:
            spark.stop()

    def test_orderby_three_columns(self):
        """Test orderBy with three columns."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"A": 1, "B": 1, "C": 3},
                    {"A": 1, "B": 1, "C": 1},
                    {"A": 1, "B": 2, "C": 2},
                    {"A": 2, "B": 1, "C": 1},
                ]
            )

            result = df.orderBy("A", "B", "C", ascending=True)
            rows = result.collect()

            assert len(rows) == 4
            # Sort by A first, then B, then C
            assert rows[0]["A"] == 1 and rows[0]["B"] == 1 and rows[0]["C"] == 1
            assert rows[1]["A"] == 1 and rows[1]["B"] == 1 and rows[1]["C"] == 3
            assert rows[2]["A"] == 1 and rows[2]["B"] == 2 and rows[2]["C"] == 2
            assert rows[3]["A"] == 2 and rows[3]["B"] == 1 and rows[3]["C"] == 1
        finally:
            spark.stop()

    def test_orderby_duplicate_values(self):
        """Test orderBy with duplicate values (should maintain stability)."""
        spark = SparkSession.builder.appName("issue-327").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 10},
                    {"Name": "Bob", "Value": 10},
                    {"Name": "Charlie", "Value": 10},
                    {"Name": "David", "Value": 5},
                ]
            )

            result = df.orderBy("Value", ascending=True)
            rows = result.collect()

            assert len(rows) == 4
            # First should be the smallest unique value
            assert rows[0]["Value"] == 5
            # Remaining should all be 10 (order may vary for duplicates)
            assert all(row["Value"] == 10 for row in rows[1:])
        finally:
            spark.stop()
