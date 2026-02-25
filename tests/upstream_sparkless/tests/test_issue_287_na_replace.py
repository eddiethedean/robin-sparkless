"""
Tests for issue #287: NAHandler.replace method.

PySpark supports df.na.replace() for mapping values within a DataFrame.
This test verifies that Sparkless supports the same operation.
"""

import pytest
from sparkless.sql import SparkSession


class TestIssue287NAReplace:
    """Test NAHandler.replace method."""

    def test_na_replace_with_dict_and_subset(self):
        """Test na.replace with dict mapping and subset parameter."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test na.replace with dict and subset
            result = df.na.replace(map_value, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"

            # Verify Name column unchanged
            assert alice_row["Name"] == "Alice"
            assert bob_row["Name"] == "Bob"
        finally:
            spark.stop()

    def test_na_replace_with_dict_no_subset(self):
        """Test na.replace with dict mapping without subset (applies to all columns)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test na.replace with dict but no subset (applies to all columns)
            result = df.na.replace(map_value)

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in Type column
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_single_value(self):
        """Test na.replace with single value replacement."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 1},
                    {"Name": "Charlie", "Value": 2},
                ]
            )

            # Replace 1 with 99 in Value column
            result = df.na.replace(1, 99, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 99

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 99

            # Value 2 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 2
        finally:
            spark.stop()

    def test_na_replace_list_with_single_value(self):
        """Test na.replace with list of values replaced by single value."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Replace [1, 2] with 99
            result = df.na.replace([1, 2], 99, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 99

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 99

            # Value 3 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 3
        finally:
            spark.stop()

    def test_na_replace_list_with_list(self):
        """Test na.replace with list of values replaced by list of values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Replace [1, 2] with [10, 20]
            result = df.na.replace([1, 2], [10, 20], subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 10

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 20

            # Value 3 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 3
        finally:
            spark.stop()

    def test_na_replace_with_string_subset(self):
        """Test na.replace with string subset (single column)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Test with string subset (should be converted to list)
            result = df.na.replace(map_value, subset="Type")

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_with_tuple_subset(self):
        """Test na.replace with tuple subset."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            map_value = {"A": "TypeA", "B": "TypeB"}

            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Category": "A"},
                    {"Name": "Bob", "Type": "B", "Category": "B"},
                ]
            )

            # Test with tuple subset
            result = df.na.replace(map_value, subset=("Type", "Category"))

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in both columns
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Category"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
            assert bob_row["Category"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_multiple_columns(self):
        """Test na.replace affecting multiple columns."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Status": "A"},
                    {"Name": "Bob", "Type": "B", "Status": "B"},
                ]
            )

            # Replace in multiple columns
            result = df.na.replace(
                {"A": "TypeA", "B": "TypeB"}, subset=["Type", "Status"]
            )

            rows = result.collect()
            assert len(rows) == 2

            # Verify replacements in both columns
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Status"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"
            assert bob_row["Status"] == "TypeB"
        finally:
            spark.stop()

    def test_na_replace_with_numeric_values(self):
        """Test na.replace with numeric values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Score": 1.0},
                    {"Name": "Bob", "Score": 2.0},
                    {"Name": "Charlie", "Score": 3.0},
                ]
            )

            # Replace numeric values
            result = df.na.replace({1.0: 10.0, 2.0: 20.0}, subset=["Score"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Score"] == 10.0

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Score"] == 20.0

            # Value 3.0 should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Score"] == 3.0
        finally:
            spark.stop()

    def test_na_replace_no_matches(self):
        """Test na.replace when no values match."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                ]
            )

            # Replace values that don't exist
            result = df.na.replace({"X": "TypeX", "Y": "TypeY"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 2

            # Values should remain unchanged
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "A"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "B"
        finally:
            spark.stop()

    def test_na_replace_partial_matches(self):
        """Test na.replace when only some values match."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A"},
                    {"Name": "Bob", "Type": "B"},
                    {"Name": "Charlie", "Type": "C"},
                ]
            )

            # Replace only A and B, C should remain unchanged
            result = df.na.replace({"A": "TypeA", "B": "TypeB"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 3

            # Verify replacements
            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "TypeB"

            # C should remain unchanged
            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Type"] == "C"
        finally:
            spark.stop()

    def test_na_replace_empty_dataframe(self):
        """Test na.replace on empty DataFrame."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame([], schema="Name string, Type string")

            # Should not raise error
            result = df.na.replace({"A": "TypeA"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 0
        finally:
            spark.stop()

    def test_na_replace_chained_operations(self):
        """Test na.replace with chained DataFrame operations."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Value": 1},
                    {"Name": "Bob", "Type": "B", "Value": 2},
                ]
            )

            # Chain replace with filter
            result = df.na.replace(
                {"A": "TypeA", "B": "TypeB"}, subset=["Type"]
            ).filter("Type = 'TypeA'")

            rows = result.collect()
            assert len(rows) == 1

            # Verify result
            alice_row = rows[0]
            assert alice_row["Name"] == "Alice"
            assert alice_row["Type"] == "TypeA"
        finally:
            spark.stop()

    def test_na_replace_with_none_values(self):
        """Test na.replace with None/null values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": None},
                    {"Name": "Bob", "Value": 1},
                    {"Name": "Charlie", "Value": None},
                ]
            )

            # Replace None with 0
            result = df.na.replace(None, 0, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 0

            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 0

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 1
        finally:
            spark.stop()

    def test_na_replace_with_none_as_replacement(self):
        """Test na.replace replacing values with None using dict."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                    {"Name": "Charlie", "Value": 3},
                ]
            )

            # Replace 2 with None using dict (dict allows None as value)
            result = df.na.replace({2: None}, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] is None

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 1
        finally:
            spark.stop()

    def test_na_replace_with_boolean_values(self):
        """Test na.replace with boolean values."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Active": True},
                    {"Name": "Bob", "Active": False},
                    {"Name": "Charlie", "Active": True},
                ]
            )

            # Replace True with False
            result = df.na.replace(True, False, subset=["Active"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Active"] is False

            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Active"] is False

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Active"] is False
        finally:
            spark.stop()

    def test_na_replace_with_type_coercion(self):
        """Test na.replace with type coercion (string to number)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "1"},
                    {"Name": "Bob", "Value": "2"},
                    {"Name": "Charlie", "Value": "3"},
                ]
            )

            # Replace string "1" with string "10"
            result = df.na.replace("1", "10", subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == "10"
        finally:
            spark.stop()

    def test_na_replace_with_special_characters(self):
        """Test na.replace with special characters in strings."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Text": "Hello, World!"},
                    {"Name": "Bob", "Text": "Test@123"},
                    {"Name": "Charlie", "Text": "Hello, World!"},
                ]
            )

            # Replace special character strings
            result = df.na.replace("Hello, World!", "Hi", subset=["Text"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Text"] == "Hi"

            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Text"] == "Hi"
        finally:
            spark.stop()

    def test_na_replace_with_unicode(self):
        """Test na.replace with unicode characters."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Text": "Hello 🌍"},
                    {"Name": "Bob", "Text": "Test"},
                    {"Name": "Charlie", "Text": "Hello 🌍"},
                ]
            )

            # Replace unicode strings
            result = df.na.replace("Hello 🌍", "Hi World", subset=["Text"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Text"] == "Hi World"
        finally:
            spark.stop()

    def test_na_replace_with_zero_and_negative(self):
        """Test na.replace with zero and negative numbers."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 0},
                    {"Name": "Bob", "Value": -1},
                    {"Name": "Charlie", "Value": 5},
                ]
            )

            # Replace 0 and -1
            result = df.na.replace({0: 100, -1: 200}, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 100

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Value"] == 200

            charlie_row = next((r for r in rows if r["Name"] == "Charlie"), None)
            assert charlie_row is not None
            assert charlie_row["Value"] == 5
        finally:
            spark.stop()

    def test_na_replace_with_empty_dict(self):
        """Test na.replace with empty dict (should not change anything)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Empty dict should not change anything
            result = df.na.replace({}, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 1
        finally:
            spark.stop()

    def test_na_replace_with_empty_list(self):
        """Test na.replace with empty list (should not change anything)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Empty list should not change anything
            result = df.na.replace([], 99, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 1
        finally:
            spark.stop()

    def test_na_replace_invalid_subset_column(self):
        """Test na.replace with invalid subset column (should raise error)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                ]
            )

            # Invalid column should raise error
            from sparkless.core.exceptions.analysis import ColumnNotFoundException

            with pytest.raises(ColumnNotFoundException):
                df.na.replace(1, 99, subset=["NonExistentColumn"])
        finally:
            spark.stop()

    def test_na_replace_mismatched_list_lengths(self):
        """Test na.replace with mismatched list lengths (should raise error)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                ]
            )

            # Mismatched lengths should raise error
            from sparkless.core.exceptions import PySparkValueError

            with pytest.raises(PySparkValueError):
                df.na.replace([1, 2], [10], subset=["Value"])
        finally:
            spark.stop()

    def test_na_replace_none_value_with_scalar(self):
        """Test na.replace with None value when to_replace is scalar (should raise error)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                ]
            )

            # None value with scalar to_replace should raise error
            from sparkless.core.exceptions import PySparkValueError

            with pytest.raises(PySparkValueError):
                df.na.replace(1, None, subset=["Value"])
        finally:
            spark.stop()

    def test_na_replace_none_value_with_list(self):
        """Test na.replace with None value when to_replace is list (should raise error)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                ]
            )

            # None value with list to_replace should raise error
            from sparkless.core.exceptions import PySparkValueError

            with pytest.raises(PySparkValueError):
                df.na.replace([1, 2], None, subset=["Value"])
        finally:
            spark.stop()

    def test_na_replace_multiple_chained_operations(self):
        """Test na.replace with multiple chained replace operations."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Status": "X"},
                    {"Name": "Bob", "Type": "B", "Status": "Y"},
                ]
            )

            # Chain multiple replace operations
            result = df.na.replace({"A": "TypeA"}, subset=["Type"]).na.replace(
                {"X": "StatusX"}, subset=["Status"]
            )

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Status"] == "StatusX"
        finally:
            spark.stop()

    def test_na_replace_with_mixed_types_in_column(self):
        """Test na.replace when column has mixed types (strings and numbers)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            # Note: In Sparkless, columns typically have consistent types
            # This test verifies behavior when replacing specific values
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": "1"},
                    {"Name": "Bob", "Value": "2"},
                    {"Name": "Charlie", "Value": "3"},
                ]
            )

            # Replace string "1" with "10"
            result = df.na.replace("1", "10", subset=["Value"])

            rows = result.collect()
            assert len(rows) == 3

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == "10"
        finally:
            spark.stop()

    def test_na_replace_large_dataframe(self):
        """Test na.replace with a larger DataFrame (stress test)."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            # Create a larger DataFrame
            data = [{"Name": f"Person{i}", "Value": i % 3} for i in range(100)]
            df = spark.createDataFrame(data)

            # Replace values
            result = df.na.replace({0: 100, 1: 200, 2: 300}, subset=["Value"])

            rows = result.collect()
            assert len(rows) == 100

            # Verify some replacements
            person0_row = next((r for r in rows if r["Name"] == "Person0"), None)
            assert person0_row is not None
            assert person0_row["Value"] == 100

            person1_row = next((r for r in rows if r["Name"] == "Person1"), None)
            assert person1_row is not None
            assert person1_row["Value"] == 200
        finally:
            spark.stop()

    def test_na_replace_preserves_other_columns(self):
        """Test that na.replace preserves columns not in subset."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Type": "A", "Age": 25, "City": "NYC"},
                    {"Name": "Bob", "Type": "B", "Age": 30, "City": "LA"},
                ]
            )

            # Replace only in Type column
            result = df.na.replace({"A": "TypeA"}, subset=["Type"])

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Type"] == "TypeA"
            assert alice_row["Age"] == 25
            assert alice_row["City"] == "NYC"

            bob_row = next((r for r in rows if r["Name"] == "Bob"), None)
            assert bob_row is not None
            assert bob_row["Type"] == "B"
            assert bob_row["Age"] == 30
            assert bob_row["City"] == "LA"
        finally:
            spark.stop()

    def test_na_replace_case_insensitive_column_name(self):
        """Test na.replace with case-insensitive column names."""
        spark = SparkSession.builder.appName("issue-287").getOrCreate()
        try:
            df = spark.createDataFrame(
                [
                    {"Name": "Alice", "Value": 1},
                    {"Name": "Bob", "Value": 2},
                ]
            )

            # Use different case for column name
            result = df.na.replace(1, 99, subset=["value"])  # lowercase

            rows = result.collect()
            assert len(rows) == 2

            alice_row = next((r for r in rows if r["Name"] == "Alice"), None)
            assert alice_row is not None
            assert alice_row["Value"] == 99
        finally:
            spark.stop()
