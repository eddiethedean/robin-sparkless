"""Tests for issue #188: String concatenation cache handling edge cases.

This module tests edge cases and potential logic bugs in the string
concatenation cache handling logic.
"""

import pytest
from sparkless import SparkSession
from sparkless.functions import F


class TestStringConcatenationCacheEdgeCases:
    """Test edge cases for string concatenation cache handling."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("string_concat_cache_test")
        yield session
        session.stop()

    def test_string_concat_with_empty_strings(self, spark):
        """Test string concatenation with empty strings in cached DataFrame."""
        df = spark.createDataFrame([("", ""), ("a", ""), ("", "b")], ["col1", "col2"])

        # String concatenation with + operator
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        results = df2_cached.collect()

        # String concat with empty strings: ""+""="", "a"+""="a", ""+"b"="b" (correct behavior)
        assert [result["concat"] for result in results] == ["", "a", "b"], (
            "String concatenation with empty strings when cached"
        )

    def test_string_concat_with_none_values(self, spark):
        """Test string concatenation with None values in cached DataFrame."""
        df = spark.createDataFrame(
            [("a", None), (None, "b"), (None, None)], ["col1", "col2"]
        )

        # String concatenation with + operator
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        results = df2_cached.collect()

        # String concat with None: None+anything => None (null propagation)
        assert results[0]["concat"] is None and results[1]["concat"] is None and results[2]["concat"] is None, (
            "String concatenation with None values when cached"
        )

    def test_nested_string_concat(self, spark):
        """Test nested string concatenation operations in cached DataFrame."""
        df = spark.createDataFrame([("a", "b", "c")], ["col1", "col2", "col3"])

        # Nested string concatenation: (col1 + col2) + col3
        df2 = df.withColumn("concat", (F.col("col1") + F.col("col2")) + F.col("col3"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        result = df2_cached.collect()[0]

        # Nested string concat (col1+col2)+col3 => "abc"
        assert result["concat"] == "abc", (
            "Nested string concatenation when cached"
        )

    def test_string_concat_vs_numeric_addition(self, spark):
        """Test that numeric addition is NOT treated as string concatenation."""
        df = spark.createDataFrame([(1, 2), (3, 4)], ["col1", "col2"])

        # Numeric addition with + operator (should NOT be affected by cache)
        df2 = df.withColumn("sum", F.col("col1") + F.col("col2"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        results = df2_cached.collect()

        # Numeric addition should work correctly (not None)
        # The heuristic checks if result is string before setting to None
        assert results[0]["sum"] == 3, (
            "Numeric addition should not be affected by cache"
        )
        assert results[1]["sum"] == 7, (
            "Numeric addition should not be affected by cache"
        )

    def test_string_concat_with_literal(self, spark):
        """Test string concatenation with literal strings in cached DataFrame."""
        df = spark.createDataFrame([("John",)], ["name"])

        # String concatenation with literal
        df2 = df.withColumn("greeting", F.col("name") + F.lit(" Doe"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        result = df2_cached.collect()[0]

        # String concat with literal: "John" + " Doe" => "John Doe"
        assert result["greeting"] == "John Doe", (
            "String concatenation with literal when cached"
        )

    def test_multiple_string_concat_columns(self, spark):
        """Test multiple string concatenation columns in cached DataFrame."""
        df = spark.createDataFrame(
            [("a", "b", "c", "d")], ["col1", "col2", "col3", "col4"]
        )

        # Multiple string concatenation columns
        df2 = df.withColumn("concat1", F.col("col1") + F.col("col2")).withColumn(
            "concat2", F.col("col3") + F.col("col4")
        )

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        result = df2_cached.collect()[0]

        # Multiple string concat columns: "ab" and "cd"
        assert result["concat1"] == "ab", (
            "First string concat column when cached"
        )
        assert result["concat2"] == "cd", (
            "Second string concat column when cached"
        )

    def test_string_concat_with_select(self, spark):
        """Test string concatenation followed by select in cached DataFrame."""
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # String concatenation with + operator
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        # Select the concatenated column
        df3 = df2.select("concat")

        # Cache the DataFrame
        df3_cached = df3.cache()
        _ = df3_cached.count()  # Force materialization

        result = df3_cached.collect()[0]

        # String concat in select when cached still returns "ab"
        assert result["concat"] == "ab", (
            "String concatenation in select when cached"
        )

    def test_string_concat_chained_operations(self, spark):
        """Test string concatenation with chained operations in cached DataFrame."""
        df = spark.createDataFrame([("a", "b", "c")], ["col1", "col2", "col3"])

        # Chain multiple operations
        df2 = (
            df.withColumn("concat", F.col("col1") + F.col("col2"))
            .filter(F.col("concat").isNotNull())
            .withColumn("full", F.col("concat") + F.col("col3"))
        )

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        results = df2_cached.collect()

        # Chained concat: col1+col2="ab", then "ab"+col3="abc"
        if results:
            assert results[0]["concat"] == "ab", (
                "Chained string concat when cached"
            )
            assert results[0]["full"] == "abc", (
                "Nested string concat in chain when cached"
            )

    def test_string_concat_without_caching(self, spark):
        """Test that string concatenation works normally without caching."""
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # String concatenation with + operator (NOT cached)
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        result = df2.collect()[0]

        # Should work normally without caching
        assert result["concat"] == "ab", (
            "String concatenation should work normally without caching"
        )

    def test_concat_function_with_caching(self, spark):
        """Test that F.concat() works correctly even with caching."""
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # Using F.concat() (recommended approach)
        df2 = df.withColumn("concat", F.concat(F.col("col1"), F.col("col2")))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        result = df2_cached.collect()[0]

        # F.concat() should work correctly even when cached
        assert result["concat"] == "ab", (
            "F.concat() should work correctly even when DataFrame is cached"
        )
