"""Tests for issue #188: String concatenation cache handling edge cases.

This module tests edge cases for string concatenation cache handling.
Uses get_spark_imports from fixture only.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F


class TestStringConcatenationCacheEdgeCases:
    """Test edge cases for string concatenation cache handling."""

    @pytest.fixture
    def spark(self, request):
        """Use conftest spark fixture."""
        return request.getfixturevalue("spark")

    def test_string_concat_with_empty_strings(self, spark):
        """Test string concatenation with empty strings in cached DataFrame."""
        df = spark.createDataFrame([("", ""), ("a", ""), ("", "b")], ["col1", "col2"])

        # String concatenation with + operator
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        results = df2_cached.collect()

        # In PySpark, using + with string columns attempts numeric addition;
        # non-numeric strings are cast to null, so all results are None.
        assert [result["concat"] for result in results] == [None, None, None], (
            "String-like addition with + yields nulls for non-numeric strings when cached"
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
        assert (
            results[0]["concat"] is None
            and results[1]["concat"] is None
            and results[2]["concat"] is None
        ), "String concatenation with None values when cached"

    def test_nested_string_concat(self, spark):
        """Test nested string concatenation operations in cached DataFrame."""
        df = spark.createDataFrame([("a", "b", "c")], ["col1", "col2", "col3"])

        # Nested string concatenation: (col1 + col2) + col3
        df2 = df.withColumn("concat", (F.col("col1") + F.col("col2")) + F.col("col3"))

        # Cache the DataFrame
        df2_cached = df2.cache()
        _ = df2_cached.count()  # Force materialization

        result = df2_cached.collect()[0]

        # In PySpark, nested + on string columns still performs numeric-style
        # addition; here it yields null.
        assert result["concat"] is None, "Nested string addition yields null in PySpark"

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

        # In PySpark, string + literal also attempts numeric addition and
        # produces null for non-numeric strings.
        assert result["greeting"] is None, (
            "String + literal yields null for non-numeric strings in PySpark"
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

        # Multiple string concat columns are all null when using + on strings.
        assert result["concat1"] is None, "First string concat column yields null"
        assert result["concat2"] is None, "Second string concat column yields null"

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

        # String concat in select yields null when using + on strings.
        assert result["concat"] is None, "String addition in select yields null"

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

        # With PySpark semantics, the initial + on strings yields null, so the
        # filter on isNotNull() drops all rows.
        assert results == [], "Chained operations yield no rows under PySpark + semantics"

    def test_string_concat_without_caching(self, spark):
        """Test that string concatenation works normally without caching."""
        df = spark.createDataFrame([("a", "b")], ["col1", "col2"])

        # String concatenation with + operator (NOT cached)
        df2 = df.withColumn("concat", F.col("col1") + F.col("col2"))

        result = df2.collect()[0]

        # Even without caching, + on string columns behaves as numeric addition
        # and yields null for non-numeric strings.
        assert result["concat"] is None, (
            "String addition with + yields null for non-numeric strings without caching"
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
