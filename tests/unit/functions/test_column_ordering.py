"""
Tests for Column ordering methods with nulls handling.

These tests ensure that:
1. The desc_nulls_last(), desc_nulls_first(), asc_nulls_last(), asc_nulls_first() methods work correctly
2. Behavior matches PySpark API exactly
3. Integration with DataFrame operations (orderBy, sort) works
4. Nulls are placed in the correct position
5. Edge cases are handled properly (all nulls, no nulls, mixed types)

These tests work with both sparkless (mock) and PySpark backends.
Set MOCK_SPARK_TEST_BACKEND=pyspark to run with real PySpark.
"""

from tests.fixtures.spark_imports import get_spark_imports

# Get imports based on backend
imports = get_spark_imports()
SparkSession = imports.SparkSession
StringType = imports.StringType
IntegerType = imports.IntegerType
StructType = imports.StructType
StructField = imports.StructField
F = imports.F  # Functions module for backend-appropriate F.col() etc.


class TestColumnOrderingNulls:
    """Test Column ordering methods with nulls handling."""

    def test_desc_nulls_last_basic(self, spark):
        """Test desc_nulls_last() method with basic example from issue #240."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
                {"value": "D"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Verify order: D, C, B, A, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["value"] == "D"
        assert rows[1]["value"] == "C"
        assert rows[2]["value"] == "B"
        assert rows[3]["value"] == "A"
        assert rows[4]["value"] is None

    def test_desc_nulls_first(self, spark):
        """Test desc_nulls_first() method - nulls should come first."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
                {"value": "D"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_first())
        rows = result.collect()

        # Verify order: None, D, C, B, A (nulls first)
        assert len(rows) == 5
        assert rows[0]["value"] is None
        assert rows[1]["value"] == "D"
        assert rows[2]["value"] == "C"
        assert rows[3]["value"] == "B"
        assert rows[4]["value"] == "A"

    def test_asc_nulls_last(self, spark):
        """Test asc_nulls_last() method - nulls should come last."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
                {"value": "D"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_last())
        rows = result.collect()

        # Verify order: A, B, C, D, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["value"] == "A"
        assert rows[1]["value"] == "B"
        assert rows[2]["value"] == "C"
        assert rows[3]["value"] == "D"
        assert rows[4]["value"] is None

    def test_asc_nulls_first(self, spark):
        """Test asc_nulls_first() method - nulls should come first."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
                {"value": "D"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_first())
        rows = result.collect()

        # Verify order: None, A, B, C, D (nulls first)
        assert len(rows) == 5
        assert rows[0]["value"] is None
        assert rows[1]["value"] == "A"
        assert rows[2]["value"] == "B"
        assert rows[3]["value"] == "C"
        assert rows[4]["value"] == "D"

    def test_desc_nulls_last_integers(self, spark):
        """Test desc_nulls_last() with integer column."""
        schema = StructType([StructField("age", IntegerType(), True)])
        df = spark.createDataFrame(
            [
                {"age": 25},
                {"age": 30},
                {"age": None},
                {"age": 20},
                {"age": 35},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("age").desc_nulls_last())
        rows = result.collect()

        # Verify order: 35, 30, 25, 20, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["age"] == 35
        assert rows[1]["age"] == 30
        assert rows[2]["age"] == 25
        assert rows[3]["age"] == 20
        assert rows[4]["age"] is None

    def test_desc_nulls_last_all_nulls(self, spark):
        """Test desc_nulls_last() when all values are null."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": None},
                {"value": None},
                {"value": None},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # All rows should be None, order doesn't matter
        assert len(rows) == 3
        assert all(row["value"] is None for row in rows)

    def test_desc_nulls_last_no_nulls(self, spark):
        """Test desc_nulls_last() when no values are null."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": "C"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Should be sorted in descending order
        assert len(rows) == 3
        assert rows[0]["value"] == "C"
        assert rows[1]["value"] == "B"
        assert rows[2]["value"] == "A"

    def test_desc_nulls_last_multiple_nulls(self, spark):
        """Test desc_nulls_last() with multiple null values."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": None},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Verify order: C, B, A, None, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["value"] == "C"
        assert rows[1]["value"] == "B"
        assert rows[2]["value"] == "A"
        assert rows[3]["value"] is None
        assert rows[4]["value"] is None

    def test_asc_nulls_first_multiple_nulls(self, spark):
        """Test asc_nulls_first() with multiple null values."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": None},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_first())
        rows = result.collect()

        # Verify order: None, None, A, B, C (nulls first)
        assert len(rows) == 5
        assert rows[0]["value"] is None
        assert rows[1]["value"] is None
        assert rows[2]["value"] == "A"
        assert rows[3]["value"] == "B"
        assert rows[4]["value"] == "C"

    def test_desc_nulls_last_with_sort_method(self, spark):
        """Test desc_nulls_last() with sort() method alias."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "B"},
                {"value": None},
                {"value": "C"},
                {"value": "D"},
            ],
            schema=schema,
        )

        result = df.sort(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Verify order: D, C, B, A, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["value"] == "D"
        assert rows[1]["value"] == "C"
        assert rows[2]["value"] == "B"
        assert rows[3]["value"] == "A"
        assert rows[4]["value"] is None

    def test_column_methods_exist(self, spark):
        """Test that all ordering methods exist on Column objects."""
        col = F.col("test")

        # Test that methods exist
        assert hasattr(col, "desc_nulls_last")
        assert hasattr(col, "desc_nulls_first")
        assert hasattr(col, "asc_nulls_last")
        assert hasattr(col, "asc_nulls_first")

        # Test that they can be called and return something callable/usable
        # In PySpark mode, they return PySpark Column objects
        # In sparkless mode, they return ColumnOperation objects
        result1 = col.desc_nulls_last()
        result2 = col.desc_nulls_first()
        result3 = col.asc_nulls_last()
        result4 = col.asc_nulls_first()

        # Just verify they return something (not None)
        assert result1 is not None
        assert result2 is not None
        assert result3 is not None
        assert result4 is not None

    def test_multiple_columns_ordering(self, spark):
        """Test ordering with multiple columns using nulls variants."""
        schema = StructType(
            [
                StructField("category", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": None},
                {"category": None, "value": 20},
                {"category": "B", "value": 15},
                {"category": None, "value": 5},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("category").asc_nulls_last(), F.col("value").desc_nulls_first()
        )
        rows = result.collect()

        # Category sorted ascending with nulls last
        # Then value sorted descending with nulls first within each category
        assert len(rows) == 5
        # Category A should come first (nulls last)
        assert rows[0]["category"] == "A"
        assert rows[1]["category"] == "A"
        # Category B should come next
        assert rows[2]["category"] == "B"
        # Category None should come last
        assert rows[3]["category"] is None
        assert rows[4]["category"] is None

    def test_desc_nulls_last_float(self, spark):
        """Test desc_nulls_last() with float column."""
        # Create DataFrame without explicit schema - let backend infer float type
        df = spark.createDataFrame(
            [
                {"score": 3.14},
                {"score": 2.71},
                {"score": None},
                {"score": 1.41},
                {"score": 4.67},
            ],
        )

        result = df.orderBy(F.col("score").desc_nulls_last())
        rows = result.collect()

        # Verify order: 4.67, 3.14, 2.71, 1.41, None (nulls last)
        assert len(rows) == 5
        assert abs(rows[0]["score"] - 4.67) < 0.01
        assert abs(rows[1]["score"] - 3.14) < 0.01
        assert abs(rows[2]["score"] - 2.71) < 0.01
        assert abs(rows[3]["score"] - 1.41) < 0.01
        assert rows[4]["score"] is None

    def test_desc_nulls_last_negative_numbers(self, spark):
        """Test desc_nulls_last() with negative numbers."""
        schema = StructType([StructField("temp", IntegerType(), True)])
        df = spark.createDataFrame(
            [
                {"temp": -5},
                {"temp": 10},
                {"temp": None},
                {"temp": -10},
                {"temp": 0},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("temp").desc_nulls_last())
        rows = result.collect()

        # Verify order: 10, 0, -5, -10, None (nulls last)
        assert len(rows) == 5
        assert rows[0]["temp"] == 10
        assert rows[1]["temp"] == 0
        assert rows[2]["temp"] == -5
        assert rows[3]["temp"] == -10
        assert rows[4]["temp"] is None

    def test_asc_nulls_last_empty_dataframe(self, spark):
        """Test asc_nulls_last() with empty DataFrame."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame([], schema=schema)

        result = df.orderBy(F.col("value").asc_nulls_last())
        rows = result.collect()

        # Should return empty result
        assert len(rows) == 0

    def test_desc_nulls_last_single_row(self, spark):
        """Test desc_nulls_last() with single row DataFrame."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame([{"value": "A"}], schema=schema)

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Should return single row
        assert len(rows) == 1
        assert rows[0]["value"] == "A"

    def test_desc_nulls_last_single_null_row(self, spark):
        """Test desc_nulls_last() with single null row."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame([{"value": None}], schema=schema)

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Should return single row with None
        assert len(rows) == 1
        assert rows[0]["value"] is None

    def test_nulls_at_beginning_middle_end(self, spark):
        """Test that nulls are correctly positioned when they appear at different positions."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": None},  # Beginning
                {"value": "B"},
                {"value": None},  # Middle
                {"value": "A"},
                {"value": None},  # End
            ],
            schema=schema,
        )

        # Test desc_nulls_last - all nulls should go to end
        result1 = df.orderBy(F.col("value").desc_nulls_last())
        rows1 = result1.collect()
        assert len(rows1) == 5
        assert rows1[0]["value"] == "B"
        assert rows1[1]["value"] == "A"
        assert rows1[2]["value"] is None
        assert rows1[3]["value"] is None
        assert rows1[4]["value"] is None

        # Test desc_nulls_first - all nulls should go to beginning
        result2 = df.orderBy(F.col("value").desc_nulls_first())
        rows2 = result2.collect()
        assert len(rows2) == 5
        assert rows2[0]["value"] is None
        assert rows2[1]["value"] is None
        assert rows2[2]["value"] is None
        assert rows2[3]["value"] == "B"
        assert rows2[4]["value"] == "A"

    def test_unicode_characters(self, spark):
        """Test ordering with Unicode characters."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "世界"},
                {"value": None},
                {"value": "Hello"},
                {"value": "🌍"},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_last())
        rows = result.collect()

        # Verify nulls are last
        assert len(rows) == 4
        assert rows[3]["value"] is None

    def test_mixed_asc_desc_nulls_variants(self, spark):
        """Test mixed ascending/descending with different nulls positions."""
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": "A", "col2": 1},
                {"col1": "A", "col2": None},
                {"col1": None, "col2": 2},
                {"col1": "B", "col2": 1},
                {"col1": "B", "col2": None},
            ],
            schema=schema,
        )

        # Mix asc_nulls_last and desc_nulls_first
        result = df.orderBy(
            F.col("col1").asc_nulls_last(), F.col("col2").desc_nulls_first()
        )
        rows = result.collect()

        assert len(rows) == 5
        # First sort by col1 (asc_nulls_last): A, A, B, B, None
        # Within each col1 group, sort by col2 (desc_nulls_first): nulls first, then descending

    def test_very_large_numbers(self, spark):
        """Test ordering with very large numbers."""
        schema = StructType([StructField("value", IntegerType(), True)])
        df = spark.createDataFrame(
            [
                {"value": 999999999},
                {"value": None},
                {"value": -999999999},
                {"value": 0},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        assert len(rows) == 4
        assert rows[0]["value"] == 999999999
        assert rows[1]["value"] == 0
        assert rows[2]["value"] == -999999999
        assert rows[3]["value"] is None

    def test_all_four_methods_comprehensive(self, spark):
        """Comprehensive test of all four methods with same data."""
        test_data = [
            {"value": "Z"},
            {"value": "A"},
            {"value": None},
            {"value": "M"},
        ]
        schema = StructType([StructField("value", StringType(), True)])

        # Test each method
        df = spark.createDataFrame(test_data, schema=schema)

        # desc_nulls_last: Z, M, A, None
        result1 = df.orderBy(F.col("value").desc_nulls_last())
        rows1 = result1.collect()
        assert rows1[0]["value"] == "Z"
        assert rows1[1]["value"] == "M"
        assert rows1[2]["value"] == "A"
        assert rows1[3]["value"] is None

        # desc_nulls_first: None, Z, M, A
        result2 = df.orderBy(F.col("value").desc_nulls_first())
        rows2 = result2.collect()
        assert rows2[0]["value"] is None
        assert rows2[1]["value"] == "Z"
        assert rows2[2]["value"] == "M"
        assert rows2[3]["value"] == "A"

        # asc_nulls_last: A, M, Z, None
        result3 = df.orderBy(F.col("value").asc_nulls_last())
        rows3 = result3.collect()
        assert rows3[0]["value"] == "A"
        assert rows3[1]["value"] == "M"
        assert rows3[2]["value"] == "Z"
        assert rows3[3]["value"] is None

        # asc_nulls_first: None, A, M, Z
        result4 = df.orderBy(F.col("value").asc_nulls_first())
        rows4 = result4.collect()
        assert rows4[0]["value"] is None
        assert rows4[1]["value"] == "A"
        assert rows4[2]["value"] == "M"
        assert rows4[3]["value"] == "Z"

    def test_comparison_with_default_desc_asc(self, spark):
        """Compare nulls variants with default desc() and asc() methods."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "B"},
                {"value": None},
                {"value": "A"},
            ],
            schema=schema,
        )

        # Default desc() behavior (may differ from desc_nulls_last in PySpark)
        result_desc = df.orderBy(F.col("value").desc())
        rows_desc = result_desc.collect()

        # desc_nulls_last() - explicit nulls last
        result_desc_nulls_last = df.orderBy(F.col("value").desc_nulls_last())
        rows_desc_nulls_last = result_desc_nulls_last.collect()

        # Both should handle non-null values the same way
        assert rows_desc[0]["value"] == rows_desc_nulls_last[0]["value"]
        assert rows_desc[1]["value"] == rows_desc_nulls_last[1]["value"]

    def test_three_column_ordering(self, spark):
        """Test ordering with three columns using different nulls variants."""
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
                StructField("col3", StringType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": "A", "col2": 1, "col3": "X"},
                {"col1": "A", "col2": None, "col3": "Y"},
                {"col1": None, "col2": 2, "col3": "Z"},
                {"col1": "B", "col2": 1, "col3": None},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("col1").asc_nulls_last(),
            F.col("col2").desc_nulls_first(),
            F.col("col3").asc_nulls_last(),
        )
        rows = result.collect()

        assert len(rows) == 4
        # Verify first column ordering (asc_nulls_last): A rows first, then B, then None
        assert rows[0]["col1"] == "A"
        assert rows[1]["col1"] == "A"
        assert rows[2]["col1"] == "B"
        assert rows[3]["col1"] is None

    def test_string_comparison_edge_cases(self, spark):
        """Test string ordering edge cases with nulls."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": ""},  # Empty string
                {"value": None},
                {"value": " "},  # Space
                {"value": "a"},
                {"value": "A"},  # Case sensitivity
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_last())
        rows = result.collect()

        assert len(rows) == 5
        # Empty string and space should come before letters
        # Null should be last
        assert rows[-1]["value"] is None

    def test_complex_ordering_scenario(self, spark):
        """Test complex ordering scenario with multiple nulls and values."""
        schema = StructType(
            [
                StructField("dept", StringType(), True),
                StructField("name", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"dept": "IT", "name": "Alice", "salary": 5000},
                {"dept": "IT", "name": None, "salary": 6000},
                {"dept": None, "name": "Bob", "salary": 4000},
                {"dept": "HR", "name": "Charlie", "salary": None},
                {"dept": "HR", "name": "David", "salary": 5500},
                {"dept": None, "name": None, "salary": None},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("dept").asc_nulls_last(),
            F.col("salary").desc_nulls_first(),
            F.col("name").asc_nulls_last(),
        )
        rows = result.collect()

        assert len(rows) == 6
        # Verify dept ordering (asc_nulls_last): HR, IT, None
        # First should be HR dept
        assert rows[0]["dept"] == "HR"
        assert rows[1]["dept"] == "HR"
        # Then IT dept
        assert rows[2]["dept"] == "IT"
        assert rows[3]["dept"] == "IT"
        # Then None dept (nulls last)
        assert rows[4]["dept"] is None
        assert rows[5]["dept"] is None

    def test_ordering_with_duplicate_values(self, spark):
        """Test ordering with duplicate values and nulls."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "A"},
                {"value": "A"},  # Duplicate
                {"value": None},
                {"value": "B"},
                {"value": "A"},  # Another duplicate
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        assert len(rows) == 5
        assert rows[0]["value"] == "B"
        assert rows[1]["value"] == "A"
        assert rows[2]["value"] == "A"
        assert rows[3]["value"] == "A"
        assert rows[4]["value"] is None

    def test_ordering_with_identical_nulls(self, spark):
        """Test that multiple identical nulls maintain consistent ordering."""
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": None, "col2": 1},
                {"col1": None, "col2": 2},
                {"col1": None, "col2": None},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("col1").asc_nulls_last(), F.col("col2").desc_nulls_first()
        )
        rows = result.collect()

        assert len(rows) == 3
        # All have None in col1, so they should be grouped together
        # Then sorted by col2 with nulls first
        assert rows[0]["col1"] is None
        assert rows[1]["col1"] is None
        assert rows[2]["col1"] is None
        assert rows[0]["col2"] is None  # nulls first in col2

    def test_mixed_data_types_ordering(self, spark):
        """Test ordering with mixed data types and nulls."""
        # Use types available from imports, or create DataFrame without explicit schema
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField(
                    "score", StringType(), True
                ),  # Use string for score to avoid type issues
                StructField(
                    "active", StringType(), True
                ),  # Use string for active to avoid type issues
            ]
        )
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 25, "score": "85.5", "active": "true"},
                {"name": None, "age": 30, "score": None, "active": None},
                {"name": "Bob", "age": None, "score": "90.0", "active": "false"},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("name").asc_nulls_last(),
            F.col("age").desc_nulls_first(),
            F.col("score").asc_nulls_last(),
            F.col("active").desc_nulls_first(),
        )
        rows = result.collect()

        assert len(rows) == 3
        # Verify name ordering (asc_nulls_last): Alice, Bob, None
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"
        assert rows[2]["name"] is None


class TestColumnOrderingParity:
    """Test class to ensure exact parity with PySpark behavior."""

    def test_pyspark_desc_nulls_last_parity(self, spark):
        """Test that desc_nulls_last() matches PySpark behavior exactly.

        PySpark behavior for desc_nulls_last():
        - Sorts in descending order
        - Places all null values at the end
        - Maintains relative order of non-null values
        """
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "Z"},
                {"value": "A"},
                {"value": None},
                {"value": "M"},
                {"value": None},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Expected PySpark order: Z, M, A, None, None
        assert len(rows) == 5
        assert rows[0]["value"] == "Z"
        assert rows[1]["value"] == "M"
        assert rows[2]["value"] == "A"
        assert rows[3]["value"] is None
        assert rows[4]["value"] is None

        # Verify all non-null values come before all null values
        non_null_indices = [i for i, row in enumerate(rows) if row["value"] is not None]
        null_indices = [i for i, row in enumerate(rows) if row["value"] is None]
        if non_null_indices and null_indices:
            assert max(non_null_indices) < min(null_indices)

    def test_pyspark_desc_nulls_first_parity(self, spark):
        """Test that desc_nulls_first() matches PySpark behavior exactly.

        PySpark behavior for desc_nulls_first():
        - Sorts in descending order
        - Places all null values at the beginning
        - Maintains relative order of non-null values
        """
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "Z"},
                {"value": "A"},
                {"value": None},
                {"value": "M"},
                {"value": None},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_first())
        rows = result.collect()

        # Expected PySpark order: None, None, Z, M, A
        assert len(rows) == 5
        assert rows[0]["value"] is None
        assert rows[1]["value"] is None
        assert rows[2]["value"] == "Z"
        assert rows[3]["value"] == "M"
        assert rows[4]["value"] == "A"

        # Verify all null values come before all non-null values
        null_indices = [i for i, row in enumerate(rows) if row["value"] is None]
        non_null_indices = [i for i, row in enumerate(rows) if row["value"] is not None]
        if null_indices and non_null_indices:
            assert max(null_indices) < min(non_null_indices)

    def test_pyspark_asc_nulls_last_parity(self, spark):
        """Test that asc_nulls_last() matches PySpark behavior exactly."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "Z"},
                {"value": "A"},
                {"value": None},
                {"value": "M"},
                {"value": None},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_last())
        rows = result.collect()

        # Expected PySpark order: A, M, Z, None, None
        assert len(rows) == 5
        assert rows[0]["value"] == "A"
        assert rows[1]["value"] == "M"
        assert rows[2]["value"] == "Z"
        assert rows[3]["value"] is None
        assert rows[4]["value"] is None

    def test_pyspark_asc_nulls_first_parity(self, spark):
        """Test that asc_nulls_first() matches PySpark behavior exactly."""
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(
            [
                {"value": "Z"},
                {"value": "A"},
                {"value": None},
                {"value": "M"},
                {"value": None},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").asc_nulls_first())
        rows = result.collect()

        # Expected PySpark order: None, None, A, M, Z
        assert len(rows) == 5
        assert rows[0]["value"] is None
        assert rows[1]["value"] is None
        assert rows[2]["value"] == "A"
        assert rows[3]["value"] == "M"
        assert rows[4]["value"] == "Z"

    def test_pyspark_multi_column_parity(self, spark):
        """Test multi-column ordering matches PySpark behavior."""
        schema = StructType(
            [
                StructField("col1", StringType(), True),
                StructField("col2", IntegerType(), True),
            ]
        )
        df = spark.createDataFrame(
            [
                {"col1": "A", "col2": 2},
                {"col1": "A", "col2": None},
                {"col1": None, "col2": 1},
                {"col1": "B", "col2": 3},
                {"col1": None, "col2": None},
            ],
            schema=schema,
        )

        result = df.orderBy(
            F.col("col1").asc_nulls_last(), F.col("col2").desc_nulls_first()
        )
        rows = result.collect()

        assert len(rows) == 5
        # First level: col1 asc_nulls_last: A, A, B, None, None
        # Within each col1 group: col2 desc_nulls_first

        # A group should come first
        assert rows[0]["col1"] == "A"
        assert rows[1]["col1"] == "A"
        # Within A group, col2 desc_nulls_first: None first, then descending
        assert rows[0]["col2"] is None
        assert rows[1]["col2"] == 2

        # B group should come next
        assert rows[2]["col1"] == "B"

        # None group should come last
        assert rows[3]["col1"] is None
        assert rows[4]["col1"] is None

    def test_pyspark_integer_ordering_parity(self, spark):
        """Test integer ordering matches PySpark behavior exactly."""
        schema = StructType([StructField("value", IntegerType(), True)])
        df = spark.createDataFrame(
            [
                {"value": 10},
                {"value": -5},
                {"value": None},
                {"value": 0},
                {"value": -10},
            ],
            schema=schema,
        )

        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Expected: 10, 0, -5, -10, None
        assert len(rows) == 5
        assert rows[0]["value"] == 10
        assert rows[1]["value"] == 0
        assert rows[2]["value"] == -5
        assert rows[3]["value"] == -10
        assert rows[4]["value"] is None

    def test_pyspark_float_ordering_parity(self, spark):
        """Test float ordering matches PySpark behavior exactly."""
        # Create DataFrame without explicit schema to avoid type compatibility issues
        # PySpark will infer float type from the data
        df = spark.createDataFrame(
            [
                {"value": 1.5},
                {"value": -2.5},
                {"value": None},
                {"value": 0.0},
                {"value": 3.14},
            ],
        )
        result = df.orderBy(F.col("value").desc_nulls_last())
        rows = result.collect()

        # Expected: 3.14, 1.5, 0.0, -2.5, None
        assert len(rows) == 5
        assert abs(rows[0]["value"] - 3.14) < 0.01
        assert abs(rows[1]["value"] - 1.5) < 0.01
        assert abs(rows[2]["value"] - 0.0) < 0.01
        assert abs(rows[3]["value"] - (-2.5)) < 0.01
        assert rows[4]["value"] is None
