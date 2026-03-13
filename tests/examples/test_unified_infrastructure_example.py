"""
Example tests demonstrating the unified test infrastructure.

This module shows how to write tests that work with both sparkless and PySpark.
Set SPARKLESS_TEST_MODE=pyspark to run tests with PySpark backend.
"""

import pytest
from sparkless.testing import (
    Mode,
    get_mode,
    is_pyspark_mode,
    get_imports,
    assert_dataframes_equal,
)


class TestUnifiedInfrastructure:
    """Examples of using the unified test infrastructure."""

    def test_basic_operation(self, spark):
        """Basic test that works with both backends automatically.

        This test will run with sparkless by default, or PySpark if
        SPARKLESS_TEST_MODE=pyspark is set.
        """
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        assert df.count() == 1
        assert df.columns == ["id", "name"]

    @pytest.mark.sparkless_only
    def test_sparkless_only(self, spark):
        """Test that only runs with sparkless backend."""
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    @pytest.mark.pyspark_only
    def test_pyspark_only(self, spark):
        """Test that only runs with PySpark.

        This test will be skipped if not running in PySpark mode.
        """
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    @pytest.mark.backend("sparkless")
    def test_with_backend_marker_sparkless(self, spark):
        """Test using @pytest.mark.backend marker for sparkless."""
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    @pytest.mark.backend("pyspark")
    def test_with_backend_marker_pyspark(self, spark):
        """Test using @pytest.mark.backend marker for pyspark."""
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    def test_with_mode_info(self, spark, spark_mode):
        """Test that can access mode information."""
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

        # spark_mode fixture provides the current Mode
        assert spark_mode in [Mode.SPARKLESS, Mode.PYSPARK]
        assert spark_mode.value in ["sparkless", "pyspark"]


class TestUnifiedImports:
    """Examples of using unified imports with shared spark fixture."""

    def test_with_unified_imports(self, spark):
        """Example: get_imports() provides all Spark types and functions."""
        imports = get_imports()

        # Access functions module
        F = imports.F

        # Create DataFrame using fixture
        df = spark.createDataFrame([{"id": 1, "name": "test"}])

        # Use functions
        result = df.select(F.upper("name").alias("upper_name"))
        assert result.collect()[0]["upper_name"] == "TEST"

    def test_with_data_types(self, spark):
        """Example: using data types from get_imports()."""
        imports = get_imports()

        schema = imports.StructType([
            imports.StructField("id", imports.IntegerType(), True),
            imports.StructField("name", imports.StringType(), True),
        ])

        df = spark.createDataFrame([{"id": 1, "name": "Alice"}], schema=schema)
        assert df.count() == 1
        assert len(df.schema.fields) == 2

    def test_with_window_functions(self, spark):
        """Example: using Window from get_imports()."""
        imports = get_imports()
        F = imports.F
        Window = imports.Window

        data = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]
        df = spark.createDataFrame(data)

        window = Window.partitionBy("category").orderBy("value")
        result = df.withColumn("row_num", F.row_number().over(window))

        rows = result.collect()
        assert len(rows) == 3


class TestDataFrameComparison:
    """Examples of using DataFrame comparison utilities."""

    def test_dataframe_comparison(self, spark):
        """Example: comparing DataFrames."""
        df1 = spark.createDataFrame([
            {"id": 1, "value": 10.0},
            {"id": 2, "value": 20.0},
        ])
        df2 = spark.createDataFrame([
            {"id": 1, "value": 10.0},
            {"id": 2, "value": 20.0},
        ])

        # These should be equal
        assert_dataframes_equal(df1, df2)

    def test_comparison_with_tolerance(self, spark):
        """Example: comparing DataFrames with floating point tolerance."""
        df1 = spark.createDataFrame([
            {"id": 1, "value": 10.0000001},
        ])
        df2 = spark.createDataFrame([
            {"id": 1, "value": 10.0},
        ])

        # Should be equal within tolerance
        assert_dataframes_equal(df1, df2, tolerance=1e-6)

    def test_comparison_ignore_order(self, spark):
        """Example: comparing DataFrames ignoring row order."""
        df1 = spark.createDataFrame([
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ])
        df2 = spark.createDataFrame([
            {"id": 2, "value": 20},
            {"id": 1, "value": 10},
        ])

        # Should be equal when ignoring order
        assert_dataframes_equal(df1, df2, check_order=False)
