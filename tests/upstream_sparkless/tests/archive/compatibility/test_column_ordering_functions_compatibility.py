"""
Compatibility tests for column/ordering functions.

Tests column and ordering operations against expected outputs generated from PySpark.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


class TestColumnOrderingFunctionsCompatibility:
    """Test column/ordering functions compatibility with PySpark."""

    @pytest.mark.skip(reason="Expected output file not found: column_asc.json")
    def test_asc(self, spark):
        """Test asc ordering function."""
        expected = load_expected_output("functions", "column_asc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.age.asc())

        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="Expected output file not found: column_desc.json")
    def test_desc(self, spark):
        """Test desc ordering function."""
        expected = load_expected_output("functions", "column_desc")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.age.desc())

        assert_dataframes_equal(result, expected)

    def test_col(self, spark, spark_backend):
        """Test col function."""
        from tests.fixtures.spark_backend import BackendType

        # Import appropriate F based on backend
        if spark_backend == BackendType.PYSPARK:
            from pyspark.sql import functions as F
        else:
            from sparkless import F

        expected = load_expected_output("functions", "column_col")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name"))

        assert_dataframes_equal(result, expected)

    def test_column(self, spark, spark_backend):
        """Test column function."""
        from tests.fixtures.spark_backend import BackendType

        # Import appropriate F based on backend
        if spark_backend == BackendType.PYSPARK:
            from pyspark.sql import functions as F
        else:
            from sparkless import F

        expected = load_expected_output("functions", "column_column")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name"))

        assert_dataframes_equal(result, expected)

    def test_lit(self, spark, spark_backend):
        """Test lit function."""
        from tests.fixtures.spark_backend import BackendType

        # Import appropriate F based on backend
        if spark_backend == BackendType.PYSPARK:
            from pyspark.sql import functions as F
        else:
            from sparkless import F

        expected = load_expected_output("functions", "column_lit")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.lit("test"))

        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="expr not yet implemented correctly")
    def test_expr(self, spark, spark_backend):
        """Test expr function."""
        from tests.fixtures.spark_backend import BackendType

        # Import appropriate F based on backend
        if spark_backend == BackendType.PYSPARK:
            from pyspark.sql import functions as F
        else:
            from sparkless import F

        expected = load_expected_output("functions", "column_expr")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.expr("age + 1"))

        assert_dataframes_equal(result, expected)
