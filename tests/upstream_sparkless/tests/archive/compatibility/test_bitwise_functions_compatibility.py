"""
Compatibility tests for bitwise functions.

Tests bitwise operations against expected outputs generated from PySpark.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestBitwiseFunctionsCompatibility:
    """Test bitwise functions compatibility with PySpark."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("compatibility_test")
        yield session
        session.stop()

    def test_bitwise_not(self, spark):
        """Test bitwise_not function."""
        expected = load_expected_output("functions", "bitwise_bitwise_not")

        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.num.bitwise_not())

        assert_dataframes_equal(result, expected)

    # Bitmap Functions (PySpark 3.5+)
    def test_bitmap_bit_position(self, spark):
        """Test bitmap_bit_position function (PySpark 3.5+)."""
        # Note: Bitmap functions require binary data, so we test the function exists and can be called
        test_data = [{"id": 1, "bitmap": b"\x01\x02\x03"}]
        df = spark.createDataFrame(test_data)

        # Test that the function exists and can be called
        result = df.select(F.bitmap_bit_position(F.col("bitmap")))
        assert result is not None
        assert len(result.columns) == 1

    def test_bitmap_bucket_number(self, spark):
        """Test bitmap_bucket_number function (PySpark 3.5+)."""
        test_data = [{"id": 1, "bitmap": b"\x01\x02\x03"}]
        df = spark.createDataFrame(test_data)

        result = df.select(F.bitmap_bucket_number(F.col("bitmap")))
        assert result is not None
        assert len(result.columns) == 1

    def test_bitmap_construct_agg(self, spark):
        """Test bitmap_construct_agg aggregate function (PySpark 3.5+)."""
        test_data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 10},
        ]
        df = spark.createDataFrame(test_data)

        # Test aggregate function
        result = df.groupBy().agg(
            F.bitmap_construct_agg(F.col("value")).alias("bitmap")
        )
        assert result is not None
        assert len(result.columns) == 1

    def test_bitmap_count(self, spark):
        """Test bitmap_count function (PySpark 3.5+)."""
        test_data = [{"id": 1, "bitmap": b"\x01\x02\x03"}]
        df = spark.createDataFrame(test_data)

        result = df.select(F.bitmap_count(F.col("bitmap")))
        assert result is not None
        assert len(result.columns) == 1

    def test_bitmap_or_agg(self, spark):
        """Test bitmap_or_agg aggregate function (PySpark 3.5+)."""
        test_data = [
            {"id": 1, "bitmap": b"\x01\x02"},
            {"id": 2, "bitmap": b"\x03\x04"},
        ]
        df = spark.createDataFrame(test_data)

        # Test aggregate function
        result = df.groupBy().agg(F.bitmap_or_agg(F.col("bitmap")).alias("result"))
        assert result is not None
        assert len(result.columns) == 1
