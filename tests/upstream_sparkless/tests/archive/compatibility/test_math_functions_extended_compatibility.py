"""
Compatibility tests for extended math/trigonometric functions.

This module validates extended math functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestMathFunctionsExtendedCompatibility:
    """Test extended math functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("math_functions_test")
        yield session
        session.stop()

    def test_acos(self, spark):
        """Test acos function."""
        expected = load_expected_output("functions", "math_acos")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.acos(df.x))
        assert_dataframes_equal(result, expected)

    def test_asin(self, spark):
        """Test asin function."""
        expected = load_expected_output("functions", "math_asin")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.asin(df.x))
        assert_dataframes_equal(result, expected)

    def test_atan(self, spark):
        """Test atan function."""
        expected = load_expected_output("functions", "math_atan")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.atan(df.x))
        assert_dataframes_equal(result, expected)

    def test_atan2(self, spark):
        """Test atan2 function."""
        expected = load_expected_output("functions", "math_atan2")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.atan2(df.x, df.y))
        assert_dataframes_equal(result, expected)

    def test_acosh(self, spark):
        """Test acosh function."""
        expected = load_expected_output("functions", "math_acosh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.acosh(df.x + 1.5))
        assert_dataframes_equal(result, expected)

    def test_asinh(self, spark):
        """Test asinh function."""
        expected = load_expected_output("functions", "math_asinh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.asinh(df.x))
        assert_dataframes_equal(result, expected)

    def test_atanh(self, spark):
        """Test atanh function."""
        expected = load_expected_output("functions", "math_atanh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.atanh(df.x * 0.5))
        assert_dataframes_equal(result, expected)

    def test_cosh(self, spark):
        """Test cosh function."""
        expected = load_expected_output("functions", "math_cosh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.cosh(df.x))
        assert_dataframes_equal(result, expected)

    def test_sinh(self, spark):
        """Test sinh function."""
        expected = load_expected_output("functions", "math_sinh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.sinh(df.x))
        assert_dataframes_equal(result, expected)

    def test_tanh(self, spark):
        """Test tanh function."""
        expected = load_expected_output("functions", "math_tanh")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.tanh(df.x))
        assert_dataframes_equal(result, expected)

    def test_cbrt(self, spark):
        """Test cbrt function."""
        expected = load_expected_output("functions", "math_cbrt")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.cbrt(df.value))
        assert_dataframes_equal(result, expected)

    def test_degrees(self, spark):
        """Test degrees function."""
        expected = load_expected_output("functions", "math_degrees")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.degrees(df.angle))
        assert_dataframes_equal(result, expected)

    def test_radians(self, spark):
        """Test radians function."""
        expected = load_expected_output("functions", "math_radians")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.radians(df.x))
        assert_dataframes_equal(result, expected)

    def test_expm1(self, spark):
        """Test expm1 function."""
        expected = load_expected_output("functions", "math_expm1")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.expm1(df.x))
        assert_dataframes_equal(result, expected)

    def test_log1p(self, spark):
        """Test log1p function."""
        expected = load_expected_output("functions", "math_log1p")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.log1p(df.x))
        assert_dataframes_equal(result, expected)

    def test_log2(self, spark):
        """Test log2 function."""
        expected = load_expected_output("functions", "math_log2")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.log2(df.value))
        assert_dataframes_equal(result, expected)

    def test_log10(self, spark):
        """Test log10 function."""
        expected = load_expected_output("functions", "math_log10")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.log10(df.value))
        assert_dataframes_equal(result, expected)

    def test_rint(self, spark):
        """Test rint function."""
        expected = load_expected_output("functions", "math_rint")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rint(df.value))
        assert_dataframes_equal(result, expected)

    def test_bround(self, spark):
        """Test bround function."""
        expected = load_expected_output("functions", "math_bround")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.bround(df.value, 2))
        assert_dataframes_equal(result, expected)

    def test_factorial(self, spark):
        """Test factorial function."""
        expected = load_expected_output("functions", "math_factorial")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.factorial(df.id))
        assert_dataframes_equal(result, expected)

    def test_hypot(self, spark):
        """Test hypot function."""
        expected = load_expected_output("functions", "math_hypot")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hypot(df.x, df.y))
        assert_dataframes_equal(result, expected)

    def test_signum(self, spark):
        """Test signum function."""
        expected = load_expected_output("functions", "math_signum")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.signum(df.value))
        assert_dataframes_equal(result, expected)

    def test_e(self, spark):
        """Test e function (Euler's number)."""
        expected = load_expected_output("functions", "math_e")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.expr("e()"))
        assert_dataframes_equal(result, expected)

    def test_pi(self, spark):
        """Test pi function."""
        expected = load_expected_output("functions", "math_pi")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.expr("pi()"))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="Random numbers cannot be deterministically compared")
    def test_rand(self, spark):
        """Test rand function."""
        expected = load_expected_output("functions", "math_rand")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.rand())
        # Cannot compare random values - just check structure
        assert len(result.collect()) == expected["expected_output"]["row_count"]

    @pytest.mark.skip(reason="Random numbers cannot be deterministically compared")
    def test_randn(self, spark):
        """Test randn function."""
        expected = load_expected_output("functions", "math_randn")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.randn())
        # Cannot compare random values - just check structure
        assert len(result.collect()) == expected["expected_output"]["row_count"]

    def test_conv(self, spark):
        """Test conv function."""
        expected = load_expected_output("functions", "math_conv")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.conv(F.col("id"), 10, 2))
        assert_dataframes_equal(result, expected)

    def test_bin(self, spark):
        """Test bin function."""
        expected = load_expected_output("functions", "math_bin")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.bin(df.id))
        assert_dataframes_equal(result, expected)

    def test_hex(self, spark):
        """Test hex function."""
        expected = load_expected_output("functions", "math_hex")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.hex(df.id))
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="not yet implemented")
    def test_bitwise_not(self, spark):
        """Test bitwise_not function."""
        expected = load_expected_output("functions", "math_bitwise_not")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.bitwise_not(df.id))
        assert_dataframes_equal(result, expected)
