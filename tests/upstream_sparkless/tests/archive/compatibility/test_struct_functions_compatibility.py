"""
Compatibility tests for struct functions.

This module validates struct functions against pre-generated PySpark outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from sparkless import F


class TestStructFunctionsCompatibility:
    """Test struct functions against expected PySpark outputs."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        from sparkless import SparkSession

        session = SparkSession("struct_functions_test")
        yield session
        session.stop()

    @pytest.mark.skip(reason="struct not yet implemented correctly")
    def test_struct(self, spark):
        """Test struct function."""
        expected = load_expected_output("functions", "struct")
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.struct(df.name, df.age))
        assert_dataframes_equal(result, expected)
