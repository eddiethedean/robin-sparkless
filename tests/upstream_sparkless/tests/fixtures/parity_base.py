"""
Base classes and utilities for PySpark parity testing.

This module provides standard patterns for all parity tests that validate
Sparkless behavior against pre-generated PySpark expected outputs.
"""

from typing import Any, Dict, List, Optional, Tuple
from sparkless import SparkSession
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal, compare_dataframes


class ParityTestBase:
    """Base class for all PySpark parity tests.

    All parity tests should inherit from this class to ensure consistent
    patterns and behavior across the test suite.

    Note: Tests should use the unified `spark` fixture from conftest.py
    which works with both sparkless and PySpark backends.
    """

    def load_expected(
        self, category: str, test_name: str, pyspark_version: str = "3.2"
    ) -> Dict[str, Any]:
        """Load expected output for a test case.

        Args:
            category: Test category (e.g., 'dataframe', 'functions', 'sql')
            test_name: Name of the test case
            pyspark_version: PySpark version to use (default: "3.2")

        Returns:
            Dictionary containing expected output data
        """
        return load_expected_output(category, test_name, pyspark_version)

    def assert_parity(
        self,
        actual_df: Any,
        expected_output: Dict[str, Any],
        tolerance: float = 1e-6,
        msg: str = "",
    ) -> None:
        """Assert that Sparkless result matches PySpark expected output.

        Args:
            actual_df: Sparkless DataFrame result
            expected_output: Expected output dictionary from load_expected_output
            tolerance: Numerical tolerance for floating point comparisons
            msg: Optional custom error message

        Raises:
            AssertionError: If results don't match expected output
        """
        assert_dataframes_equal(
            actual_df,
            expected_output,
            tolerance=tolerance,
            msg=msg,
        )

    def compare_parity(
        self,
        actual_df: Any,
        expected_output: Dict[str, Any],
        tolerance: float = 1e-6,
    ) -> Tuple[bool, Optional[str]]:
        """Compare Sparkless result with PySpark expected output.

        Args:
            actual_df: Sparkless DataFrame result
            expected_output: Expected output dictionary from load_expected_output
            tolerance: Numerical tolerance for floating point comparisons

        Returns:
            Tuple of (is_equal, error_message)
        """
        result = compare_dataframes(
            actual_df,
            expected_output,
            tolerance=tolerance,
        )
        if result.equivalent:
            return True, None
        else:
            return False, "\n".join(result.errors)


def create_test_dataframe(spark: SparkSession, data: List[Dict[str, Any]]) -> Any:
    """Create a DataFrame from test data.

    This is a convenience function for creating DataFrames in tests.

    Args:
        spark: Sparkless SparkSession
        data: List of dictionaries representing rows

    Returns:
        DataFrame
    """
    return spark.createDataFrame(data)
