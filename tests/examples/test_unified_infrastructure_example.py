"""
Example tests demonstrating the unified test infrastructure.

This module shows how to write tests that work with both PySpark and mock-spark.
When running with SPARKLESS_TEST_BACKEND=pyspark, mock-only and both-backend
tests are skipped (they require sparkless/mock session).
"""

import os

import pytest
from tests.fixtures.comparison import assert_dataframes_equal

# Skip mock-only or both-backend tests when running in PySpark-only mode (no mock session).
_skip_if_pyspark_only = pytest.mark.skipif(
    (os.environ.get("SPARKLESS_TEST_BACKEND") or "").strip().lower() == "pyspark",
    reason="Requires mock/both backends; skipped in PySpark-only run",
)


class TestUnifiedInfrastructure:
    """Examples of using the unified test infrastructure."""

    def test_basic_operation(self, spark):
        """Basic test that works with both backends automatically.

        This test will run with mock-spark by default, or PySpark if
        MOCK_SPARK_TEST_BACKEND=pyspark is set.
        """
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        assert df.count() == 1
        assert df.columns == ["id", "name"]

    @pytest.mark.backend("mock")
    @_skip_if_pyspark_only
    def test_mock_only(self, spark):
        """Test that only runs with mock-spark."""
        # This test will be skipped if backend is PySpark
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    @pytest.mark.backend("pyspark")
    def test_pyspark_only(self, spark):
        """Test that only runs with PySpark.

        This test will be skipped if PySpark is not available.
        """
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    @pytest.mark.backend("both")
    @_skip_if_pyspark_only
    def test_comparison(self, mock_spark_session, pyspark_session):
        """Test that compares results from both backends.

        This test runs with both backends and verifies they produce
        identical results.
        """
        data = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]

        mock_df = mock_spark_session.createDataFrame(data)
        pyspark_df = pyspark_session.createDataFrame(data)

        # Compare basic DataFrames
        assert_dataframes_equal(mock_df, pyspark_df)

        # Compare filtered results
        mock_filtered = mock_df.filter(mock_df.id > 1)
        pyspark_filtered = pyspark_df.filter(pyspark_df.id > 1)
        assert_dataframes_equal(mock_filtered, pyspark_filtered)

    @pytest.mark.backend("both")
    @_skip_if_pyspark_only
    def test_aggregation_comparison(self, mock_spark_session, pyspark_session):
        """Compare aggregation results between backends."""
        from tests.fixtures.spark_backend import BackendType
        from tests.fixtures.spark_imports import get_spark_imports

        mock_F = get_spark_imports(BackendType.MOCK).F
        pyspark_F = get_spark_imports(BackendType.PYSPARK).F

        data = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 20},
            {"category": "B", "value": 30},
        ]

        mock_df = mock_spark_session.createDataFrame(data)
        pyspark_df = pyspark_session.createDataFrame(data)

        mock_result = mock_df.groupBy("category").agg(
            mock_F.sum("value").alias("total")
        )
        pyspark_result = pyspark_df.groupBy("category").agg(
            pyspark_F.sum("value").alias("total")
        )

        # Compare with tolerance for floating point; allow schema differences (e.g. int vs long).
        assert_dataframes_equal(
            mock_result,
            pyspark_result,
            tolerance=1e-6,
            check_schema=False,
            check_order=False,
        )

    def test_with_backend_info(self, spark, spark_backend):
        """Test that can access backend information."""
        # spark_backend fixture provides the current backend type
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

        # Can check which backend is being used if needed
        # (though usually tests should be backend-agnostic)
        backend_name = spark_backend.value
        assert backend_name in ["mock", "pyspark", "robin"]


class TestUnifiedImports:
    """Examples of using unified imports with shared spark fixture."""

    def test_with_unified_imports(self, spark):
        """Example: get_imports() provides SparkSession, F, StructType; use spark fixture for session."""
        from tests.fixtures.spark_imports import get_imports

        SparkSession, F, StructType = get_imports()
        # Session comes from conftest spark fixture (backend from env/markers)
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1

    def test_with_full_imports_object(self, spark):
        """Example: SparkImports() and spark fixture; no manual session creation."""
        from tests.fixtures.spark_imports import get_spark_imports

        imports = get_spark_imports()
        # Session from fixture; use imports for F/StructType when needed
        df = spark.createDataFrame([{"id": 1}])
        assert df.count() == 1
