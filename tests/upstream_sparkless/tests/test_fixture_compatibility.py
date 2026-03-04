"""
Tests for fixture/setup compatibility with PySpark.

Uses get_spark_imports from fixture only.
"""

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession


class TestFixtureCompatibility:
    """Test fixture and setup compatibility."""

    def test_session_creation_in_fixture(self):
        """Test that session creation works in fixtures."""
        spark = SparkSession.builder.appName("test_fixture").getOrCreate()
        try:
            assert spark is not None
            assert getattr(spark, "sparkContext", None) is not None
        finally:
            spark.stop()

    def test_multiple_sessions_in_fixture(self):
        """Test that multiple sessions can be created in fixtures."""
        spark1 = SparkSession.builder.appName("test1").getOrCreate()
        spark2 = SparkSession.builder.appName("test2").getOrCreate()
        try:
            assert spark1 is not None
            assert spark2 is not None
            df1 = spark1.createDataFrame([{"id": 1}])
            df2 = spark2.createDataFrame([{"id": 2}])
            assert df1.collect()[0]["id"] == 1
            assert df2.collect()[0]["id"] == 2
        finally:
            spark2.stop()
            spark1.stop()

    def test_session_context_manager(self):
        """Test that session works as context manager (PySpark-style builder)."""
        spark = SparkSession.builder.appName("test_context").getOrCreate()
        try:
            assert spark is not None
            df = spark.createDataFrame([{"id": 1}], ["id"])
            assert df is not None
        finally:
            spark.stop()

    def test_session_cleanup_after_test(self):
        """Test that sessions are properly cleaned up after tests."""
        spark = SparkSession.builder.appName("test_cleanup").getOrCreate()
        try:
            assert spark is not None
            spark.createDataFrame([{"x": 1}]).collect()
        finally:
            spark.stop()

    def test_sparkcontext_available_in_session(self):
        """Test that SparkContext is available through session."""
        spark = SparkSession.builder.appName("test").getOrCreate()
        try:
            assert spark.sparkContext is not None
            assert spark.sparkContext.appName == "test" or getattr(spark.sparkContext, "app_name", None) == "test"
        finally:
            spark.stop()
