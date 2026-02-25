"""
Tests for fixture/setup compatibility with PySpark.

This test suite verifies that test fixtures and setup code work
the same way with mock-spark as they do with PySpark.
"""

from sparkless import SparkSession


class TestFixtureCompatibility:
    """Test fixture and setup compatibility."""

    def test_session_creation_in_fixture(self):
        """Test that session creation works in fixtures."""
        spark = SparkSession("test_fixture")
        try:
            assert spark is not None
            assert spark.app_name == "test_fixture"
            assert spark.sparkContext is not None
        finally:
            spark.stop()

    def test_multiple_sessions_in_fixture(self):
        """Test that multiple sessions can be created in fixtures."""
        # Get initial count to account for sessions from other tests
        initial_count = len(SparkSession._active_sessions)

        spark1 = SparkSession("test1")
        spark2 = SparkSession("test2")

        try:
            assert spark1.app_name == "test1"
            assert spark2.app_name == "test2"

            # Both should be active (initial + 2 new sessions)
            assert len(SparkSession._active_sessions) == initial_count + 2
            assert spark1 in SparkSession._active_sessions
            assert spark2 in SparkSession._active_sessions
        finally:
            spark2.stop()
            spark1.stop()

            # Verify cleanup
            assert len(SparkSession._active_sessions) == initial_count

    def test_session_context_manager(self):
        """Test that session works as context manager."""
        with SparkSession("test_context") as spark:
            assert spark is not None
            df = spark.createDataFrame([{"id": 1}], ["id"])
            assert df is not None

        # Session should be stopped after context exit
        assert spark not in SparkSession._active_sessions

    def test_session_cleanup_after_test(self):
        """Test that sessions are properly cleaned up after tests."""
        initial_count = len(SparkSession._active_sessions)

        spark = SparkSession("test_cleanup")
        assert len(SparkSession._active_sessions) == initial_count + 1

        spark.stop()
        assert len(SparkSession._active_sessions) == initial_count

    def test_sparkcontext_available_in_session(self):
        """Test that SparkContext is available through session."""
        spark = SparkSession("test")
        try:
            # SparkContext should be accessible
            assert spark.sparkContext is not None
            assert spark.sparkContext.app_name == "test"
        finally:
            spark.stop()
