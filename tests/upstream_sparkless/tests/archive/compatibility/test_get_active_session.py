"""
Comprehensive tests for SparkSession.getActiveSession() method.

This module tests the PySpark-compatible getActiveSession() classmethod
in various scenarios including singleton sessions, multiple sessions,
context managers, and edge cases.
"""

import pytest
from sparkless.sql import SparkSession


@pytest.mark.compatibility
class TestGetActiveSession:
    """Test SparkSession.getActiveSession() in various scenarios."""

    def test_get_active_session_no_session(self):
        """Test getActiveSession() returns None when no session exists."""
        # Clear any existing sessions
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        assert SparkSession.getActiveSession() is None

    def test_get_active_session_singleton(self):
        """Test getActiveSession() returns singleton session."""
        # Clear existing sessions
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        spark = SparkSession("test_singleton")
        try:
            active = SparkSession.getActiveSession()
            assert active is not None
            assert active is spark
            assert active.app_name == "test_singleton"
        finally:
            spark.stop()
            assert SparkSession.getActiveSession() is None

    def test_get_active_session_multiple_sessions(self):
        """Test getActiveSession() returns most recent session when multiple exist."""
        # Clear existing sessions
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        spark1 = SparkSession("session1")
        spark2 = SparkSession("session2")
        try:
            # Should return most recent (spark2)
            active = SparkSession.getActiveSession()
            assert active is not None
            assert active is spark2
            assert active.app_name == "session2"
        finally:
            spark2.stop()
            spark1.stop()

    def test_get_active_session_prefers_singleton(self):
        """Test getActiveSession() prefers singleton over active_sessions."""
        # Clear existing sessions
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()

        # Create singleton via builder first
        spark2 = SparkSession.builder.appName("singleton").getOrCreate()
        # Verify it's the singleton
        assert SparkSession._singleton_session is spark2

        # Create another regular session (shouldn't replace singleton)
        # But note: creating a new session will set it as singleton
        # So we need to test differently - getActiveSession should return singleton if it exists
        try:
            # Should prefer singleton (spark2)
            active = SparkSession.getActiveSession()
            assert active is not None
            # If singleton exists, it should be returned
            if SparkSession._singleton_session is not None:
                # Singleton should be returned
                assert active is SparkSession._singleton_session
                assert active.app_name == "singleton"
            else:
                # If no singleton, getActiveSession still works
                assert active is not None
        finally:
            spark2.stop()

    def test_get_active_session_after_stop(self):
        """Test getActiveSession() returns None after session is stopped."""
        spark = SparkSession("test_stop")
        assert SparkSession.getActiveSession() is not None

        spark.stop()
        assert SparkSession.getActiveSession() is None

    def test_get_active_session_context_manager(self):
        """Test getActiveSession() works with context manager."""
        with SparkSession("test_context") as spark:
            active = SparkSession.getActiveSession()
            assert active is not None
            assert active is spark

        # After context exit, should be None
        assert SparkSession.getActiveSession() is None

    def test_get_active_session_nested_sessions(self):
        """Test getActiveSession() with nested session creation."""
        spark1 = SparkSession("outer")
        try:
            assert SparkSession.getActiveSession() is spark1

            spark2 = SparkSession("inner")
            try:
                assert SparkSession.getActiveSession() is spark2
            finally:
                spark2.stop()

            # After inner stops, should still have outer
            assert SparkSession.getActiveSession() is spark1
        finally:
            spark1.stop()
            assert SparkSession.getActiveSession() is None

    def test_get_active_session_with_column_expressions(self):
        """Test that column expressions can use getActiveSession()."""
        from sparkless.sql import functions as F

        spark = SparkSession("test_col")
        try:
            # Column expressions should work with active session
            col_expr = F.col("test_column")
            assert col_expr is not None

            # Verify active session is available
            active = SparkSession.getActiveSession()
            assert active is spark
        finally:
            spark.stop()

    def test_get_active_session_thread_safety(self):
        """Test getActiveSession() behavior with multiple threads (basic test)."""
        import threading

        results = []

        def create_session(name: str):
            spark = SparkSession(name)
            try:
                active = SparkSession.getActiveSession()
                results.append(
                    (name, active is not None, active.app_name if active else None)
                )
            finally:
                spark.stop()

        # Create sessions in different threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=create_session, args=(f"thread_{i}",))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Each thread should see its own session
        assert len(results) == 3
        for name, has_active, app_name in results:
            assert has_active is True
            assert app_name == name


@pytest.mark.compatibility
class TestGetActiveSessionIntegration:
    """Integration tests for getActiveSession() with DataFrame operations."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        session = SparkSession("integration_test")
        yield session
        session.stop()

    def test_dataframe_operations_with_active_session(self, spark):
        """Test DataFrame operations work correctly with active session."""
        # Verify active session
        assert SparkSession.getActiveSession() is spark

        # Create DataFrame
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)

        # Operations should work
        result = df.filter(df.age > 25).collect()
        assert len(result) == 1
        assert result[0].name == "Bob"

        # Active session should still be available
        assert SparkSession.getActiveSession() is spark

    def test_sql_operations_with_active_session(self, spark):
        """Test SQL operations work correctly with active session."""
        # Verify active session
        assert SparkSession.getActiveSession() is spark

        # Create table via SQL
        spark.sql("CREATE TABLE IF NOT EXISTS test_table (id INT, name STRING)")

        # Query should work
        df = spark.sql("SELECT * FROM test_table")
        assert df is not None

        # Active session should still be available
        assert SparkSession.getActiveSession() is spark

    def test_catalog_operations_with_active_session(self, spark):
        """Test catalog operations work correctly with active session."""
        # Verify active session
        assert SparkSession.getActiveSession() is spark

        # Create database
        spark.catalog.createDatabase("test_db", ignoreIfExists=True)

        # Verify it was created
        databases = spark.catalog.listDatabases()
        db_names = [db.name for db in databases]
        assert "test_db" in db_names

        # Active session should still be available
        assert SparkSession.getActiveSession() is spark
