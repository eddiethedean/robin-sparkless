"""
Tests for SparkContext/Session validation in function calls.

This test suite verifies that functions require an active SparkSession,
matching PySpark's behavior exactly.
"""

import pytest
from sparkless import SparkSession, functions as F


class TestSessionValidation:
    """Test that functions require active SparkSession."""

    def test_col_does_not_require_active_session(self):
        """Test that col() does NOT require active session (PySpark behavior)."""
        # Clear any existing sessions
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        # In PySpark, col() can be called without a session
        # It just creates a column expression that's evaluated later
        col_expr = F.col("id")
        assert col_expr is not None
        assert hasattr(col_expr, "name") or hasattr(col_expr, "column_name")

    def test_col_works_with_active_session(self):
        """Test that col() works with active session."""
        spark = SparkSession("test")
        try:
            col_expr = F.col("id")
            assert col_expr is not None
            assert hasattr(col_expr, "name") or hasattr(col_expr, "column_name")
        finally:
            spark.stop()

    def test_col_works_after_session_stopped(self):
        """Test that col() still works after session is stopped (PySpark behavior)."""
        spark = SparkSession("test")
        spark.stop()

        # In PySpark, col() can be called even after session is stopped
        # It just creates a column expression
        col_expr = F.col("id")
        assert col_expr is not None

    def test_lit_does_not_require_active_session(self):
        """Test that lit() does NOT require active session (PySpark behavior)."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        # In PySpark, lit() can be called without a session
        # It just creates a literal expression that's evaluated later
        lit_expr = F.lit(42)
        assert lit_expr is not None

    def test_expr_requires_active_session(self):
        """Test that expr() requires active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.expr("id + 1")

    def test_when_requires_active_session(self):
        """Test that when() requires active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.when(F.col("x") > 0, 1)

    def test_aggregate_functions_require_session(self):
        """Test that aggregate functions require active session."""
        # In PySpark mode, aggregate functions don't require session check
        # They're called on DataFrames that already have sessions
        # Skip this test in PySpark mode as it's Sparkless-specific validation
        import os

        if os.getenv("MOCK_SPARK_TEST_BACKEND") == "pyspark":
            pytest.skip(
                "This test is Sparkless-specific and doesn't apply to PySpark mode"
            )

        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.count("id")

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.sum("value")

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.avg("value")

    def test_window_functions_require_session(self):
        """Test that window functions require active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.row_number()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.rank()

    def test_datetime_functions_require_session(self):
        """Test that datetime functions require active session."""
        SparkSession._active_sessions.clear()
        SparkSession._singleton_session = None

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.current_date()

        with pytest.raises(RuntimeError, match="No active SparkSession found"):
            F.current_timestamp()

    def test_multiple_sessions(self):
        """Test session tracking with multiple sessions."""
        spark1 = SparkSession("test1")
        spark2 = SparkSession("test2")

        try:
            # Should work with active sessions (col() doesn't require session, but works with it)
            col_expr = F.col("id")
            assert col_expr is not None

            # Most recent session should be active (use getActiveSession for PySpark compatibility)
            active = SparkSession.getActiveSession()
            assert active is not None
            # Should be one of the active sessions (prefer singleton, otherwise most recent)
            assert active in [spark1, spark2]
        finally:
            spark2.stop()
            spark1.stop()

        # col() should still work after all sessions stopped (PySpark behavior)
        # It just creates a column expression that's evaluated later
        col_expr = F.col("id")
        assert col_expr is not None
