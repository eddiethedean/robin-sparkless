"""
Tests for SparkContext/Session validation in function calls.

Asserts PySpark behavior: col(), lit(), when(), expr(), aggregate and window
functions work with an active SparkSession. (PySpark requires active SparkContext
for these when called from Python.)
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession
F = imports.F


class TestSessionValidation:
    """Assert PySpark behavior: expression-building functions work with active session."""

    def test_col_works_with_active_session(self, spark):
        """col() works with active session."""
        col_expr = F.col("id")
        assert col_expr is not None
        assert (
            hasattr(col_expr, "name")
            or hasattr(col_expr, "_jc")
            or hasattr(col_expr, "column_name")
        )

    def test_lit_works_with_active_session(self, spark):
        """lit() works with active session."""
        lit_expr = F.lit(42)
        assert lit_expr is not None

    def test_expr_works_with_active_session(self, spark):
        """expr() works with active session."""
        col_expr = F.expr("id + 1")
        assert col_expr is not None

    def test_when_works_with_active_session(self, spark):
        """when() works with active session."""
        when_expr = F.when(F.col("x") > 0, 1)
        assert when_expr is not None

    def test_aggregate_functions_work_with_active_session(self, spark):
        """count/sum/avg work with active session."""
        c1 = F.count("id")
        c2 = F.sum("value")
        c3 = F.avg("value")
        assert c1 is not None
        assert c2 is not None
        assert c3 is not None

    def test_window_functions_work_with_active_session(self, spark):
        """row_number(), rank() work with active session."""
        rn = F.row_number()
        rk = F.rank()
        assert rn is not None
        assert rk is not None

    def test_datetime_functions_work_with_active_session(self, spark):
        """current_date(), current_timestamp() work with active session."""
        d = F.current_date()
        t = F.current_timestamp()
        assert d is not None
        assert t is not None

    @pytest.mark.skip(reason="Issue #1250: unskip when fixing")
    def test_multiple_sessions(self, spark):
        """getActiveSession() returns one of the active sessions; col() fails after all are stopped (PySpark)."""
        spark2 = SparkSession.builder.appName("test2").getOrCreate()
        try:
            col_expr = F.col("id")
            assert col_expr is not None
            active = SparkSession.getActiveSession()
            assert active is not None
        finally:
            spark2.stop()
        # After stopping the singleton SparkSession in PySpark, there is no
        # active SparkContext and expression builders like col() assert.
        import pytest

        with pytest.raises(AssertionError):
            F.col("id")
