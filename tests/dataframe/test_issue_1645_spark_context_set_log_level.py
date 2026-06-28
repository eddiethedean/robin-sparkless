"""Regression test for issue #1645: SparkContext.setLogLevel stub."""

from __future__ import annotations


class TestIssue1645SparkContextSetLogLevel:
    """PySpark accepts spark.sparkContext.setLogLevel(level) with no return value."""

    def test_set_log_level_warn(self, spark) -> None:
        """Exact scenario from issue #1645."""
        result = spark.sparkContext.setLogLevel("WARN")
        assert result is None

    def test_set_log_level_common_levels(self, spark) -> None:
        for level in ("ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"):
            assert spark.sparkContext.setLogLevel(level) is None
