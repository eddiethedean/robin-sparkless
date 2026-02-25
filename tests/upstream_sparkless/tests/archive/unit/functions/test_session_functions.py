"""
Tests for session-aware SQL functions such as current_database() and current_user().
"""

from __future__ import annotations

import getpass

import pytest

from sparkless.sql import SparkSession, functions as F


class TestSessionFunctions:
    """Verify session-aware helpers reflect Spark session state."""

    def setup_method(self) -> None:
        SparkSession._singleton_session = None
        assert SparkSession.builder is not None  # Type guard for mypy
        self.spark = SparkSession.builder.appName(
            "session-functions-test"
        ).getOrCreate()
        self.spark.catalog.createDatabase("analytics", ignoreIfExists=True)
        self.spark.catalog.setCurrentDatabase("analytics")
        assert self.spark.catalog.currentDatabase() == "analytics"

    def teardown_method(self) -> None:
        try:
            self.spark.catalog.setCurrentDatabase("default")
            self.spark.catalog.dropDatabase("analytics", ignoreIfNotExists=True)
        finally:
            self.spark.stop()
            SparkSession._singleton_session = None

    def test_current_database_uses_active_session(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_database()).collect()[0][0]  # type: ignore[operator]
        assert result == "analytics"

    def test_current_schema_aliases_current_database(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_schema()).collect()[0][0]  # type: ignore[operator]
        assert result == "analytics"

    def test_current_catalog_returns_default(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_catalog()).collect()[0][0]  # type: ignore[operator]
        assert result == "spark_catalog"

    def test_current_user_reflects_spark_context_user(self) -> None:
        df = self.spark.createDataFrame([{"value": 1}])
        result = df.select(F.current_user()).collect()[0][0]  # type: ignore[operator]
        assert result == getpass.getuser()

    def test_current_helpers_are_session_isolated(self) -> None:
        """Ensure session-aware helpers reflect each session's catalog state."""
        primary_row = (
            self.spark.createDataFrame([{"value": 1}])
            .select(
                F.current_database(),  # type: ignore[operator]
                F.current_catalog(),  # type: ignore[operator]
                F.current_user(),  # type: ignore[operator]
            )
            .collect()[0]
        )

        # Capture original session BEFORE newSession (newSession changes the singleton)
        original_session = SparkSession._singleton_session
        other_session = self.spark.newSession()
        try:
            other_session.catalog.createDatabase("default", ignoreIfExists=True)
            other_session.catalog.setCurrentDatabase("default")

            SparkSession._singleton_session = other_session
            secondary_row = (
                other_session.createDataFrame([{"value": 1}])
                .select(
                    F.current_database(),  # type: ignore[operator]
                    F.current_catalog(),  # type: ignore[operator]
                    F.current_user(),  # type: ignore[operator]
                )
                .collect()[0]
            )
        finally:
            other_session.stop()
            SparkSession._singleton_session = original_session or self.spark

        assert primary_row[0] == "analytics"
        assert primary_row[1] == "spark_catalog"
        assert secondary_row[0] == "default"
        assert secondary_row[1] == "spark_catalog"
        assert primary_row[2] == secondary_row[2] == getpass.getuser()

        reaffirm = (
            self.spark.createDataFrame([{"value": 1}])
            .select(F.current_database())  # type: ignore[operator]
            .collect()[0][0]
        )
        assert reaffirm == "analytics"

    def test_current_helpers_survive_catalog_drop_and_recreate(self) -> None:
        """`current_*` helpers stay consistent across catalog lifecycle events."""
        initial_row = (
            self.spark.createDataFrame([{"value": 1}])
            .select(
                F.current_database(),  # type: ignore[operator]
                F.current_schema(),  # type: ignore[operator]
                F.current_catalog(),  # type: ignore[operator]
            )
            .collect()[0]
        )
        assert initial_row[0] == "analytics"
        assert initial_row[1] == "analytics"
        assert initial_row[2] == "spark_catalog"

        self.spark.catalog.setCurrentDatabase("default")
        self.spark.catalog.dropDatabase("analytics", ignoreIfNotExists=False)
        self.spark.catalog.createDatabase("analytics", ignoreIfExists=False)
        self.spark.catalog.setCurrentDatabase("analytics")

        post_reset_row = (
            self.spark.createDataFrame([{"value": 2}])
            .select(
                F.current_database(),  # type: ignore[operator]
                F.current_schema(),  # type: ignore[operator]
                F.current_catalog(),  # type: ignore[operator]
            )
            .collect()[0]
        )

        assert post_reset_row[0] == "analytics"
        assert post_reset_row[1] == "analytics"
        assert post_reset_row[2] == "spark_catalog"

    def test_error_when_no_active_session(self) -> None:
        SparkSession._singleton_session = None
        SparkSession._active_sessions.clear()
        from sparkless.errors import PySparkValueError
        from sparkless.functions.functions import Functions

        try:
            # F.current_database() might use a different error path
            # Test with a function that definitely requires active session
            with pytest.raises((PySparkValueError, RuntimeError)):
                Functions.current_database()
        finally:
            SparkSession._singleton_session = self.spark
            if self.spark not in SparkSession._active_sessions:
                SparkSession._active_sessions.append(self.spark)
