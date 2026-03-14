"""Pytest fixtures for dual sparkless/pyspark testing.

This module provides pytest fixtures that can be used directly or
registered via the pytest plugin.
"""

from __future__ import annotations

import contextlib
import gc
import os
import re
import sys
import uuid
from typing import Any, Generator

import pytest

from .mode import Mode, get_mode, is_pyspark_mode
from .imports import SparkImports, get_imports
from .session import create_session


# Prevent numpy crashes on macOS ARM chips with Python 3.9
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")

# Ensure PySpark workers use the same Python as the driver
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class _SharedSessionWrapper:
    """Wraps a shared SparkSession so stop() is a no-op.

    This prevents one test from killing the session for others when
    using shared sessions for performance.
    """

    __slots__ = ("_session",)

    def __init__(self, session: Any):
        self._session = session

    def stop(self) -> None:
        pass  # no-op

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)


# Global shared sessions (one per mode)
_shared_sessions: dict[Mode, Any] = {}


def _use_shared_session() -> bool:
    """Check if shared sessions should be used.

    Shared sessions are faster but require unique table names.
    Enable with SPARKLESS_SHARED_SESSION=1.
    """
    if os.environ.get("SPARKLESS_SHARED_SESSION", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        # Don't use shared sessions in pytest-xdist workers
        if os.environ.get("PYTEST_XDIST_WORKER"):
            return False
        # Don't use shared sessions in PySpark mode (JVM issues)
        return not is_pyspark_mode()
    return False


def _get_shared_session(mode: Mode) -> Any:
    """Get or create a shared session for the given mode."""
    if mode not in _shared_sessions:
        session = create_session(app_name="shared_test", mode=mode)
        _shared_sessions[mode] = session
    return _shared_sessions[mode]


@pytest.fixture
def spark_mode() -> Mode:
    """Get the current test mode.

    Returns:
        Mode: The current test mode (SPARKLESS or PYSPARK).

    Example:
        def test_something(spark, spark_mode):
            if spark_mode == Mode.PYSPARK:
                # PySpark-specific behavior
                pass
    """
    return get_mode()


@pytest.fixture
def spark_imports() -> SparkImports:
    """Get mode-appropriate Spark imports.

    Returns:
        SparkImports: Container with SparkSession, F, types, etc.

    Example:
        def test_imports(spark_imports):
            F = spark_imports.F
            df = spark.createDataFrame([(1,)], ["id"])
            df.select(F.col("id") + 1).show()
    """
    return get_imports()


@pytest.fixture
def spark(request: pytest.FixtureRequest) -> Generator[Any, None, None]:
    """SparkSession fixture that uses SPARKLESS_TEST_MODE to select backend.

    This is the main fixture for creating a SparkSession in tests. It
    automatically creates the appropriate session type based on the
    SPARKLESS_TEST_MODE environment variable.

    Delta Lake support can be enabled for PySpark mode by:
    - Using the @pytest.mark.delta marker on the test
    - Setting SPARKLESS_ENABLE_DELTA=1 environment variable

    Yields:
        SparkSession: A SparkSession for the current test mode.

    Example:
        def test_filter(spark):
            df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
            result = df.filter(df.id > 1).collect()
            assert len(result) == 1

        @pytest.mark.delta
        def test_delta_table(spark):
            # This test will have Delta Lake enabled in PySpark mode
            df = spark.createDataFrame([(1,)], ["id"])
            df.write.format("delta").save("/tmp/delta_table")
    """
    mode = get_mode()

    # Check if Delta Lake should be enabled
    enable_delta = False
    if request.node.get_closest_marker("delta"):
        enable_delta = True
    elif os.environ.get("SPARKLESS_ENABLE_DELTA", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        enable_delta = True

    # Use shared session if enabled (but not for delta tests)
    if _use_shared_session() and not enable_delta:
        session = _get_shared_session(mode)
        yield _SharedSessionWrapper(session)
        return

    # Create a new session for this test
    test_name = "test_app"
    if hasattr(request, "node") and hasattr(request.node, "name"):
        test_name = f"test_{request.node.name[:50]}"

    try:
        session = create_session(
            app_name=test_name, mode=mode, enable_delta=enable_delta
        )
    except (ImportError, RuntimeError) as e:
        error_msg = str(e)
        if (
            "PySpark is not available" in error_msg
            or "pyspark" in error_msg.lower()
            or "Java" in error_msg
        ):
            pytest.skip(f"PySpark session creation failed: {e}")
        raise

    yield session

    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def isolated_session(request: pytest.FixtureRequest) -> Generator[Any, None, None]:
    """Create an isolated SparkSession for tests requiring strict isolation.

    Unlike the `spark` fixture, this always creates a new session and
    never uses shared sessions, even when SPARKLESS_SHARED_SESSION is set.

    Delta Lake support can be enabled for PySpark mode by:
    - Using the @pytest.mark.delta marker on the test
    - Setting SPARKLESS_ENABLE_DELTA=1 environment variable

    Yields:
        SparkSession: A fresh, isolated SparkSession.

    Example:
        def test_isolated(isolated_session):
            # This test won't share state with other tests
            isolated_session.createDataFrame([(1,)], ["id"]).show()
    """
    mode = get_mode()
    session_name = f"test_isolated_{uuid.uuid4().hex[:8]}"

    # Check if Delta Lake should be enabled
    enable_delta = False
    if request.node.get_closest_marker("delta"):
        enable_delta = True
    elif os.environ.get("SPARKLESS_ENABLE_DELTA", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        enable_delta = True

    try:
        session = create_session(
            app_name=session_name, mode=mode, enable_delta=enable_delta
        )
    except (ImportError, RuntimeError) as e:
        error_msg = str(e)
        if (
            "PySpark is not available" in error_msg
            or "pyspark" in error_msg.lower()
            or "Java" in error_msg
        ):
            pytest.skip(f"PySpark session creation failed: {e}")
        raise

    yield session

    with contextlib.suppress(BaseException):
        session.stop()
    gc.collect()


@pytest.fixture
def table_prefix(request: pytest.FixtureRequest) -> str:
    """Unique prefix for table/view names when using shared sessions.

    Use this to ensure table names don't collide across tests when
    using SPARKLESS_SHARED_SESSION=1.

    Returns:
        str: A unique prefix like "t_test_name_abc123".

    Example:
        def test_tables(spark, table_prefix):
            df = spark.createDataFrame([(1,)], ["id"])
            df.write.saveAsTable(f"{table_prefix}_my_table")
    """
    name = getattr(request.node, "name", "test")[:40]
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    return f"t_{safe}_{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="function", autouse=True)
def cleanup_after_each_test() -> Generator[None, None, None]:
    """Automatically clean up resources after each test."""
    yield
    gc.collect()
