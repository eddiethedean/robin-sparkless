"""
Pytest fixtures for robin_sparkless Python tests.

Use the `spark` fixture in ported tests so session creation is shared and
tests can assume a single SparkSession. For expected results from PySpark,
run the same scenario with PySpark in the test or use precomputed expected
(see docs/SPARKLESS_PYTHON_TEST_PORT.md).

Multiprocessing / pytest-xdist: When using pytest-xdist (pytest -n N), call
_configure_for_multiprocessing() early to reduce worker crashes. This conftest
does so automatically.

Compatibility: If the sparkless package (4.x) is installed, it is registered
as robin_sparkless so existing tests that "import robin_sparkless" run
unchanged. Sparkless uses .builder or .builder() and .app_name() (snake_case).
"""

from __future__ import annotations

import os
import sys

import pytest

# Decide mode early so we can avoid importing sparkless in PySpark mode.
def _is_pyspark_mode() -> bool:
    """True when tests should run with real PySpark (no sparkless)."""
    return (
        (os.getenv("MOCK_SPARK_TEST_BACKEND") or os.getenv("SPARKLESS_TEST_BACKEND") or "")
        .strip()
        .lower()
        == "pyspark"
    )

# Ensure PySpark workers use the same Python as the driver when PySpark backend is used
if "PYSPARK_PYTHON" not in os.environ:
    os.environ["PYSPARK_PYTHON"] = sys.executable
if "PYSPARK_DRIVER_PYTHON" not in os.environ:
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# In PySpark mode, do not import or alias sparkless/robin_sparkless at all.
if not _is_pyspark_mode():
    # Compatibility: use sparkless package as robin_sparkless when available
    try:
        import sparkless

        sys.modules["robin_sparkless"] = sparkless
    except ImportError:
        pass

    # Limit Polars to single thread for fork-safety with pytest-xdist (issue #178).
    # Must run before any SparkSession/DataFrame operations.
    import robin_sparkless as _rs  # noqa: F401

    if getattr(_rs, "_configure_for_multiprocessing", None) is not None:
        _rs._configure_for_multiprocessing()


@pytest.fixture
def spark():
    """Yield a SparkSession: PySpark when MOCK_SPARK_TEST_BACKEND=pyspark, else robin_sparkless."""
    if _is_pyspark_mode():
        from pyspark.sql import SparkSession as PySparkSession

        session = (
            PySparkSession.builder.appName("test")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        yield session
        return
    import robin_sparkless as rs

    # .builder() supported by sparkless 4.x (classattr with __call__); else .builder
    b = getattr(rs.SparkSession.builder, "__call__", lambda: rs.SparkSession.builder)()
    session = b.app_name("test").get_or_create()
    yield session
