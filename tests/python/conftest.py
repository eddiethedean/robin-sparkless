"""
Pytest fixtures for robin_sparkless Python tests.

Use the `spark` fixture in ported tests so session creation is shared and
tests can assume a single SparkSession. For expected results from PySpark,
run the same scenario with PySpark in the test or use precomputed expected
(see docs/SPARKLESS_PYTHON_TEST_PORT.md).

Multiprocessing / pytest-xdist: When using pytest-xdist (pytest -n N), call
_configure_for_multiprocessing() early to reduce worker crashes. This conftest
does so automatically.
"""

from __future__ import annotations

import pytest

# Limit Polars to single thread for fork-safety with pytest-xdist (issue #178).
# Must run before any SparkSession/DataFrame operations.
import robin_sparkless as _rs  # noqa: F401

_rs._configure_for_multiprocessing()


@pytest.fixture
def spark():
    """Yield a robin_sparkless SparkSession for the test."""
    import robin_sparkless as rs

    session = rs.SparkSession.builder().app_name("test").get_or_create()
    yield session
