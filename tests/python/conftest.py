"""
Pytest fixtures for robin_sparkless Python tests.

Use the `spark` fixture in ported tests so session creation is shared and
tests can assume a single SparkSession. For expected results from PySpark,
run the same scenario with PySpark in the test or use precomputed expected
(see docs/SPARKLESS_PYTHON_TEST_PORT.md).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def spark():
    """Yield a robin_sparkless SparkSession for the test."""
    import robin_sparkless as rs

    session = rs.SparkSession.builder().app_name("test").get_or_create()
    yield session
