"""Pytest plugin for sparkless.testing.

This module provides the pytest plugin entry point that registers
all fixtures from sparkless.testing when users add:

    pytest_plugins = ["sparkless.testing"]

to their conftest.py.
"""

from __future__ import annotations

import pytest

from .mode import Mode, ENV_VAR_NAME


# Import fixtures to register them with pytest
from .fixtures import (
    spark,
    spark_mode,
    spark_imports,
    isolated_session,
    table_prefix,
    cleanup_after_each_test,
)


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest with custom markers for sparkless testing."""
    config.addinivalue_line(
        "markers",
        "sparkless_only: mark test to run only in sparkless mode",
    )
    config.addinivalue_line(
        "markers",
        "pyspark_only: mark test to run only in PySpark mode",
    )
    config.addinivalue_line(
        "markers",
        "delta: mark test as requiring Delta Lake (may be skipped)",
    )
    config.addinivalue_line(
        "markers",
        "performance: mark test as a performance benchmark",
    )
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration test (may require external setup)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    """Skip tests based on mode markers.

    Tests marked with @pytest.mark.sparkless_only are skipped in PySpark mode.
    Tests marked with @pytest.mark.pyspark_only are skipped in sparkless mode.
    """
    from .mode import get_mode

    mode = get_mode()

    skip_sparkless = pytest.mark.skip(
        reason="Test marked sparkless_only, running in PySpark mode"
    )
    skip_pyspark = pytest.mark.skip(
        reason="Test marked pyspark_only, running in sparkless mode"
    )

    for item in items:
        if mode == Mode.PYSPARK and "sparkless_only" in item.keywords:
            item.add_marker(skip_sparkless)
        elif mode == Mode.SPARKLESS and "pyspark_only" in item.keywords:
            item.add_marker(skip_pyspark)


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Add sparkless testing info to pytest header."""
    from .mode import get_mode

    mode = get_mode()
    return [f"sparkless.testing mode: {mode.value}"]


# Re-export fixtures so they're registered when this module is loaded as a plugin
__all__ = [
    "spark",
    "spark_mode",
    "spark_imports",
    "isolated_session",
    "table_prefix",
    "cleanup_after_each_test",
    "pytest_configure",
    "pytest_collection_modifyitems",
    "pytest_report_header",
]
