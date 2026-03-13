"""Dual sparkless/pyspark testing framework.

This module provides a unified API for writing tests that can run against
both sparkless (Rust/Polars) and PySpark backends. Use the SPARKLESS_TEST_MODE
environment variable to switch between modes:

- SPARKLESS_TEST_MODE=sparkless (default): Run with sparkless
- SPARKLESS_TEST_MODE=pyspark: Run with PySpark

Quick Start (for external projects):

1. Add to your conftest.py:

    pytest_plugins = ["sparkless.testing"]

2. Use the `spark` fixture in your tests:

    def test_filter(spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        result = df.filter(df.id > 1).collect()
        assert len(result) == 1

3. Run tests:

    # Run with sparkless (default)
    pytest tests/

    # Run with PySpark
    SPARKLESS_TEST_MODE=pyspark pytest tests/

For more control, use the module directly:

    from sparkless.testing import Mode, create_session, get_imports

    spark = create_session(mode=Mode.SPARKLESS)
    imports = get_imports()
    F = imports.F

Example usage with comparison utilities:

    from sparkless.testing import assert_dataframes_equal, assert_rows_equal

    def test_transform(spark):
        df1 = transform_a(spark)
        df2 = transform_b(spark)
        assert_dataframes_equal(df1, df2, check_order=False)
"""

from __future__ import annotations

# Mode detection and configuration
from .mode import (
    Mode,
    ENV_VAR_NAME,
    get_mode,
    is_pyspark_mode,
    is_sparkless_mode,
    set_mode,
)

# Session creation
from .session import create_session

# Unified imports
from .imports import SparkImports, get_imports

# Comparison utilities
from .comparison import (
    ComparisonResult,
    compare_dataframes,
    assert_dataframes_equal,
    assert_rows_equal,
)

# Pytest fixtures (for direct import if not using plugin)
from .fixtures import (
    spark,
    spark_mode,
    spark_imports,
    isolated_session,
    table_prefix,
)

__all__ = [
    # Mode
    "Mode",
    "ENV_VAR_NAME",
    "get_mode",
    "is_pyspark_mode",
    "is_sparkless_mode",
    "set_mode",
    # Session
    "create_session",
    # Imports
    "SparkImports",
    "get_imports",
    # Comparison
    "ComparisonResult",
    "compare_dataframes",
    "assert_dataframes_equal",
    "assert_rows_equal",
    # Fixtures
    "spark",
    "spark_mode",
    "spark_imports",
    "isolated_session",
    "table_prefix",
]
