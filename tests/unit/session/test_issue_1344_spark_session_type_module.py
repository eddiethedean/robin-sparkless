"""
Tests for #1344: SparkSession type __module__ identifies sparkless (not builtins).

Code that detects engine via type(spark).__module__ should see "sparkless" for sparkless.
"""

from __future__ import annotations

import pytest

from sparkless.testing import Mode, get_mode, is_pyspark_mode, create_session
from sparkless.testing import get_imports

imports = get_imports()
SparkSession = imports.SparkSession


def test_spark_session_type_module_identifies_sparkless(spark):
    """type(spark).__module__ should contain 'sparkless' for engine detection (#1344)."""
    if get_mode() == Mode.PYSPARK:
        pytest.skip("PySpark reports pyspark.sql.session; test is for sparkless")
    t = type(spark)
    assert "sparkless" in t.__module__, (
        f"Expected 'sparkless' in type(spark).__module__ for engine detection, got {t.__module__!r}"
    )
    assert t.__module__ == "sparkless.sql.session", (
        f"Expected __module__ 'sparkless.sql.session', got {t.__module__!r}"
    )
    assert t.__name__ in ("SparkSession", "PySparkSession")
