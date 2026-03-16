"""
Tests for #1344: SparkSession type __module__ identifies sparkless (not builtins).

Code that detects engine via type(spark).__module__ should see "sparkless" for sparkless.
"""

from __future__ import annotations


from sparkless.testing import get_imports

imports = get_imports()
SparkSession = imports.SparkSession


def test_spark_session_type_module_identifies_backend(spark):
    """type(spark).__module__ identifies the backend in both modes (#1344)."""
    t = type(spark)
    mod = t.__module__
    assert "sparkless" in mod or "pyspark" in mod, (
        f"Expected 'sparkless' or 'pyspark' in type(spark).__module__, got {mod!r}"
    )
    assert "session" in mod or "sql" in mod
    assert t.__name__ in ("SparkSession", "PySparkSession")
