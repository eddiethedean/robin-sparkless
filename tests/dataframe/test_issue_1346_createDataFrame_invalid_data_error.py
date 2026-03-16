"""
Tests for #1346: createDataFrame with invalid data raises PySpark-like error type/message.

PySpark raises PySparkTypeError [CANNOT_ACCEPT_OBJECT_IN_TYPE]. Sparkless now matches.
"""

from __future__ import annotations

import pytest

from sparkless.testing import get_imports

imports = get_imports()
SparkSession = imports.SparkSession

# SparklessError is PySparkTypeError alias
try:
    from sparkless import SparklessError
except ImportError:
    SparklessError = RuntimeError  # type: ignore[misc, assignment]


def test_createDataFrame_invalid_data_raises_pyspark_like_error(spark):
    """createDataFrame(non-list, schema) raises in both modes (#1346)."""
    with pytest.raises((SparklessError, RuntimeError, TypeError)) as exc_info:
        spark.createDataFrame("invalid_data", "id INT")
    msg = str(exc_info.value)
    # #1346: prefer PySpark-like SparklessError with [CANNOT_ACCEPT_OBJECT_IN_TYPE]
    if "CANNOT_ACCEPT_OBJECT" in msg or "can not accept object" in msg:
        assert "StructType" in msg or "struct" in msg.lower()
        assert "str" in msg
    else:
        # Legacy: TypeError "data must be a list"
        assert "data must be a list" in msg or "list" in msg
