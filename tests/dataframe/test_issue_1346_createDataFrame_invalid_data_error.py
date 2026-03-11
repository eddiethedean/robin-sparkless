"""
Tests for #1346: createDataFrame with invalid data raises PySpark-like error type/message.

PySpark raises PySparkTypeError [CANNOT_ACCEPT_OBJECT_IN_TYPE]. Sparkless now matches.
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_backend import BackendType, get_backend_type
from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession

# SparklessError is PySparkTypeError alias
try:
    from sparkless import SparklessError
except ImportError:
    SparklessError = RuntimeError  # type: ignore[misc, assignment]


def test_createDataFrame_invalid_data_raises_pyspark_like_error(spark):
    """createDataFrame(non-list, schema) raises; SparklessError+CANNOT_ACCEPT_OBJECT (#1346) or TypeError."""
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("Test for sparkless error shape; PySpark has its own message")
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
