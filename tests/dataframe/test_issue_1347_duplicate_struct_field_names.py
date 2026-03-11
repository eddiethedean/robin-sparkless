"""
Tests for #1347: duplicate field names in StructType (PySpark allows, sparkless rejects).

Robin-sparkless rejects schemas with duplicate field names; PySpark accepts them.
See docs/PYSPARK_DIFFERENCES.md.
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_backend import BackendType, get_backend_type
from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession
StructType = imports.StructType
StructField = imports.StructField
IntegerType = imports.IntegerType
StringType = imports.StringType


def test_duplicate_field_names_sparkless_raises(spark):
    """Sparkless rejects StructType with duplicate field names (#1347)."""
    if get_backend_type() == BackendType.PYSPARK:
        pytest.skip("Only sparkless rejects duplicate field names; PySpark allows them")
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("id", StringType()),
        ]
    )
    # One row, two values (positional) for the two columns
    data = [[1, "a"]]
    with pytest.raises(Exception) as exc_info:
        spark.createDataFrame(data, schema=schema)
    msg = str(exc_info.value)
    assert "duplicate column name" in msg or "duplicate" in msg.lower()
    assert "id" in msg


def test_duplicate_field_names_pyspark_allows(spark):
    """PySpark allows StructType with duplicate field names (#1347)."""
    if get_backend_type() != BackendType.PYSPARK:
        pytest.skip("PySpark backend only")
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("id", StringType()),
        ]
    )
    data = [[1, "a"]]
    df = spark.createDataFrame(data, schema=schema)
    assert df.count() == 1
    assert len(df.columns) == 2
