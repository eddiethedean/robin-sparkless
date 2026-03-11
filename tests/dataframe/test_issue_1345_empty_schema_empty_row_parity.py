"""
Tests for #1345: createDataFrame([], StructType([])) and createDataFrame([{}], StructType([])) parity.

PySpark accepts both; sparkless now matches (empty schema + empty row -> 1 row, 0 cols).
"""

from __future__ import annotations

import pytest

from tests.fixtures.spark_imports import get_spark_imports

imports = get_spark_imports()
SparkSession = imports.SparkSession
StructType = imports.StructType


def test_empty_schema_empty_list(spark):
    """createDataFrame([], StructType([])) -> 0 rows, 0 cols (both engines)."""
    schema = StructType([])
    df = spark.createDataFrame([], schema)
    assert df.count() == 0
    assert len(df.columns) == 0


def test_empty_schema_single_empty_dict(spark):
    """createDataFrame([{}], StructType([])) -> 1 row, 0 cols (#1345 PySpark parity)."""
    schema = StructType([])
    df = spark.createDataFrame([{}], schema)
    assert df.count() == 1
    assert len(df.columns) == 0
