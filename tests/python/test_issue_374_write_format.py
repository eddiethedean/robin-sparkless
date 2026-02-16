"""Tests for #374: DataFrame.write.format() and DataFrameWriter API (PySpark parity)."""

from __future__ import annotations

import os
import tempfile

import robin_sparkless as rs


def _spark():
    return rs.SparkSession.builder().app_name("issue_374").get_or_create()


def test_write_as_property_format_mode_save() -> None:
    """df.write (property) has .format(), .mode(), .save() - no parentheses on write."""
    spark = _spark()
    df = spark.createDataFrame([{"id": 1}, {"id": 2}], schema=[("id", "int")])
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "out")
        # PySpark style: df.write.format(...).mode(...).save(path)
        df.write.format("parquet").mode("overwrite").save(path)
        read_back = spark.read().parquet(path)
        rows = read_back.collect()
        assert len(rows) == 2


def test_write_parquet_shortcut() -> None:
    """df.write.parquet(path) works."""
    spark = _spark()
    df = spark.createDataFrame([{"x": 1}], schema=[("x", "int")])
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "p.parquet")
        df.write.parquet(path)
        back = spark.read().parquet(path)
        assert len(back.collect()) == 1
