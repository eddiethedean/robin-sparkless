"""C-1: warehouse table() must not read outside spark.sql.warehouse.dir."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession


def test_table_rejects_path_traversal_outside_warehouse(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    secret = outside / "secret_table"
    secret.mkdir()
  # empty dir mimics saveAsTable layout

    spark = (
        SparkSession.builder.app_name("path-security")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    try:
        with pytest.raises(Exception):
            spark.table("../../outside/secret_table").collect()
    finally:
        spark.stop()


def test_valid_warehouse_table_reads(tmp_path) -> None:
    warehouse = tmp_path / "warehouse"
    table_dir = warehouse / "people"
    table_dir.mkdir(parents=True)
    # minimal parquet via sparkless
    spark = (
        SparkSession.builder.app_name("path-security-ok")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .getOrCreate()
    )
    try:
        df = spark.createDataFrame([{"id": 1}], schema="id bigint")
        df.write.mode("overwrite").parquet(str(table_dir / "data.parquet"))
        out = spark.table("people").collect()
        assert len(out) == 1
    finally:
        spark.stop()
