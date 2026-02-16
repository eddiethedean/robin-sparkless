"""Tests for issue #395: catalog.setCurrentDatabase and read.csv."""

from __future__ import annotations

import tempfile
from pathlib import Path

import robin_sparkless as rs


def test_catalog_set_current_database() -> None:
    """spark.catalog.setCurrentDatabase(name) exists and runs (no-op for default)."""
    spark = rs.SparkSession.builder().app_name("issue_395").get_or_create()
    spark.catalog.setCurrentDatabase("default")
    assert spark.catalog.currentDatabase() == "default"


def test_read_csv() -> None:
    """spark.read.csv(path) reads a CSV file (PySpark parity)."""
    spark = rs.SparkSession.builder().app_name("issue_395").get_or_create()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("a,b\n1,2\n3,4\n")
        path = f.name
    try:
        df = spark.read.csv(path)
        rows = df.collect()
        assert len(rows) == 2
        assert list(rows[0].keys()) == ["a", "b"] or list(rows[0].keys()) == [
            "column_1",
            "column_2",
        ]
        # With header, columns are a,b
        if "a" in rows[0]:
            assert rows[0]["a"] == 1 and rows[0]["b"] == 2
    finally:
        Path(path).unlink(missing_ok=True)


def test_read_csv_via_format_load() -> None:
    """spark.read.format('csv').load(path) also works."""
    spark = rs.SparkSession.builder().app_name("issue_395").get_or_create()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("x,y\n10,20\n")
        path = f.name
    try:
        df = spark.read.format("csv").load(path)
        rows = df.collect()
        assert len(rows) >= 1
    finally:
        Path(path).unlink(missing_ok=True)
