"""Tests for issue #395: catalog.setCurrentDatabase and read.csv."""

from __future__ import annotations

import tempfile
from pathlib import Path


def test_catalog_set_current_database(spark) -> None:
    """spark.catalog.setCurrentDatabase(name) exists and runs (no-op for default)."""
    spark.catalog.setCurrentDatabase("default")
    assert spark.catalog.currentDatabase() == "default"


def test_read_csv(spark) -> None:
    """spark.read.csv(path) reads a CSV file (PySpark parity)."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("a,b\n1,2\n3,4\n")
        path = f.name
    try:
        # In PySpark, header must be specified explicitly to avoid reading the header row as data.
        df = spark.read.option("header", True).csv(path)
        rows = df.collect()
        assert len(rows) == 2
        assert list(rows[0].asDict().keys()) == ["a", "b"]
        # With header, columns are a,b (CSV may infer string or int depending on backend)
        if "a" in rows[0]:
            a_val, b_val = rows[0]["a"], rows[0]["b"]
            assert a_val in (1, "1") and b_val in (2, "2")
    finally:
        Path(path).unlink(missing_ok=True)


def test_read_csv_via_format_load(spark) -> None:
    """spark.read.format('csv').load(path) also works."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("x,y\n10,20\n")
        path = f.name
    try:
        df = spark.read.format("csv").load(path)
        rows = df.collect()
        assert len(rows) >= 1
    finally:
        Path(path).unlink(missing_ok=True)
