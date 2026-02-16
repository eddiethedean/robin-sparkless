"""
Tests for issue #275: create_map() with no arguments (PySpark parity).

PySpark F.create_map() and F.create_map([]) return a column of empty maps ({} per row).
Robin previously raised: TypeError: py_create_map() missing 1 required positional argument: 'cols'
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_create_map_no_args() -> None:
    """create_map() with no args returns column of empty maps."""
    spark = F.SparkSession.builder().app_name("test_275").get_or_create()
    data = [{"id": 1}]
    df = spark.createDataFrame(data, [("id", "int")])
    df = df.with_column("m", F.create_map())
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["m"] is not None
    assert isinstance(rows[0]["m"], dict), "empty map should be dict {} (fixes #380)"
    assert rows[0]["m"] == {}


def test_create_map_empty_list() -> None:
    """create_map([]) also returns column of empty maps (PySpark accepts both)."""
    spark = F.SparkSession.builder().app_name("test_275").get_or_create()
    data = [{"id": 1}, {"id": 2}]
    df = spark.createDataFrame(data, [("id", "int")])
    df = df.with_column("m", F.create_map())  # no args
    rows = df.collect()
    assert len(rows) == 2
    for r in rows:
        assert isinstance(r["m"], dict), "empty map should be dict {} (fixes #380)"
        assert r["m"] == {}
