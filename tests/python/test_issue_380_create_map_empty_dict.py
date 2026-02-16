"""Tests for #380: Empty create_map() return dict not list in row."""

from __future__ import annotations

import robin_sparkless as rs
from robin_sparkless import create_map, col


def _spark():
    return rs.SparkSession.builder().app_name("issue_380").get_or_create()


def test_empty_create_map_collects_as_dict() -> None:
    """create_map() with no args: collected row value is {} (dict), not [] (list)."""
    spark = _spark()
    df = spark.createDataFrame([{"id": 1}, {"id": 2}], schema=[("id", "int")])
    out = df.with_column("m", create_map())
    rows = out.collect()
    assert len(rows) == 2
    for r in rows:
        m = r["m"]
        assert isinstance(m, dict), f"expected dict, got {type(m)}"
        assert m == {}, f"expected {{}}, got {m}"


def test_non_empty_create_map_collects_as_dict() -> None:
    """create_map(col, col) collects to Python dict {key: value}."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
        schema=[("k", "string"), ("v", "int")],
    )
    out = df.with_column("m", create_map(col("k"), col("v")))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["m"] == {"a": 1}
    assert rows[1]["m"] == {"b": 2}
