"""Tests for #380: Empty create_map() return dict not list in row."""

from __future__ import annotations

from tests.python.utils import get_functions

F = get_functions()


def test_empty_create_map_collects_as_dict(spark) -> None:
    """create_map() with no args: collected row value is {} (dict), not [] (list)."""
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    out = df.withColumn("m", F.create_map())
    rows = out.collect()
    assert len(rows) == 2
    for r in rows:
        m = r["m"]
        assert isinstance(m, dict), f"expected dict, got {type(m)}"
        assert m == {}, f"expected {{}}, got {m}"


def test_non_empty_create_map_collects_as_dict(spark) -> None:
    """create_map(col, col) collects to Python dict {key: value}."""
    df = spark.createDataFrame([("a", 1), ("b", 2)], ["k", "v"])
    out = df.withColumn("m", F.create_map(F.col("k"), F.col("v")))
    rows = out.collect()
    assert len(rows) == 2
    assert rows[0]["m"] == {"a": 1}
    assert rows[1]["m"] == {"b": 2}
