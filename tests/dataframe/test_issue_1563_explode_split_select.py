"""Tests for issue #1563: explode(split(col)) in select replicates other columns."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_select_explode_split_replicates_id(spark) -> None:
    """PySpark expands rows and keeps id on each exploded word."""
    df = spark.createDataFrame(
        [("1", "hello world")],
        "id STRING, name STRING",
    )
    result = df.select("id", F.explode(F.split(F.col("name"), " ")).alias("word"))
    rows = result.collect()

    assert len(rows) == 2
    assert {r["id"] for r in rows} == {"1"}
    words = sorted(r["word"] for r in rows)
    assert words == ["hello", "world"]
