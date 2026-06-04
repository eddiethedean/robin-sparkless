"""Regression tests for issue #1570: withColumn(explode(split(...))) on a string column."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_withcolumn_explode_split_replaces_name(spark) -> None:
    """PySpark: withColumn('name', explode(split(name, ' '))) expands into multiple rows."""
    df = spark.createDataFrame([{"name": "john doe"}])
    result = df.withColumn("name", F.explode(F.split(F.col("name"), " ")))
    rows = result.collect()

    assert len(rows) == 2
    words = sorted(r["name"] for r in rows)
    assert words == ["doe", "john"]


def test_withcolumn_explode_split_preserves_other_columns(spark) -> None:
    """Sibling columns replicate when exploding split into a new column."""
    df = spark.createDataFrame([{"id": 1, "name": "a b"}])
    result = df.withColumn("word", F.explode(F.split(F.col("name"), " ")))
    rows = result.collect()

    assert len(rows) == 2
    assert all(r["id"] == 1 for r in rows)
    assert sorted(r["word"] for r in rows) == ["a", "b"]
