"""
Tests for #1025: Row key / schema case sensitivity (Key not found in row).

After select/agg, row keys must match the output column names (aliases), not internal
qualified names like "Person.name". PySpark preserves alias names in row keys.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

from tests.utils import _row_to_dict


def test_select_alias_row_keys_match_output_names(spark) -> None:
    """df.select(col('id').alias('ID'), 'name').collect() -> row['ID'], row['name'] (not KeyError)."""
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    rows = df.select(F.col("id").alias("ID"), "name").collect()
    assert len(rows) == 1
    row = rows[0]
    d = _row_to_dict(row)
    # Row keys must be the output names (alias "ID" and column "name"), not schema-internal names.
    assert d["ID"] == 1
    assert d["name"] == "a"
    assert "ID" in d
    assert "name" in d
    assert sorted(d.keys()) == ["ID", "name"]


def test_select_alias_as_dict(spark) -> None:
    """row.asDict() returns dict with same keys as row (alias names)."""
    df = spark.createDataFrame([(2, "b")], ["id", "name"])
    rows = df.select(F.col("id").alias("ID"), "name").collect()
    d = rows[0].asDict()
    assert d == {"ID": 2, "name": "b"}
