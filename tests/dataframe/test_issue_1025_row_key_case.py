"""
Tests for #1025: Row key / schema case sensitivity (Key not found in row).

After select/agg, row keys must match the output column names (aliases), not internal
qualified names like "Person.name". PySpark preserves alias names in row keys.
"""

from __future__ import annotations

from tests.utils import get_functions, get_spark, _row_to_dict

F = get_functions()


def _spark():
    return get_spark("issue_1025")


def test_select_alias_row_keys_match_output_names() -> None:
    """df.select(col('id').alias('ID'), 'name').collect() -> row['ID'], row['name'] (not KeyError)."""
    spark = _spark()
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


def test_select_alias_as_dict() -> None:
    """row.asDict() returns dict with same keys as row (alias names)."""
    spark = _spark()
    df = spark.createDataFrame([(2, "b")], ["id", "name"])
    rows = df.select(F.col("id").alias("ID"), "name").collect()
    d = rows[0].asDict()
    assert d == {"ID": 2, "name": "b"}
