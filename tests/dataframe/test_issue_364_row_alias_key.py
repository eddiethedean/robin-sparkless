"""
Tests for #364: Row/result use column alias as key (PySpark parity).

After df.select(expr.alias("map_col")), collect() Row objects must use the alias
as the key so row["map_col"] works. PySpark uses output column names (aliases) as keys.
"""

from __future__ import annotations

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
F = _imports.F

from tests.utils import _row_to_dict

def test_row_uses_alias_as_key_issue_repro(spark) -> None:
    """rows[0]['map_col'] works when select(lit(42).alias('map_col')) (issue repro)."""
    df = spark.createDataFrame([{"x": 1}], ["x"])
    rows = df.select(F.lit(42).alias("map_col")).collect()
    row_dict = _row_to_dict(rows[0])
    assert list(row_dict.keys()) == ["map_col"]
    assert row_dict["map_col"] == 42


def test_row_keys_match_select_aliases(spark) -> None:
    """Multiple aliased columns: row keys are the aliases."""
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    rows = df.select(
        F.col("id").alias("k"),
        F.col("name").alias("v"),
    ).collect()
    row_dict = _row_to_dict(rows[0])
    assert list(row_dict.keys()) == ["k", "v"]
    assert row_dict["k"] == 1
    assert row_dict["v"] == "a"
