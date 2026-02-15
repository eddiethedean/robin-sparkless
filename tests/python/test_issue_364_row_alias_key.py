"""
Tests for #364: Row/result use column alias as key (PySpark parity).

After df.select(expr.alias("map_col")), collect() Row objects must use the alias
as the key so row["map_col"] works. PySpark uses output column names (aliases) as keys.
"""

from __future__ import annotations


def test_row_uses_alias_as_key_issue_repro() -> None:
    """rows[0]['map_col'] works when select(lit(42).alias('map_col')) (issue repro)."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_364").get_or_create()
    df = spark.createDataFrame([{"x": 1}], [("x", "int")])
    rows = df.select(rs.lit(42).alias("map_col")).collect()
    assert list(rows[0].keys()) == ["map_col"]
    assert rows[0]["map_col"] == 42


def test_row_keys_match_select_aliases() -> None:
    """Multiple aliased columns: row keys are the aliases."""
    import robin_sparkless as rs

    spark = rs.SparkSession.builder().app_name("issue_364").get_or_create()
    df = spark.createDataFrame([(1, "a")], ["id", "name"])
    rows = df.select(
        rs.col("id").alias("k"),
        rs.col("name").alias("v"),
    ).collect()
    assert list(rows[0].keys()) == ["k", "v"]
    assert rows[0]["k"] == 1
    assert rows[0]["v"] == "a"
