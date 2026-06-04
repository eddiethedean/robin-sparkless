"""Regression: json_tuple/json_object shape heuristics must not affect unrelated frames."""

from __future__ import annotations


def test_c0_c1_column_names_keep_numeric_collect(spark):
    df = spark.createDataFrame([(1, 2), (3, 4)], ["c0", "c1"])
    rows = df.collect()
    assert rows[0]["c0"] == 1
    assert rows[0]["c1"] == 2
    assert isinstance(rows[0]["c0"], int)
