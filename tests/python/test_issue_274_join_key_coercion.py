"""
Tests for issue #274: join on columns with different types (PySpark parity).

PySpark coerces join keys to a common type (e.g. str "1" and int 1 both match).
Robin previously raised: RuntimeError: datatypes of join keys don't match.
"""

from __future__ import annotations

import robin_sparkless as rs

F = rs


def test_join_str_key_left_int_key_right() -> None:
    """Join on 'id': left id is str, right id is int; keys are coerced to common type."""
    spark = F.SparkSession.builder().app_name("test_274").get_or_create()
    create_df = getattr(spark, "create_dataframe_from_rows", None) or getattr(
        spark, "_create_dataframe_from_rows"
    )
    df1 = create_df(
        [{"id": "1", "label": "a"}],
        [("id", "str"), ("label", "str")],
    )
    df2 = create_df(
        [{"id": 1, "x": 10}],
        [("id", "int"), ("x", "int")],
    )
    joined = df1.join(df2, on=["id"], how="inner")
    rows = joined.collect()
    assert len(rows) == 1
    assert rows[0]["label"] == "a"
    assert rows[0]["x"] == 10
