"""
Tests for issue #274: join on columns with different types (PySpark parity).

PySpark coerces join keys to a common type (e.g. str "1" and int 1 both match).
Robin previously raised: RuntimeError: datatypes of join keys don't match.
"""

from __future__ import annotations

from tests.python.utils import get_spark


def test_join_str_key_left_int_key_right() -> None:
    """Join on 'id': left id is str, right id is int; keys are coerced to common type."""
    spark = get_spark("test_274")
    df1 = spark.createDataFrame(
        [{"id": "1", "label": "a"}],
        ["id", "label"],
    )
    df2 = spark.createDataFrame(
        [{"id": 1, "x": 10}],
        ["id", "x"],
    )
    joined = df1.join(df2, on=["id"], how="inner")
    rows = joined.collect()
    assert len(rows) == 1
    assert rows[0]["label"] == "a"
    assert rows[0]["x"] == 10
