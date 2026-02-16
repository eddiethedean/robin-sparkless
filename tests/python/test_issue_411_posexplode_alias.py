"""
Tests for #411: posexplode().alias("pos", "val") for select (PySpark parity).
"""

from __future__ import annotations

import robin_sparkless as rs


def _spark() -> rs.SparkSession:
    return rs.SparkSession.builder().app_name("issue_411").get_or_create()


def test_posexplode_alias_in_select() -> None:
    """posexplode("arr").alias("pos", "val") returns (pos_col, val_col) usable in select."""
    spark = _spark()
    df = spark.createDataFrame(
        [{"id": 1, "arr": [10, 20]}],
        schema=[("id", "bigint"), ("arr", "list")],
    )
    # Select only posexplode result (two columns); no mixing with id to avoid row-length mismatch.
    out = df.select(rs.posexplode("arr").alias("pos", "val"))
    rows = out.collect()
    assert out.columns() == ["pos", "val"]
    assert len(rows) == 2
    # posexplode uses 1-based position (PySpark parity)
    assert rows[0] == {"pos": 1, "val": 10}
    assert rows[1] == {"pos": 2, "val": 20}
