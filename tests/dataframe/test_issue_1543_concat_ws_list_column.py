"""Tests for issue #1543: concat_ws on list and string columns."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def test_concat_ws_on_list_column(spark) -> None:
    """concat_ws(sep, array_col) joins list elements (issue #1543 repro)."""
    df = spark.createDataFrame(
        [["A", ["B", "C"]]],
        ["D", "E"],
    )
    result = df.withColumn("E", F.concat_ws(", ", F.col("E")))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["D"] == "A"
    assert rows[0]["E"] == "B, C"


def test_concat_ws_on_string_column_is_noop(spark) -> None:
    """concat_ws(sep, string_col) returns the string unchanged (PySpark parity)."""
    df = spark.createDataFrame([{"remarks": "hello"}], schema="remarks string")
    result = df.withColumn("out", F.concat_ws(", ", F.col("remarks")))
    rows = result.collect()
    assert rows[0]["out"] == "hello"
