"""Tests for issue #390: DataFrame.sampleBy, crosstab, freqItems."""

from __future__ import annotations



def test_sample_by(spark) -> None:
    """sampleBy(col, fractions, seed) stratified sample by column value."""
    # col "x" values 1,2,3; sample 100% of 1, 50% of 2, 0% of 3
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "c"), (2, "d"), (3, "e")], ["x", "y"]
    )
    out = df.sampleBy("x", {1: 1.0, 2: 0.5, 3: 0.0}, seed=42)
    rows = out.collect()
    # All x=1 (2 rows), ~half of x=2 (1 or 2 rows), no x=3
    assert len(rows) >= 2 and len(rows) <= 4
    xs = [r["x"] for r in rows]
    assert 3 not in xs
    assert all(x in (1, 2) for x in xs)


def test_crosstab(spark) -> None:
    """crosstab(col1, col2) returns col1, col2, count."""
    from tests.utils import _row_to_dict
    df = spark.createDataFrame(
        [("a", "x"), ("a", "x"), ("a", "y"), ("b", "x")],
        ["c1", "c2"],
    )
    out = df.crosstab("c1", "c2")
    rows = out.collect()
    assert len(rows) >= 1
    names = list(_row_to_dict(rows[0]).keys())
    # PySpark crosstab returns first col (e.g. c1_c2), then one column per c2 value
    assert len(names) >= 2


def test_freq_items(spark) -> None:
    """freqItems(columns, support) returns one row with {col}_freqItems arrays."""
    df = spark.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["x", "y"])
    out = df.freqItems(["x", "y"], support=0.3)
    rows = out.collect()
    assert len(rows) == 1
    # Columns should be x_freqItems, y_freqItems (or similar)
    from tests.utils import _row_to_dict
    keys = list(_row_to_dict(rows[0]).keys())
    assert any("freqItems" in k for k in keys)
