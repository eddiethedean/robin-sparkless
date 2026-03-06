"""Tests for #406: na.fill(value, subset=[list of str])."""

from __future__ import annotations
import pytest

@pytest.mark.skip(reason="Issue #1237: unskip when fixing")

def test_na_fill_subset_list_of_str(spark) -> None:
    """df.na.fill(0, subset=["b"]) fills nulls only in "b", leaves "a" unchanged."""
    df = spark.createDataFrame(
        [{"a": 1, "b": None}, {"a": None, "b": 2}],
        schema=["a", "b"],
    )
    result = df.na.fill(0, subset=["b"]).collect()
    rows = list(result)
    assert len(rows) == 2
    # First row: a=1, b was null -> filled with 0
    assert rows[0]["a"] == 1
    assert rows[0]["b"] == 0
    # Second row: a was null (unchanged), b=2
    assert rows[1]["a"] is None
    assert rows[1]["b"] == 2
