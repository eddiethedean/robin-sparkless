"""Tests for issue #1542: createDataFrame accepts tuple (and iterable) of dict rows."""

from __future__ import annotations


def test_create_dataframe_tuple_of_dicts(spark) -> None:
    """PySpark accepts a tuple of dicts like a list of dicts."""
    rows = (
        {"name": "Alice"},
        {"name": "Bob"},
    )
    df = spark.createDataFrame(rows)
    out = df.collect()
    assert len(out) == 2
    assert out[0]["name"] == "Alice"
    assert out[1]["name"] == "Bob"


def test_create_dataframe_generator_of_dicts(spark) -> None:
    """PySpark accepts any iterable of dict rows."""

    def row_iter():
        yield {"name": "Alice"}
        yield {"name": "Bob"}

    df = spark.createDataFrame(row_iter())
    out = df.collect()
    assert len(out) == 2
    assert {r["name"] for r in out} == {"Alice", "Bob"}


def test_create_dataframe_list_of_dicts_still_works(spark) -> None:
    """Regression: list of dicts unchanged."""
    df = spark.createDataFrame([{"name": "Alice"}, {"name": "Bob"}])
    assert len(df.collect()) == 2
