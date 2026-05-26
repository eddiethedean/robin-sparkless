"""Tests for issue #1558: DataFrame __getitem__ supports list/tuple of column names."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession


def test_getitem_list_of_column_names_returns_dataframe() -> None:
    spark = SparkSession.builder.appName("issue_1558").get_or_create()
    df = spark.createDataFrame([("Alice", 1, "A")], ["CA", "CB", "CC"])
    out = df[["CA", "CB"]]
    assert out.columns == ["CA", "CB"]
    rows = out.collect()
    assert rows[0]["CA"] == "Alice"
    assert rows[0]["CB"] == 1


def test_getitem_tuple_of_column_names_returns_dataframe() -> None:
    spark = SparkSession.builder.appName("issue_1558_tup").get_or_create()
    df = spark.createDataFrame([("Alice", 1, "A")], ["CA", "CB", "CC"])
    out = df[("CB", "CC")]
    assert out.columns == ["CB", "CC"]
    rows = out.collect()
    assert rows[0]["CB"] == 1
    assert rows[0]["CC"] == "A"
