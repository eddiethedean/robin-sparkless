"""Issue #1643: pivot().agg() supports F.lit() and F.first() aggregations."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F


def _issue_1643_df(spark):
    return spark.createDataFrame(
        [("A", "X", "v1", "w1"), ("A", "Y", "v2", "w2"), ("B", "X", "v3", "w3")],
        ["col1", "col2", "col3", "col4"],
    )


def test_pivot_agg_lit_issue_1643(spark) -> None:
    df = _issue_1643_df(spark)
    result = df.groupBy("col1").pivot("col2").agg(F.lit(1))
    rows = {r["col1"]: r.asDict() for r in result.collect()}

    assert set(result.columns) == {"col1", "X", "Y"}
    assert rows["A"]["X"] == 1
    assert rows["A"]["Y"] == 1
    assert rows["B"]["X"] == 1
    assert rows["B"]["Y"] is None


def test_pivot_agg_multiple_first_issue_1643(spark) -> None:
    df = _issue_1643_df(spark)
    result = df.groupBy("col1").pivot("col2").agg(
        F.first(F.col("col3")).alias("col3"),
        F.first(F.col("col4")).alias("col4"),
    )
    rows = {r["col1"]: r.asDict() for r in result.collect()}

    assert "X_col3" in result.columns
    assert "X_col4" in result.columns
    assert "Y_col3" in result.columns
    assert "Y_col4" in result.columns
    assert rows["A"]["X_col3"] == "v1"
    assert rows["A"]["X_col4"] == "w1"
    assert rows["A"]["Y_col3"] == "v2"
    assert rows["A"]["Y_col4"] == "w2"
    assert rows["B"]["X_col3"] == "v3"
    assert rows["B"]["X_col4"] == "w3"
    assert rows["B"]["Y_col3"] is None
    assert rows["B"]["Y_col4"] is None
