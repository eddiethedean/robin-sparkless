"""Regression tests for issue #1587: df['*'] in select() expands source DataFrame columns."""

from __future__ import annotations


def test_select_star_bracket_after_join(spark) -> None:
    """df1['*'] in select() after join expands df1 columns (PySpark parity)."""
    df1 = spark.createDataFrame(
        [("A", "Apple", "Peter"), ("B", "Banana", "Jane")],
        ["DDD", "CCC", "EEE"],
    )
    df2 = spark.createDataFrame(
        [("X1", "A", "Yes"), ("X2", "B", "Yes")],
        ["AAA", "BBB", "CCC"],
    )

    result = (
        df1.join(df2, df1.DDD == df2.BBB, "left")
        .select(df2["AAA"], df1["*"])
        .dropDuplicates()
    )

    assert result.columns == ["AAA", "DDD", "CCC", "EEE"]
    rows = sorted(result.collect(), key=lambda r: r["AAA"])
    assert rows[0].asDict() == {
        "AAA": "X1",
        "DDD": "A",
        "CCC": "Apple",
        "EEE": "Peter",
    }
    assert rows[1].asDict() == {
        "AAA": "X2",
        "DDD": "B",
        "CCC": "Banana",
        "EEE": "Jane",
    }


def test_select_star_bracket_single_dataframe(spark) -> None:
    """df['*'] on a single DataFrame selects all columns."""
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    result = df.select(df["*"])
    assert result.columns == ["a", "b", "c"]
    row = result.collect()[0]
    assert row[0] == 1 and row[1] == 2 and row[2] == 3
