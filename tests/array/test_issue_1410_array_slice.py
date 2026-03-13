"""Regression test for #1410: array.slice parity."""

from sparkless.testing import get_imports


def test_array_slice_parity(spark):
    imports = get_imports()
    F = imports.F

    df = spark.createDataFrame(
        [(["a", "b", "c"],), (["x"],), (None,)],
        ["arr"],
    )

    # Match PySpark: slice(arr, 2, 2) → ["b", "c"], [], None
    df2 = df.select(F.slice("arr", 2, 2).alias("sliced"))
    rows = df2.collect()

    assert len(rows) == 3
    assert rows[0]["sliced"] == ["b", "c"]
    assert rows[1]["sliced"] == []
    assert rows[2]["sliced"] is None
