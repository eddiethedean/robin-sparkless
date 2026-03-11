"""Regression test for #1413: df.limit after range() with default step should work."""


def test_df_limit_after_range_with_default_step(spark):
    df = spark.range(0, 5).limit(2).orderBy("id")
    rows = df.collect()

    assert len(rows) == 2
    assert [r.id for r in rows] == [0, 1]

