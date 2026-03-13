"""Regression test for #1407: array_size / size parity on null and empty arrays."""

from sparkless.testing import Mode, create_session, get_imports


def test_array_size_parity():
    imports = get_imports(Mode.SPARKLESS)
    F = imports.F
    spark = create_session(app_name="array_size_1407", mode=Mode.SPARKLESS)

    df = spark.createDataFrame(
        [(["a", "b"],), ([],), (None,)],
        ["arr"],
    )

    out = df.select(F.size("arr").alias("out")).collect()
    vals = [row["out"] for row in out]

    # Current Sparkless behavior (and PySpark for size(null)): 2, 0, None.
    assert vals == [2, 0, None]

    spark.stop()
