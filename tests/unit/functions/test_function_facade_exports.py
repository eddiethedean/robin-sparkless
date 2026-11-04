"""Guard the SQL function facade while implementations live in private modules."""

from sparkless_testing import get_imports


def test_facade_keeps_historical_callable_attributes():
    F = get_imports().F
    # These operations are used directly through ``F`` but are not all in
    # ``__all__``. The native column ``over`` binding also looks up its helper
    # on this facade at runtime.
    names = (
        "slice",
        "percentile_approx",
        "variance",
        "instr",
        "sqrt",
        "ceil",
        "window",
    )

    missing = [name for name in names if not callable(getattr(F, name, None))]

    assert not missing


def test_facade_window_helper_supports_over(spark):
    imports = get_imports()
    F = imports.F
    Window = imports.Window

    df = spark.createDataFrame([(1,), (2,)], ["id"])
    rows = df.select(F.row_number().over(Window.orderBy("id")).alias("row"))
    actual = [row["row"] for row in rows.orderBy("row").collect()]

    assert actual == [1, 2]
