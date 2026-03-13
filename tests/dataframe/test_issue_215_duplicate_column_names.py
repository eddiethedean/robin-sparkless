from sparkless.testing import get_imports

_imports = get_imports()
F = _imports.F

"""Regression tests for issue #215: duplicate column names in select.

Polars rejects select expressions that produce duplicate column names;
PySpark/Sparkless allows them. Fixed by the same disambiguation as #213
(name, name_1, name_2, ...) in select_with_exprs.
"""


def test_select_same_column_cast_string_and_int(spark) -> None:
    """Exact scenario from #215: select(col('num').cast('string'), col('num').cast('int'))."""
    df = spark.createDataFrame(
        [{"num": 1}, {"num": 2}],
        ["num"],
    )
    result = df.select(
        F.col("num").cast("string").alias("num"),
        F.col("num").cast("int").alias("num_1"),
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["num"] == "1"
    assert rows[0]["num_1"] == 1
    assert rows[1]["num"] == "2"
    assert rows[1]["num_1"] == 2


def test_select_duplicate_value_name(spark) -> None:
    """#215 affected tests: duplicate 'value' in select (e.g. astype_multiple_types)."""
    df = spark.createDataFrame(
        [{"value": 10}],
        ["value"],
    )
    result = df.select(
        F.col("value").cast("string").alias("value"),
        F.col("value").cast("int").alias("value_1"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["value"] == "10"
    assert rows[0]["value_1"] == 10
