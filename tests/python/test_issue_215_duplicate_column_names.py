"""Regression tests for issue #215: duplicate column names in select.

Polars rejects select expressions that produce duplicate column names;
PySpark/Sparkless allows them. Fixed by the same disambiguation as #213
(name, name_1, name_2, ...) in select_with_exprs.
"""

import robin_sparkless as rs


def test_select_same_column_cast_string_and_int() -> None:
    """Exact scenario from #215: select(col('num').cast('string'), col('num').cast('int'))."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"num": 1}, {"num": 2}],
        [("num", "bigint")],
    )
    result = df.select(
        rs.col("num").cast("string"),
        rs.col("num").cast("int"),
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["num"] == "1"
    assert rows[0]["num_1"] == 1
    assert rows[1]["num"] == "2"
    assert rows[1]["num_1"] == 2


def test_select_duplicate_value_name() -> None:
    """#215 affected tests: duplicate 'value' in select (e.g. astype_multiple_types)."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"value": 10}],
        [("value", "bigint")],
    )
    result = df.select(
        rs.col("value").cast("string"),
        rs.col("value").cast("int"),
    )
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["value"] == "10"
    assert rows[0]["value_1"] == 10
