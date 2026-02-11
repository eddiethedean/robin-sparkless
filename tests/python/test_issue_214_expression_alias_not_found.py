"""Regression tests for issue #214: Expression/alias 'not found' in select.

Select with aliased or computed expressions (when().otherwise().alias('result'),
window functions, chained arithmetic) must not raise RuntimeError: not found: <alias>.
Fixed by the same resolve_expr_column_names behavior as #212 (#200).
"""

import robin_sparkless as rs


def test_select_when_otherwise_alias_result() -> None:
    """Exact scenario from #214: when().then().otherwise().alias('result') then collect()."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"x": 1}, {"x": 2}],
        [("x", "bigint")],
    )
    result = df.select(
        rs.when(rs.col("x") > 1)
        .then(rs.lit("yes"))
        .otherwise(rs.lit("no"))
        .alias("result")
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["result"] == "no"
    assert rows[1]["result"] == "yes"


def test_select_window_rank_alias() -> None:
    """#214: window function with alias('rank') must not raise 'not found: rank'."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows(
        [{"x": 10}, {"x": 20}, {"x": 20}],
        [("x", "bigint")],
    )
    result = df.select(rs.col("x").rank(False).over(["x"]).alias("rank"))
    rows = result.collect()
    assert len(rows) == 3
    assert all("rank" in r for r in rows)
