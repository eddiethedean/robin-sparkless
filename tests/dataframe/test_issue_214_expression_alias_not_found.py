"""Regression tests for issue #214: Expression/alias 'not found' in select.

Select with aliased or computed expressions (when().otherwise().alias('result'),
window functions, chained arithmetic) must not raise RuntimeError: not found: <alias>.
Fixed by the same resolve_expr_column_names behavior as #212 (#200).
"""

from tests.utils import get_functions, get_spark, get_window_cls

F = get_functions()
Window = get_window_cls()


def test_select_when_otherwise_alias_result() -> None:
    """Exact scenario from #214: when().then().otherwise().alias('result') then collect()."""
    spark = get_spark("issue_214")
    df = spark.createDataFrame(
        [{"x": 1}, {"x": 2}],
        ["x"],
    )
    # PySpark: when(cond, value).otherwise(else_value); no .then()
    result = df.select(
        F.when(F.col("x") > 1, F.lit("yes")).otherwise(F.lit("no")).alias("result")
    )
    rows = result.collect()
    assert len(rows) == 2
    assert rows[0]["result"] == "no"
    assert rows[1]["result"] == "yes"


def test_select_window_rank_alias() -> None:
    """#214: window function with alias('rank') must not raise 'not found: rank' (PySpark: F.rank().over())."""
    spark = get_spark("issue_214")
    df = spark.createDataFrame(
        [{"x": 10}, {"x": 20}, {"x": 20}],
        ["x"],
    )
    result = df.select(F.rank().over(Window.partitionBy("x").orderBy(F.lit(1))).alias("rank"))
    rows = result.collect()
    assert len(rows) == 3
    assert all("rank" in r for r in rows)
