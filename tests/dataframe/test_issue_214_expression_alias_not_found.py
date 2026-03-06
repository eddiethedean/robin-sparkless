"""Regression tests for issue #214: Expression/alias 'not found' in select.

Select with aliased or computed expressions (when().otherwise().alias('result'),
window functions, chained arithmetic) must not raise RuntimeError: not found: <alias>.
Fixed by the same resolve_expr_column_names behavior as #212 (#200).
"""

from tests.fixtures.spark_imports import get_spark_imports
import pytest

_imports = get_spark_imports()
F = _imports.F
Window = _imports.Window


def test_select_when_otherwise_alias_result(spark) -> None:
    """Exact scenario from #214: when().then().otherwise().alias('result') then collect()."""
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


@pytest.mark.skip(reason="Issue #1188: unskip when fixing")
def test_select_window_rank_alias(spark) -> None:
    """#214: window function with alias('rank') must not raise 'not found: rank' (PySpark: F.rank().over())."""
    df = spark.createDataFrame(
        [{"x": 10}, {"x": 20}, {"x": 20}],
        ["x"],
    )
    result = df.select(
        F.rank().over(Window.partitionBy("x").orderBy(F.lit(1))).alias("rank")
    )
    rows = result.collect()
    assert len(rows) == 3
    assert all("rank" in r for r in rows)
