"""Tests for issue #212: Expression/alias 'not found' in select.

Alias output names (when().otherwise().alias('result'), rank().over([]).alias('rank'),
etc.) must not be resolved as input columns; they are preserved by resolve_expr_column_names (see #200).
"""

import json

import robin_sparkless as rs


def test_select_when_otherwise_alias() -> None:
    """Select with when().then().otherwise().alias('result') should not raise 'not found: result'."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
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
    """Select with col().rank().over([...]).alias('rank') should not raise 'not found: rank'."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark.createDataFrame(
        [{"x": 10}, {"x": 20}, {"x": 20}],
        [("x", "bigint")],
    )
    # Partition by x so over() has at least one key; alias "rank" must not be resolved as input column.
    result = df.select(rs.col("x").rank(False).over(["x"]).alias("rank"))
    rows = result.collect()
    assert len(rows) == 3
    assert all("rank" in r for r in rows)
    assert all(isinstance(r["rank"], int) for r in rows)


def test_select_expression_alias_via_execute_plan() -> None:
    """Plan select with expression + alias (Sparkless path) should not resolve alias as input column."""
    data = [{"a": 1}, {"a": 2}]
    schema = [("a", "bigint")]
    plan = [
        {
            "op": "select",
            "payload": [
                {
                    "name": "result",
                    "expr": {
                        "op": "gt",
                        "left": {"col": "a"},
                        "right": {"lit": 1},
                    },
                }
            ],
        }
    ]
    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["result"] is False  # a=1 > 1 is false
    assert rows[1]["result"] is True  # a=2 > 1 is true
