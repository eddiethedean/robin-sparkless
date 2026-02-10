"""Repro for issue #211: astype/cast returns None instead of expected value."""

import json

import robin_sparkless as rs


def test_cast_int_to_string_in_with_column() -> None:
    """Basic cast: int to string in with_column; collected value should be '1', not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"num": 1}], [("num", "bigint")])
    result = df.with_column("num_str", rs.col("num").cast("string"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"


def test_cast_int_to_string_in_select() -> None:
    """Cast in select; collected value should be '1', not None."""
    spark = rs.SparkSession.builder().app_name("test").get_or_create()
    df = spark._create_dataframe_from_rows([{"num": 1}], [("num", "bigint")])
    result = df.select(rs.col("num").cast("string").alias("num_str"))
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"


def test_cast_via_execute_plan() -> None:
    """Cast in _execute_plan withColumn; collected value should be '1', not None (Sparkless path)."""
    data = [{"num": 1}]
    schema = [("num", "bigint")]
    plan = [
        {
            "op": "withColumn",
            "payload": {
                "name": "num_str",
                "expr": {"fn": "cast", "args": [{"col": "num"}, {"lit": "string"}]},
            },
        }
    ]
    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["num_str"] == "1", f"expected '1', got {rows[0]['num_str']!r}"
