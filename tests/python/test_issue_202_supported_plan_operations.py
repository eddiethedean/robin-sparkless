"""Tests for supported_plan_operations() and plan filter (issue #202)."""

import robin_sparkless as rs


def test_supported_plan_operations_includes_filter() -> None:
    ops = rs.supported_plan_operations()
    assert isinstance(ops, tuple)
    assert "filter" in ops
    assert "select" in ops
    assert "limit" in ops
    assert "orderBy" in ops
    assert "withColumn" in ops
    assert "groupBy" in ops
    assert "join" in ops


def test_execute_plan_filter_column_column() -> None:
    """Plan with filter (column-column comparison) runs successfully (Sparkless parity)."""
    data = [{"a": 5, "b": 1}, {"a": 3, "b": 4}, {"a": 7, "b": 2}]
    schema = [("a", "bigint"), ("b", "bigint")]
    # Keep rows where a > b
    plan = [
        {
            "op": "filter",
            "payload": {"op": "gt", "left": {"col": "a"}, "right": {"col": "b"}},
        }
    ]
    import json

    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    rows = df.collect()
    assert len(rows) == 2  # (5>1), (7>2); (3>4) is false
    values = [r["a"] for r in rows]
    assert 5 in values and 7 in values
    assert 3 not in values
