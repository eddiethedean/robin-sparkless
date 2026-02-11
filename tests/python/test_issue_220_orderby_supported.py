"""Tests for issue #220: orderBy not supported (Sparkless-side).

robin-sparkless supports orderBy in _execute_plan and lists it in
supported_plan_operations(). Sparkless should query supported_plan_operations()
to avoid SparkUnsupportedOperationError for orderBy.
"""

import json

import robin_sparkless as rs


def test_orderby_in_supported_plan_operations() -> None:
    """orderBy must be in supported_plan_operations() so Sparkless can allow it."""
    ops = rs.supported_plan_operations()
    assert "orderBy" in ops


def test_execute_plan_with_orderby() -> None:
    """Plan with orderBy (e.g. test_reverse_operations_in_orderby) runs successfully."""
    data = [{"x": 3}, {"x": 1}, {"x": 2}]
    schema = [("x", "bigint")]
    plan = [
        {"op": "orderBy", "payload": {"columns": ["x"], "ascending": [True]}},
    ]
    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    rows = df.collect()
    assert len(rows) == 3
    assert [r["x"] for r in rows] == [1, 2, 3]


def test_execute_plan_orderby_desc() -> None:
    """orderBy with ascending=False sorts descending."""
    data = [{"id": 1}, {"id": 2}, {"id": 3}]
    schema = [("id", "bigint")]
    plan = [
        {"op": "orderBy", "payload": {"columns": ["id"], "ascending": [False]}},
    ]
    plan_json = json.dumps(plan)
    df = rs._execute_plan(data, schema, plan_json)
    rows = df.collect()
    assert [r["id"] for r in rows] == [3, 2, 1]
