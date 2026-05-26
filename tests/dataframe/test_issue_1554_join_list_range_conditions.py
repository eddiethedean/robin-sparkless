"""#1554: join(on=[eq, range, ...]) must AND conditions, not treat '<expr>' as column names."""

from __future__ import annotations

from datetime import date

from sparkless.testing import get_imports

_imports = get_imports()


def test_issue_1554_join_list_with_range_predicates(spark) -> None:
    """List join conditions with >= / < must not resolve '<expr>' as a column name."""
    left = spark.createDataFrame(
        [("A", date(2024, 3, 1)), ("B", date(2024, 6, 1))],
        ["CA", "CB"],
    )
    right = spark.createDataFrame(
        [("A", 99, date(2024, 1, 1), date(2024, 12, 31))],
        ["CA", "CC", "CD", "CE"],
    )
    join_cond = [
        right.CA == left.CA,
        left.CB >= right.CD,
        left.CB < right.CE,
    ]
    # Previously raised: unresolved_column: column '<expr>' not found (#1554).
    result = left.join(right, join_cond, "left").select(left.CA, left.CB, right.CC)
    rows = result.collect()
    assert len(rows) >= 1
    matched = [r for r in rows if r["CA"] == "A"]
    assert len(matched) == 1
    assert matched[0]["CC"] == 99


def test_issue_1554_join_list_multiple_equalities(spark) -> None:
    """List of equality conditions is equivalent to ANDing them (#353-style)."""
    left = spark.createDataFrame([{"a": 1, "b": 2, "v": 10}], ["a", "b", "v"])
    right = spark.createDataFrame([{"a": 1, "b": 2, "w": 20}], ["a", "b", "w"])
    from tests.utils import _row_to_dict, assert_rows_equal

    result = left.join(
        right,
        [left["a"] == right["a"], left["b"] == right["b"]],
        "inner",
    ).collect()
    assert_rows_equal(
        [_row_to_dict(r) for r in result],
        [{"a": 1, "b": 2, "v": 10, "w": 20}],
        order_matters=True,
    )
