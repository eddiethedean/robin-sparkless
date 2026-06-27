"""Issue #1638: Spark SQL built-in functions in spark.sql()."""

from __future__ import annotations

import math

import pytest


@pytest.fixture
def tbl(spark):
    df = spark.createDataFrame(
        [(1, 10.0, "abc-123"), (2, 100.0, "xy"), (3, None, "z")],
        ["col1", "col2", "col3"],
    )
    df.createOrReplaceTempView("tbl")
    return df


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT coalesce(col2, 0) AS c FROM tbl WHERE col1 = 2", {"c": 100.0}),
        ("SELECT coalesce(col2, 0) AS c FROM tbl WHERE col1 = 3", {"c": 0.0}),
        ("SELECT nvl(col2, 0) AS n FROM tbl WHERE col1 = 3", {"n": 0.0}),
        ("SELECT log10(col2) AS l FROM tbl WHERE col1 = 2", {"l": 2.0}),
        ("SELECT log(col2) AS l FROM tbl WHERE col1 = 2", {"l": math.log(100.0)}),
        ("SELECT log(10, col2) AS l FROM tbl WHERE col1 = 2", {"l": 2.0}),
        ("SELECT log2(col2) AS l FROM tbl WHERE col1 = 2", {"l": math.log2(100.0)}),
        ("SELECT pow(col2, 2) AS p FROM tbl WHERE col1 = 1", {"p": 100.0}),
        ("SELECT mod(col1, 3) AS m FROM tbl WHERE col1 = 1", {"m": 1.0}),
        (
            r"SELECT regexp_extract(col3, '([0-9]+)', 1) AS r FROM tbl WHERE col1 = 1",
            {"r": "123"},
        ),
        ("SELECT len(col3) AS l FROM tbl WHERE col1 = 1", {"l": 7}),
    ],
)
def test_sql_builtin_functions_issue_1638(spark, tbl, sql, expected):
    del tbl
    rows = spark.sql(sql).collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    for key, val in expected.items():
        actual = row[key]
        if isinstance(val, float):
            assert actual == pytest.approx(val)
        else:
            assert actual == val
