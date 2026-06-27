"""Issue #1640: CAST expressions in SQL WHERE clause."""

from __future__ import annotations


def test_sql_cast_in_where_issue_1640(spark):
    df = spark.createDataFrame([("2025",), ("2026",)], ["col1"])
    df.createOrReplaceTempView("tbl")

    rows = spark.sql("SELECT col1 FROM tbl WHERE cast(col1 as int) > 2025").collect()

    assert len(rows) == 1
    assert rows[0].asDict()["col1"] == "2026"
