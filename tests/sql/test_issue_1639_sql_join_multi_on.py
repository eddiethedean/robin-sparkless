"""Issue #1639: SQL JOIN ON with multiple AND conditions."""

from __future__ import annotations


def test_sql_join_on_multiple_conditions_issue_1639(spark):
    df_a = spark.createDataFrame([("A", 1), ("B", 2)], ["col1", "col2"])
    df_b = spark.createDataFrame(
        [("A", 1, "X"), ("B", 3, "Y")], ["col1", "col2", "col3"]
    )
    df_a.createOrReplaceTempView("tbl_a")
    df_b.createOrReplaceTempView("tbl_b")

    rows = spark.sql(
        """
        SELECT a.col1, b.col3
        FROM tbl_a a
        JOIN tbl_b b ON a.col1 = b.col1 AND a.col2 = b.col2
        """
    ).collect()

    assert len(rows) == 1
    row = rows[0].asDict()
    # Qualified SELECT output uses prefixed names (a_col1, b_col3) after aliased JOIN.
    assert (row.get("col1") or row.get("a_col1")) == "A"
    assert (row.get("col3") or row.get("b_col3")) == "X"
