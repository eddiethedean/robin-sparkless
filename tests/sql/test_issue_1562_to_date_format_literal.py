"""Tests for issue #1562: to_date format string in spark.sql with double-quoted literal."""

from __future__ import annotations


def test_sql_to_date_double_quoted_format_literal(spark) -> None:
    """PySpark parses the format argument as a string, not a column reference."""
    df = spark.createDataFrame(
        [("1", "2024/01/15 10:30:00")],
        "A STRING, B STRING",
    )
    df.createOrReplaceTempView("temp_table")

    result = spark.sql(
        """
        SELECT
            A,
            to_date(B, "yyyy/MM/dd HH:mm:ss") AS parsed_date
        FROM temp_table
        """
    )
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["A"] == "1"
    assert str(rows[0]["parsed_date"]) == "2024-01-15"
