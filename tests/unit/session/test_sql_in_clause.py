def test_sql_in_clause_basic(spark, table_prefix) -> None:
    """BUG-010 regression: basic IN (25, 35) should filter correctly."""
    tbl = f"{table_prefix}_in_unit_test"
    try:
        df = spark.createDataFrame(
            [("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["name", "age"]
        )
        df.write.mode("overwrite").saveAsTable(tbl)

        result = spark.sql(f"SELECT * FROM {tbl} WHERE age IN (25, 35)")
        names = sorted(row["name"] for row in result.collect())

        assert names == ["Alice", "Charlie"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
