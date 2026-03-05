def test_sql_like_simple_prefix_pattern(spark, table_prefix) -> None:
    """BUG-009 regression: basic LIKE 'A%' pattern should work in SQL."""
    tbl = f"{table_prefix}_like_unit_test"
    try:
        df = spark.createDataFrame([("Alice",), ("Bob",), ("Anna",)], ["name"])
        df.write.mode("overwrite").saveAsTable(tbl)

        result = spark.sql(f"SELECT * FROM {tbl} WHERE name LIKE 'A%'")
        names = sorted(row["name"] for row in result.collect())

        assert names == ["Alice", "Anna"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
