def test_sql_in_clause_basic(spark) -> None:
    """BUG-010 regression: basic IN (25, 35) should filter correctly."""
    # SparkSession not needed - using spark fixture

    try:
        df = spark.createDataFrame(
            [("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["name", "age"]
        )
        df.write.mode("overwrite").saveAsTable("in_unit_test")

        result = spark.sql("SELECT * FROM in_unit_test WHERE age IN (25, 35)")
        names = sorted(row["name"] for row in result.collect())

        assert names == ["Alice", "Charlie"]
    finally:
        spark.sql("DROP TABLE IF EXISTS in_unit_test")
