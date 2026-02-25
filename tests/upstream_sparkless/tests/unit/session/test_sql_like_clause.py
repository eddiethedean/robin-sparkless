def test_sql_like_simple_prefix_pattern(spark) -> None:
    """BUG-009 regression: basic LIKE 'A%' pattern should work in SQL."""
    # SparkSession not needed - using spark fixture

    try:
        df = spark.createDataFrame([("Alice",), ("Bob",), ("Anna",)], ["name"])
        df.write.mode("overwrite").saveAsTable("like_unit_test")

        result = spark.sql("SELECT * FROM like_unit_test WHERE name LIKE 'A%'")
        names = sorted(row["name"] for row in result.collect())

        assert names == ["Alice", "Anna"]
    finally:
        spark.sql("DROP TABLE IF EXISTS like_unit_test")
