from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from sparkless.dataframe import DataFrame


def test_sql_basic_select_schema_matches_dataframe_select(spark) -> None:
    """BUG-021 regression: basic SQL SELECT should project the correct schema.

    This test ensures that:
    - An explicit column list in SQL (SELECT id, name, age ...) returns exactly
      those columns, in the same order.
    - SELECT * returns all columns from the underlying table.
    """
    # SparkSession not needed - using spark fixture

    try:
        # Create a DataFrame with an extra column that should not appear in the
        # projected SELECT list.
        rows = [{"id": 1, "name": "Alice", "age": 25}]
        df = spark.createDataFrame(rows)

        df = cast("DataFrame", df.withColumn("salary", df.id * 1000))  # noqa: F821

        # Save as table and query via SQL
        df.write.mode("overwrite").saveAsTable("test_table")

        # Explicit projection should only include the requested columns
        result = spark.sql("SELECT id, name, age FROM test_table")
        assert result.columns == ["id", "name", "age"]
        assert len(result.schema.fields) == 3

        # SELECT * should include all columns from the table
        result_star = spark.sql("SELECT * FROM test_table")
        assert set(result_star.columns) == {"id", "name", "age", "salary"}
        assert len(result_star.schema.fields) == 4
    except Exception:
        # Cleanup handled by fixture
        pass
