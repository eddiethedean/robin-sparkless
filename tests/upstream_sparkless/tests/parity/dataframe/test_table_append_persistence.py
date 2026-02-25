"""Test table append persistence - ensures data is visible after append writes.

Tests fix for issue #112: Table data not visible after append write.
"""

from tests.fixtures.parity_base import ParityTestBase


class TestTableAppendPersistence(ParityTestBase):
    """Test that table data is immediately visible after append writes."""

    def test_append_data_visible_immediately(self, spark):
        """Test that data appended to a table is immediately visible.

        This test verifies the fix for issue #112 where data written with
        append mode was not immediately visible when querying the table.
        """
        # Create schema
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Create table with initial data
        data1 = [{"id": 1, "name": "test1"}]
        df1 = spark.createDataFrame(data1, "id int, name string")
        df1.write.mode("overwrite").saveAsTable("test_schema.test_table")

        # Verify initial data
        result1 = spark.table("test_schema.test_table")
        assert result1.count() == 1, "Initial table should have 1 row"

        # Append more data
        data2 = [{"id": 2, "name": "test2"}]
        df2 = spark.createDataFrame(data2, "id int, name string")
        df2.write.mode("append").saveAsTable("test_schema.test_table")

        # Verify appended data is immediately visible
        result2 = spark.table("test_schema.test_table")
        count = result2.count()
        assert count == 2, (
            f"Table should have 2 rows after append, got {count}. "
            "This verifies fix for issue #112."
        )

        # Verify all data is present
        rows = result2.collect()
        assert len(rows) == 2
        assert rows[0]["id"] in [1, 2]
        assert rows[1]["id"] in [1, 2]
        assert {row["id"] for row in rows} == {1, 2}

    def test_append_to_new_table(self, spark):
        """Test that appending to a non-existent table creates it correctly."""
        # Create schema
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Append to non-existent table (should create it)
        data1 = [{"id": 1, "name": "test1"}]
        df1 = spark.createDataFrame(data1, "id int, name string")
        df1.write.mode("append").saveAsTable("test_schema.new_table")

        # Verify data is immediately visible
        result = spark.table("test_schema.new_table")
        assert result.count() == 1, "New table created by append should have 1 row"

        # Append more data
        data2 = [{"id": 2, "name": "test2"}]
        df2 = spark.createDataFrame(data2, "id int, name string")
        df2.write.mode("append").saveAsTable("test_schema.new_table")

        # Verify all data is visible
        result2 = spark.table("test_schema.new_table")
        assert result2.count() == 2, "Table should have 2 rows after second append"

    def test_multiple_append_operations(self, spark):
        """Test that multiple append operations preserve all data."""
        # Create schema
        spark.sql("CREATE SCHEMA IF NOT EXISTS test_schema")

        # Create initial table
        data1 = [{"id": 1, "value": "a"}]
        df1 = spark.createDataFrame(data1, "id int, value string")
        df1.write.mode("overwrite").saveAsTable("test_schema.multi_append")

        # Perform multiple append operations
        for i in range(2, 6):
            data = [{"id": i, "value": chr(ord("a") + i - 1)}]
            df = spark.createDataFrame(data, "id int, value string")
            df.write.mode("append").saveAsTable("test_schema.multi_append")

            # Verify data is visible after each append
            result = spark.table("test_schema.multi_append")
            assert result.count() == i, (
                f"After {i - 1} appends, table should have {i} rows, got {result.count()}"
            )

        # Final verification
        result = spark.table("test_schema.multi_append")
        assert result.count() == 5, "Final table should have 5 rows"
        rows = result.collect()
        ids = {row["id"] for row in rows}
        assert ids == {1, 2, 3, 4, 5}, "All rows should be present"
