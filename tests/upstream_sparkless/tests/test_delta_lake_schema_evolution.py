"""
Comprehensive tests for Delta Lake schema evolution in mock-spark.

These tests validate that mock-spark behaves like PySpark for schema evolution,
specifically for Delta Lake features like overwriteSchema and schema merging.

Note: These tests are primarily for mock-spark's schema evolution features.
When running with PySpark, some tests may be skipped or need special handling
due to differences in table management and schema evolution behavior.
"""

import pytest

from tests.fixtures.spark_backend import get_backend_type, BackendType


def _is_sparkless_mode() -> bool:
    """Check if running in sparkless mode."""
    backend = get_backend_type()
    return backend == BackendType.MOCK


# Import appropriate types and functions based on backend
_backend = get_backend_type()
if _backend == BackendType.PYSPARK:
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StringType,
            IntegerType,
            DoubleType,
            LongType,
            TimestampType,
            DateType,
            BooleanType,
            StructField,
        )
    except ImportError:
        from sparkless import functions as F
        from sparkless import (
            StringType,
            IntegerType,
            DoubleType,
            LongType,
            TimestampType,
            DateType,
            BooleanType,
            StructField,
        )
else:
    from sparkless import functions as F
    from sparkless import (
        StringType,
        IntegerType,
        DoubleType,
        LongType,
        TimestampType,
        DateType,
        BooleanType,
        StructField,
    )


@pytest.mark.xdist_group(name="delta_serial")
class TestDeltaLakeSchemaEvolution:
    """Test Delta Lake schema evolution features."""

    def test_lit_none_works(self, spark):
        """Test that F.lit(None) works without JVM error."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # This should work without errors - no JVM dependency
        result = df.withColumn("null_col", F.lit(None))
        assert "null_col" in result.columns

        # Should also work with casting
        result2 = df.withColumn("null_str", F.lit(None).cast(StringType()))
        assert "null_str" in result2.columns

        # Verify the column exists and can be selected
        rows = result2.select("null_str").collect()
        assert len(rows) == 1
        assert rows[0]["null_str"] is None

    def test_type_casting_works(self, spark):
        """Test that type casting works without JVM."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Test casting None to different types
        df1 = df.withColumn("null_str", F.lit(None).cast(StringType()))
        df2 = df.withColumn("null_int", F.lit(None).cast(IntegerType()))
        df3 = df.withColumn("null_double", F.lit(None).cast(DoubleType()))

        assert "null_str" in df1.columns
        assert "null_int" in df2.columns
        assert "null_double" in df3.columns

        # Test casting actual values
        df4 = df.withColumn("id_str", F.col("id").cast(StringType()))
        assert df4.select("id_str").collect()[0]["id_str"] == "1"

    def test_null_literal_casting_to_various_types(self, spark):
        """Test casting None to various types preserves type in schema."""
        df = spark.createDataFrame([("Alice",)], ["name"])

        # Cast None to different types (PySpark API)
        df = df.withColumn("str_col", F.lit(None).cast(StringType()))
        df = df.withColumn("int_col", F.lit(None).cast(IntegerType()))
        df = df.withColumn("ts_col", F.lit(None).cast(TimestampType()))
        df = df.withColumn("date_col", F.lit(None).cast(DateType()))
        df = df.withColumn("bool_col", F.lit(None).cast(BooleanType()))
        df = df.withColumn("long_col", F.lit(None).cast(LongType()))

        # Verify types in schema (PySpark API - iterate through fields)
        schema = df.schema
        str_field = next((f for f in schema.fields if f.name == "str_col"), None)
        int_field = next((f for f in schema.fields if f.name == "int_col"), None)
        ts_field = next((f for f in schema.fields if f.name == "ts_col"), None)
        date_field = next((f for f in schema.fields if f.name == "date_col"), None)
        bool_field = next((f for f in schema.fields if f.name == "bool_col"), None)
        long_field = next((f for f in schema.fields if f.name == "long_col"), None)

        assert str_field is not None
        assert int_field is not None
        assert ts_field is not None
        assert date_field is not None
        assert bool_field is not None
        assert long_field is not None

        # Check types using isinstance (PySpark API)
        assert isinstance(str_field.dataType, StringType)
        assert isinstance(int_field.dataType, IntegerType)
        assert isinstance(ts_field.dataType, TimestampType)
        assert isinstance(date_field.dataType, DateType)
        assert isinstance(bool_field.dataType, BooleanType)
        assert isinstance(long_field.dataType, LongType)

        # Verify values are None
        # PySpark always returns a list
        rows = df.head(1)
        assert len(rows) == 1
        row = rows[0]
        assert row["str_col"] is None
        assert row["int_col"] is None
        assert row["ts_col"] is None
        assert row["date_col"] is None
        assert row["bool_col"] is None
        assert row["long_col"] is None

        # Verify columns are nullable
        assert str_field.nullable is True
        assert int_field.nullable is True
        assert ts_field.nullable is True

    def test_null_cast_preserves_schema_type(self, spark):
        """Test that F.lit(None).cast() preserves type information in schema."""
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])

        # Add a null column with specific type
        df_with_timestamp = df.withColumn(
            "created_at", F.lit(None).cast(TimestampType())
        )

        # Verify the schema (PySpark API)
        assert "created_at" in df_with_timestamp.columns
        created_at_field = next(
            (f for f in df_with_timestamp.schema.fields if f.name == "created_at"), None
        )
        assert created_at_field is not None
        assert isinstance(created_at_field.dataType, TimestampType)
        assert created_at_field.nullable is True

        # Verify the value is None
        rows = df_with_timestamp.head(1)
        assert len(rows) == 1
        row = rows[0]
        assert row["created_at"] is None

    def test_add_column_with_null_and_type(self, spark):
        """Test adding column with null values and specific type for schema evolution."""
        # Create initial DataFrame
        df = spark.createDataFrame([("Alice", 25)], ["name", "age"])

        # Add null column with specific type (simulating schema evolution)
        df = df.withColumn("created_at", F.lit(None).cast(TimestampType()))

        # Verify schema evolution worked (PySpark API)
        assert "created_at" in df.columns
        created_at_field = next(
            (f for f in df.schema.fields if f.name == "created_at"), None
        )
        assert created_at_field is not None
        assert isinstance(created_at_field.dataType, TimestampType)
        assert created_at_field.nullable is True

        # Verify data
        rows = df.head(1)
        assert len(rows) == 1
        row = rows[0]
        assert row["name"] == "Alice"
        assert row["age"] == 25
        assert row["created_at"] is None

    def test_add_column_from_struct_field(self, spark):
        """Test adding column using StructField.dataType."""
        df = spark.createDataFrame([("Alice",)], ["name"])

        # Add column from StructField
        field = StructField("created_at", TimestampType(), nullable=True)
        df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

        # Verify (PySpark API)
        assert "created_at" in df.columns
        created_at_field = next(
            (f for f in df.schema.fields if f.name == "created_at"), None
        )
        assert created_at_field is not None
        assert isinstance(created_at_field.dataType, TimestampType)
        rows = df.head(1)
        assert len(rows) == 1
        assert rows[0]["created_at"] is None

    def test_schema_merge_with_type_preservation(self, spark):
        """Test merging schemas preserves types correctly."""
        import uuid

        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.employees"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Existing table schema
        existing_df = spark.createDataFrame(
            [("Alice", 25, 50000.0)], ["name", "age", "salary"]
        )
        existing_df.write.mode("overwrite").saveAsTable(table_name)

        # New DataFrame with different columns
        new_df = spark.createDataFrame([("Bob", "engineer")], ["name", "job"])

        # Add missing columns to match existing schema using StructField
        existing_schema = spark.table(table_name).schema
        for field in existing_schema.fields:
            if field.name not in new_df.columns:
                new_df = new_df.withColumn(field.name, F.lit(None).cast(field.dataType))

        # Verify types are preserved (PySpark API)
        age_field = next((f for f in new_df.schema.fields if f.name == "age"), None)
        salary_field = next(
            (f for f in new_df.schema.fields if f.name == "salary"), None
        )
        assert age_field is not None
        assert salary_field is not None
        # Note: Python int is inferred as LongType, not IntegerType
        assert isinstance(age_field.dataType, LongType)
        assert isinstance(salary_field.dataType, DoubleType)
        rows = new_df.head(1)
        assert len(rows) == 1
        assert rows[0]["age"] is None
        assert rows[0]["salary"] is None

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass

    def test_delta_create_or_replace_table_as_select(self, spark):
        """CTAS with OR REPLACE should allow schema evolution for Delta tables."""
        if _backend != BackendType.MOCK:
            pytest.skip(
                "Delta CTAS replacement is only validated in sparkless mock mode"
            )

        import uuid

        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"ctas_schema_{table_suffix}"
        table_name = f"{schema_name}.clean_events"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        base_df = spark.createDataFrame(
            [("u1", "Alice", 100), ("u2", "Bob", 200)],
            ["user_id", "name", "value"],
        )
        base_df.write.format("delta").mode("overwrite").saveAsTable(table_name)

        spark.sql(
            f"""
            CREATE OR REPLACE TABLE {table_name}
            USING delta AS
            SELECT user_id, name, value, '2025-01-01' AS processed_at
            FROM {table_name}
            """
        )

        result = spark.table(table_name)
        assert set(result.columns) == {"user_id", "name", "value", "processed_at"}
        rows = {r["user_id"]: r for r in result.collect()}
        assert rows["u1"]["processed_at"] == "2025-01-01"
        assert rows["u2"]["processed_at"] == "2025-01-01"
        assert result.count() == 2

        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass

    def test_immediate_table_access(self, spark):
        """Test that table is immediately accessible after saveAsTable."""
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.test_table"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        # Write table
        df.write.mode("overwrite").saveAsTable(table_name)

        # Should be immediately accessible (no delay, no retry needed)
        table = spark.table(table_name)
        assert table is not None
        assert table.count() == 1
        assert "id" in table.columns
        assert "name" in table.columns

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_basic_schema_evolution(self, spark):
        """Test basic schema evolution: add new columns."""
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        result = spark.table(table_name)
        assert set(result.columns) == {"id", "name", "age"}

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_overwrite_schema_option(self, spark):
        """Test that overwriteSchema option preserves existing columns."""
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create initial table
        df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Verify initial schema
        table1 = spark.table(table_name)
        assert set(table1.columns) == {"id", "name"}

        # Add new column with overwriteSchema
        df2 = spark.createDataFrame([(1, "Alice", 25)], ["id", "name", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        # Verify schema evolution: existing columns preserved, new column added
        table2 = spark.table(table_name)
        assert "id" in table2.columns
        assert "name" in table2.columns
        assert "age" in table2.columns
        assert len(table2.columns) == 3

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_preserve_existing_columns(self, spark):
        """Test that overwriteSchema completely overwrites the schema (PySpark behavior).

        Note: In PySpark, overwriteSchema=true means completely overwrite the schema,
        NOT merge/preserve existing columns. This test verifies that behavior.
        """
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.data"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial: [id, name, value]
        df1 = spark.createDataFrame([(1, "Alice", 100)], ["id", "name", "value"])
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Overwrite with: [id, age] (missing name and value)
        df2 = spark.createDataFrame([(1, 25)], ["id", "age"])
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        result = spark.table(table_name)
        assert "id" in result.columns
        assert "age" in result.columns  # New column
        # PySpark behavior: overwriteSchema completely overwrites, doesn't preserve
        assert "name" not in result.columns  # Should NOT be preserved
        assert "value" not in result.columns  # Should NOT be preserved

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    def test_schema_merge_on_overwrite(self, spark):
        """Test that overwriteSchema completely overwrites the schema (PySpark behavior).

        Note: In PySpark, overwriteSchema=true means completely overwrite the schema,
        NOT merge/preserve existing columns. This test verifies that behavior.
        """
        import uuid

        # Use unique table name to avoid conflicts when running with PySpark
        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.users"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table with columns: [id, name, value]
        df1 = spark.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)], ["id", "name", "value"]
        )
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Overwrite with new columns but missing some existing
        # New DataFrame has: [id, age, city] (missing name and value)
        df2 = spark.createDataFrame(
            [(1, 25, "NYC"), (2, 30, "LA")], ["id", "age", "city"]
        )

        # With overwriteSchema=true, PySpark completely overwrites the schema
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        # Verify: PySpark behavior is to completely overwrite, not merge
        result = spark.table(table_name)
        assert "id" in result.columns
        assert "age" in result.columns  # New column
        assert "city" in result.columns  # New column
        # PySpark behavior: overwriteSchema completely overwrites, doesn't preserve
        assert "name" not in result.columns  # Should NOT be preserved
        assert "value" not in result.columns  # Should NOT be preserved

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass  # Ignore cleanup errors

    @pytest.mark.delta
    @pytest.mark.xdist_group(name="delta_serial")
    def test_merge_schema_append(self, spark):
        """Test mergeSchema with append mode for all table types."""
        import uuid

        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.people_{table_suffix}"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table (PySpark requires Delta format for mergeSchema)
        # Drop table first if it exists (Delta doesn't support truncate in batch mode)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        df1 = spark.createDataFrame([("Alice", 25)], ["name", "age"])
        df1.write.format("delta").saveAsTable(table_name)

        # Append with new column using mergeSchema
        df2 = spark.createDataFrame([("Bob", 30, "NYC")], ["name", "age", "city"])
        df2.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(table_name)

        # Verify merged schema
        result = spark.table(table_name)
        assert set(result.columns) == {"name", "age", "city"}
        assert result.count() == 2

        # Verify data
        rows = result.collect()
        alice = next(r for r in rows if r["name"] == "Alice")
        bob = next(r for r in rows if r["name"] == "Bob")
        assert alice["city"] is None
        assert bob["city"] == "NYC"

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass

    @pytest.mark.delta
    @pytest.mark.xdist_group(name="delta_serial")
    def test_merge_schema_bidirectional(self, spark):
        """Test mergeSchema handles bidirectional merging."""
        import uuid

        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.employees_{table_suffix}"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial table with columns: name, age, salary (PySpark requires Delta format for mergeSchema)
        # Drop table first if it exists (Delta doesn't support truncate in batch mode)
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        df1 = spark.createDataFrame([("Alice", 25, 50000.0)], ["name", "age", "salary"])
        df1.write.format("delta").saveAsTable(table_name)

        # New DataFrame with columns: name, job
        df2 = spark.createDataFrame([("Bob", "engineer")], ["name", "job"])

        # Append with mergeSchema - should merge both ways
        df2.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(table_name)

        # Verify merged schema has all columns
        result = spark.table(table_name)
        assert set(result.columns) == {"name", "age", "salary", "job"}
        assert result.count() == 2

        # Verify data
        rows = result.collect()
        alice = next(r for r in rows if r["name"] == "Alice")
        bob = next(r for r in rows if r["name"] == "Bob")
        assert alice["job"] is None
        assert bob["age"] is None
        assert bob["salary"] is None
        assert bob["job"] == "engineer"

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass

    def test_complete_schema_evolution_scenario(self, spark):
        """Test a complete schema evolution scenario like SparkForge uses."""
        import uuid

        table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
        schema_name = f"test_schema_{table_suffix}"
        table_name = f"{schema_name}.events"

        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Step 1: Create initial table
        df1 = spark.createDataFrame(
            [("user1", "Alice", 100)], ["user_id", "name", "value"]
        )
        df1.write.mode("overwrite").saveAsTable(table_name)

        # Step 2: Add new column using F.lit(None).cast()
        df2 = spark.createDataFrame(
            [("user2", "Bob", 200)], ["user_id", "name", "value"]
        )
        df2 = df2.withColumn("processed_at", F.lit(None).cast(TimestampType()))

        # Step 3: Merge with existing schema
        existing_table = spark.table(table_name)
        existing_schema = existing_table.schema

        # Add missing columns from existing schema
        for field in existing_schema.fields:
            if field.name not in df2.columns:
                df2 = df2.withColumn(field.name, F.lit(None).cast(field.dataType))

        # Step 4: Write with schema evolution
        df2.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            table_name
        )

        # Step 5: Verify result
        result = spark.table(table_name)
        assert "user_id" in result.columns
        assert "name" in result.columns
        assert "value" in result.columns
        assert "processed_at" in result.columns
        assert result.count() == 1

        # Cleanup
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass
