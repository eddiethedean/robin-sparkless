"""Tests for Delta Lake fixture support in sparkless.testing.

These tests verify that the @pytest.mark.delta marker and SPARKLESS_ENABLE_DELTA
environment variable correctly enable Delta Lake support in PySpark mode.
"""

import os
import shutil
import tempfile
import uuid

import pytest


@pytest.mark.pyspark_only
@pytest.mark.delta
class TestDeltaFixtureWithMarker:
    """Tests that use @pytest.mark.delta to enable Delta Lake."""

    def test_delta_write_and_read(self, spark, spark_imports):
        """Test basic Delta Lake write and read with the delta marker."""
        # Create a temporary directory for Delta table
        delta_path = tempfile.mkdtemp(prefix="delta_test_")

        try:
            # Create and write a Delta table
            df = spark.createDataFrame(
                [(1, "Alice", 100), (2, "Bob", 200)],
                ["id", "name", "value"],
            )
            df.write.format("delta").mode("overwrite").save(delta_path)

            # Read it back
            result = spark.read.format("delta").load(delta_path)

            assert result.count() == 2
            assert set(result.columns) == {"id", "name", "value"}

            # Verify data
            rows = {r["id"]: r for r in result.collect()}
            assert rows[1]["name"] == "Alice"
            assert rows[2]["value"] == 200

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)

    def test_delta_table_append(self, spark, spark_imports):
        """Test Delta Lake append mode with the delta marker."""
        delta_path = tempfile.mkdtemp(prefix="delta_append_")

        try:
            # Initial write
            df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
            df1.write.format("delta").mode("overwrite").save(delta_path)

            # Append more data
            df2 = spark.createDataFrame([(2, "Bob"), (3, "Charlie")], ["id", "name"])
            df2.write.format("delta").mode("append").save(delta_path)

            # Verify all data
            result = spark.read.format("delta").load(delta_path)
            assert result.count() == 3

            names = {r["name"] for r in result.collect()}
            assert names == {"Alice", "Bob", "Charlie"}

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)

    def test_delta_table_overwrite(self, spark, spark_imports):
        """Test Delta Lake overwrite mode with the delta marker."""
        delta_path = tempfile.mkdtemp(prefix="delta_overwrite_")

        try:
            # Initial write
            df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
            df1.write.format("delta").mode("overwrite").save(delta_path)

            # Overwrite with new data
            df2 = spark.createDataFrame([(3, "Charlie")], ["id", "name"])
            df2.write.format("delta").mode("overwrite").save(delta_path)

            # Verify only new data exists
            result = spark.read.format("delta").load(delta_path)
            assert result.count() == 1
            assert result.collect()[0]["name"] == "Charlie"

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)

    def test_delta_with_schema_evolution(self, spark, spark_imports):
        """Test Delta Lake schema evolution with mergeSchema."""
        delta_path = tempfile.mkdtemp(prefix="delta_schema_")

        try:
            # Initial write with schema [id, name]
            df1 = spark.createDataFrame([(1, "Alice")], ["id", "name"])
            df1.write.format("delta").mode("overwrite").save(delta_path)

            # Append with new column using mergeSchema
            df2 = spark.createDataFrame([(2, "Bob", 30)], ["id", "name", "age"])
            df2.write.format("delta").mode("append").option("mergeSchema", "true").save(
                delta_path
            )

            # Verify merged schema
            result = spark.read.format("delta").load(delta_path)
            assert set(result.columns) == {"id", "name", "age"}
            assert result.count() == 2

            # Alice should have null age
            rows = {r["id"]: r for r in result.collect()}
            assert rows[1]["age"] is None
            assert rows[2]["age"] == 30

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)

    def test_delta_partitioned_table(self, spark, spark_imports):
        """Test Delta Lake with partitioning."""
        delta_path = tempfile.mkdtemp(prefix="delta_partitioned_")

        try:
            # Create partitioned Delta table
            df = spark.createDataFrame(
                [
                    (1, "Alice", "US"),
                    (2, "Bob", "US"),
                    (3, "Charlie", "UK"),
                ],
                ["id", "name", "country"],
            )
            df.write.format("delta").partitionBy("country").mode("overwrite").save(
                delta_path
            )

            # Read and verify
            result = spark.read.format("delta").load(delta_path)
            assert result.count() == 3

            # Filter by partition
            us_only = result.filter(result.country == "US")
            assert us_only.count() == 2

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)


@pytest.mark.pyspark_only
@pytest.mark.delta
def test_delta_saveAsTable(spark, spark_imports):
    """Test Delta Lake saveAsTable with the delta marker."""
    table_suffix = str(uuid.uuid4()).replace("-", "_")[:8]
    schema_name = f"delta_test_{table_suffix}"
    table_name = f"{schema_name}.test_table_{table_suffix}"

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Create and save as Delta table (use unique table name to avoid conflicts)
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob")],
            ["id", "name"],
        )
        # Use saveAsTable without overwrite for new table
        df.write.format("delta").saveAsTable(table_name)

        # Read from catalog
        result = spark.table(table_name)
        assert result.count() == 2

    finally:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            spark.sql(f"DROP SCHEMA IF EXISTS {schema_name}")
        except Exception:
            pass


@pytest.mark.pyspark_only
@pytest.mark.delta
def test_delta_sql_operations(spark, spark_imports):
    """Test Delta Lake SQL operations with the delta marker."""
    delta_path = tempfile.mkdtemp(prefix="delta_sql_")

    try:
        # Create Delta table via DataFrame API
        df = spark.createDataFrame(
            [(1, "Alice", 100), (2, "Bob", 200)],
            ["id", "name", "value"],
        )
        df.write.format("delta").mode("overwrite").save(delta_path)

        # Register as temp view for SQL
        spark.read.format("delta").load(delta_path).createOrReplaceTempView(
            "delta_test_view"
        )

        # Query via SQL
        result = spark.sql("SELECT * FROM delta_test_view WHERE value > 150")
        assert result.count() == 1
        assert result.collect()[0]["name"] == "Bob"

    finally:
        shutil.rmtree(delta_path, ignore_errors=True)


@pytest.mark.pyspark_only
@pytest.mark.delta
def test_isolated_session_with_delta(isolated_session, spark_imports):
    """Test that isolated_session also supports Delta Lake with the marker."""
    delta_path = tempfile.mkdtemp(prefix="delta_isolated_")

    try:
        # Use the isolated_session directly
        df = isolated_session.createDataFrame([(1, "test")], ["id", "name"])
        df.write.format("delta").mode("overwrite").save(delta_path)

        result = isolated_session.read.format("delta").load(delta_path)
        assert result.count() == 1

    finally:
        shutil.rmtree(delta_path, ignore_errors=True)


@pytest.mark.pyspark_only
class TestDeltaFixtureVerification:
    """Tests that verify Delta Lake fixture behavior."""

    def test_without_delta_marker_cannot_use_delta(self, spark):
        """Verify that without @pytest.mark.delta, Delta operations may fail.

        This test documents the expected behavior: without the delta marker,
        PySpark may not have Delta Lake configured. This test is skipped if
        Delta happens to be available anyway (e.g., via env var or cached jars
        from other tests in the same JVM session).
        """
        # Skip if Delta is globally enabled
        if os.environ.get("SPARKLESS_ENABLE_DELTA", "0").strip().lower() in (
            "1",
            "true",
            "yes",
        ):
            pytest.skip("SPARKLESS_ENABLE_DELTA is set globally")

        delta_path = tempfile.mkdtemp(prefix="delta_no_marker_")

        try:
            df = spark.createDataFrame([(1, "test")], ["id", "name"])

            # Without Delta configured, this should fail
            # However, if other delta tests ran first in this JVM, jars are cached
            try:
                df.write.format("delta").mode("overwrite").save(delta_path)
                # If we get here, Delta is available (likely from cached jars)
                pytest.skip("Delta jars are cached from previous tests in this JVM")
            except Exception as e:
                # The error should mention Delta or format not found
                error_msg = str(e).lower()
                assert any(
                    keyword in error_msg
                    for keyword in [
                        "delta",
                        "format",
                        "datasource",
                        "not found",
                        "failed",
                    ]
                ), f"Unexpected error: {e}"

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)

    @pytest.mark.delta
    def test_with_delta_marker_can_use_delta(self, spark):
        """Verify that with @pytest.mark.delta, Delta operations work."""
        delta_path = tempfile.mkdtemp(prefix="delta_with_marker_")

        try:
            df = spark.createDataFrame([(1, "test")], ["id", "name"])

            # With Delta configured via marker, this should work
            df.write.format("delta").mode("overwrite").save(delta_path)

            result = spark.read.format("delta").load(delta_path)
            assert result.count() == 1

        finally:
            shutil.rmtree(delta_path, ignore_errors=True)
