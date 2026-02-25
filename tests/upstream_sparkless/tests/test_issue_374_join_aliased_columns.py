"""Test issue #374: Aliased column references in joins (e.g. sm.brand_id).

PySpark allows F.col("sm.brand_id") in join conditions when the left
DataFrame is aliased as "sm". Sparkless should resolve alias.column to
the actual column name for join resolution.
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F


class TestIssue374JoinAliasedColumns:
    """Test join with aliased column references."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_join_aliased_column_refs(self):
        """Test join with F.col('sm.brand_id') == F.col('b.code') (issue example)."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            mapping_df = spark.createDataFrame(
                [
                    {
                        "brand_id": "BRAND001",
                        "taxonomy_id": "TAX001",
                        "confidence": 0.95,
                    }
                ]
            )
            brands_df = spark.createDataFrame(
                [
                    {"id": "uuid-001", "code": "BRAND001"},
                ]
            )
            result = (
                mapping_df.alias("sm")
                .join(
                    brands_df.alias("b").select(
                        F.col("id").alias("brand_uuid"), F.col("code")
                    ),
                    F.col("sm.brand_id") == F.col("b.code"),
                    "inner",
                )
                .select(
                    F.col("brand_uuid"),
                    F.col("sm.taxonomy_id"),
                    F.col("sm.confidence"),
                )
            )
            rows = result.collect()
            assert len(rows) == 1
            assert rows[0]["brand_uuid"] == "uuid-001"
            # Select uses alias-prefixed names; result columns keep that (sm.taxonomy_id, sm.confidence)
            assert rows[0]["sm.taxonomy_id"] == "TAX001"
            assert rows[0]["sm.confidence"] == 0.95
        finally:
            spark.stop()

    def test_join_multiple_aliased_tables(self):
        """Test join with 3+ aliased tables."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            users = spark.createDataFrame(
                [(1, "Alice", 10), (2, "Bob", 20)], ["user_id", "name", "dept_id"]
            )
            departments = spark.createDataFrame(
                [(10, "Engineering", 100), (20, "Sales", 200)],
                ["dept_id", "dept_name", "location_id"],
            )
            locations = spark.createDataFrame(
                [(100, "NYC"), (200, "SF")], ["location_id", "city"]
            )

            result = (
                users.alias("u")
                .join(departments.alias("d"), F.col("u.dept_id") == F.col("d.dept_id"))
                .join(
                    locations.alias("l"),
                    F.col("d.location_id") == F.col("l.location_id"),
                )
                .select(F.col("u.name"), F.col("d.dept_name"), F.col("l.city"))
            )

            rows = result.collect()
            assert len(rows) == 2
            # Check Alice is in Engineering in NYC
            alice = [r for r in rows if "Alice" in str(r)][0]
            assert "Engineering" in str(alice)
            assert "NYC" in str(alice)
        finally:
            spark.stop()

    def test_join_aliased_column_without_prefix(self):
        """Test that unaliased columns still work after aliasing DataFrame."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
            df2 = spark.createDataFrame([(1, "x"), (2, "y")], ["id", "data"])

            # After join, columns might be id, val, data (no prefix)
            result = (
                df1.alias("t1")
                .join(df2.alias("t2"), F.col("t1.id") == F.col("t2.id"))
                .select("id", "val", "data")  # Use unprefixed names
            )

            rows = result.collect()
            assert len(rows) == 2
        finally:
            spark.stop()

    def test_join_aliased_self_join(self):
        """Test self-join with different aliases."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            employees = spark.createDataFrame(
                [(1, "Alice", None), (2, "Bob", 1), (3, "Carol", 1)],
                ["id", "name", "manager_id"],
            )

            # Self-join to get employee and manager names
            result = (
                employees.alias("e")
                .join(
                    employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left"
                )
                .select(
                    F.col("e.name").alias("employee"), F.col("m.name").alias("manager")
                )
            )

            rows = result.collect()
            assert len(rows) == 3
            # At least one person has Alice as manager (Bob or Carol)
            managed = [r for r in rows if r["manager"] == "Alice"]
            assert len(managed) >= 1
        finally:
            spark.stop()

    def test_join_complex_condition_with_aliases(self):
        """Test join with complex conditions using aliased columns."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            orders = spark.createDataFrame(
                [(1, 100, 50.0), (2, 101, 75.0), (3, 100, 25.0)],
                ["order_id", "customer_id", "amount"],
            )
            customers = spark.createDataFrame(
                [(100, "Alice", "premium"), (101, "Bob", "standard")],
                ["customer_id", "name", "tier"],
            )

            # Join with multiple conditions
            result = (
                orders.alias("o")
                .join(
                    customers.alias("c"),
                    (F.col("o.customer_id") == F.col("c.customer_id"))
                    & (F.col("o.amount") > 30),
                    "inner",
                )
                .select(F.col("o.order_id"), F.col("c.name"), F.col("o.amount"))
            )

            rows = result.collect()
            # Should have at least 2 rows (orders with amount > 30)
            assert len(rows) >= 2
            # Check that we have the expected structure
            assert "name" in rows[0].asDict() or "c.name" in rows[0].asDict()
        finally:
            spark.stop()
