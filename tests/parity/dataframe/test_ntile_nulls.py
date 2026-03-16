"""
Tests for ntile() window function with NULL values.

Issue #1484: ntile() was handling NULL values differently than PySpark:
1. NULL ordering: PySpark puts NULLs first in ascending, last in descending
2. ntile for NULL rows: should return integer, not NULL

These tests work with both sparkless and PySpark backends.
Set SPARKLESS_TEST_MODE=pyspark to run with real PySpark.
"""

from sparkless.testing import get_imports

imports = get_imports()
F = imports.F
Window = imports.Window


class TestNtileNulls:
    """Test ntile() handles NULL values like PySpark."""

    def test_ntile_nulls_ascending(self, spark):
        """Test ntile() with NULL in ascending order - NULL should come first (#1484)."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 1},
                {"grp": "A", "val": None},
                {"grp": "A", "val": 2},
                {"grp": "A", "val": 3},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.orderBy("val").collect()

        # PySpark behavior: NULL comes first in ascending order
        # 4 rows, ntile(2): first 2 get bucket 1, last 2 get bucket 2
        assert len(rows) == 4

        # NULL row should be first and have ntile=1 (not NULL!)
        null_row = next((r for r in rows if r["val"] is None), None)
        assert null_row is not None
        assert null_row["ntile"] == 1, (
            f"NULL row should have ntile=1, got {null_row['ntile']}"
        )

        # Row with val=1 should have ntile=1 (second row in ascending)
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1["ntile"] == 1

        # Row with val=2 should have ntile=2
        row_2 = next((r for r in rows if r["val"] == 2), None)
        assert row_2["ntile"] == 2

        # Row with val=3 should have ntile=2
        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3["ntile"] == 2

    def test_ntile_nulls_descending(self, spark):
        """Test ntile() with NULL in descending order - NULL should come last (#1484)."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 1},
                {"grp": "A", "val": None},
                {"grp": "A", "val": 2},
                {"grp": "A", "val": 3},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy(F.col("val").desc())
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.collect()

        # PySpark behavior: NULL comes last in descending order
        # Order: 3, 2, 1, NULL -> ntile: 1, 1, 2, 2
        assert len(rows) == 4

        # Row with val=3 should be first, ntile=1
        row_3 = next((r for r in rows if r["val"] == 3), None)
        assert row_3["ntile"] == 1

        # Row with val=2 should have ntile=1
        row_2 = next((r for r in rows if r["val"] == 2), None)
        assert row_2["ntile"] == 1

        # Row with val=1 should have ntile=2
        row_1 = next((r for r in rows if r["val"] == 1), None)
        assert row_1["ntile"] == 2

        # NULL row should be last and have ntile=2 (not NULL!)
        null_row = next((r for r in rows if r["val"] is None), None)
        assert null_row is not None
        assert null_row["ntile"] == 2, (
            f"NULL row should have ntile=2, got {null_row['ntile']}"
        )

    def test_ntile_all_nulls(self, spark):
        """Test ntile() when all ORDER BY values are NULL."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": None},
                {"grp": "A", "val": None},
                {"grp": "A", "val": None},
                {"grp": "A", "val": None},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.collect()

        assert len(rows) == 4
        # All rows should have integer ntile values (1 or 2), not NULL
        ntile_values = [r["ntile"] for r in rows]
        assert all(v is not None for v in ntile_values), "ntile should never be NULL"
        assert set(ntile_values) == {1, 2}, (
            f"Expected ntile values 1 and 2, got {set(ntile_values)}"
        )

    def test_ntile_no_nulls(self, spark):
        """Test ntile() without NULL values still works correctly."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 4},
            ]
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.orderBy("val").collect()

        assert len(rows) == 4
        assert rows[0]["val"] == 1 and rows[0]["ntile"] == 1
        assert rows[1]["val"] == 2 and rows[1]["ntile"] == 1
        assert rows[2]["val"] == 3 and rows[2]["ntile"] == 2
        assert rows[3]["val"] == 4 and rows[3]["ntile"] == 2

    def test_ntile_multiple_groups_with_nulls(self, spark):
        """Test ntile() with NULL values across multiple groups."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": 1},
                {"grp": "A", "val": None},
                {"grp": "A", "val": 2},
                {"grp": "B", "val": None},
                {"grp": "B", "val": 10},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.collect()

        # Group A: 3 rows, ntile(2) -> [1, 1, 2] for [NULL, 1, 2]
        grp_a = [r for r in rows if r["grp"] == "A"]
        assert len(grp_a) == 3
        null_a = next((r for r in grp_a if r["val"] is None), None)
        assert null_a["ntile"] == 1

        # Group B: 2 rows, ntile(2) -> [1, 2] for [NULL, 10]
        grp_b = [r for r in rows if r["grp"] == "B"]
        assert len(grp_b) == 2
        null_b = next((r for r in grp_b if r["val"] is None), None)
        assert null_b["ntile"] == 1
        val_10 = next((r for r in grp_b if r["val"] == 10), None)
        assert val_10["ntile"] == 2

    def test_ntile_single_null_row(self, spark):
        """Test ntile() with a single NULL row."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": None},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(2).over(w))
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["ntile"] == 1, "Single row should have ntile=1"

    def test_ntile_ntile3_with_nulls(self, spark):
        """Test ntile(3) with NULL values."""
        df = spark.createDataFrame(
            [
                {"grp": "A", "val": None},
                {"grp": "A", "val": 1},
                {"grp": "A", "val": 2},
                {"grp": "A", "val": 3},
                {"grp": "A", "val": 4},
                {"grp": "A", "val": 5},
            ],
            schema="grp string, val int",
        )

        w = Window.partitionBy("grp").orderBy("val")
        result = df.withColumn("ntile", F.ntile(3).over(w))
        rows = result.orderBy("val").collect()

        # 6 rows, ntile(3): [1,1], [2,2], [3,3]
        # Order: NULL, 1, 2, 3, 4, 5
        assert len(rows) == 6

        # NULL comes first
        null_row = next((r for r in rows if r["val"] is None), None)
        assert null_row["ntile"] == 1

        # Check distribution
        ntile_values = [r["ntile"] for r in rows]
        assert ntile_values.count(1) == 2
        assert ntile_values.count(2) == 2
        assert ntile_values.count(3) == 2
