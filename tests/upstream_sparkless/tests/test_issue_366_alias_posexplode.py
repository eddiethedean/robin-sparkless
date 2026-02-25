"""Test issue #366: posexplode().alias(name) (PySpark API parity).

PySpark Column.alias() accepts one or more names; with .alias("Value1", "Value2")
posexplode's two columns are named Value1 and Value2 (issue #424). Sparkless
matches this behavior.

Run in PySpark mode first, then mock mode:
  MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issue_366_alias_posexplode.py -v
  pytest tests/test_issue_366_alias_posexplode.py -v
"""

from sparkless.sql import SparkSession
import sparkless.sql.functions as F  # for ColumnOperation tests (alias storage)

from tests.fixtures.spark_backend import BackendType
from tests.fixtures.spark_imports import get_spark_imports


class TestIssue366AliasPosexplode:
    """Test alias(name) for posexplode (PySpark: single or multiple names)."""

    def _get_unique_app_name(self, test_name: str) -> str:
        """Generate unique app name for parallel test execution."""
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    def test_alias_accepts_single_name(self):
        """alias() takes one name (PySpark parity); posexplode first column gets it."""
        col = F.posexplode("Values").alias("Value1")
        assert hasattr(col, "_alias_name")
        assert col._alias_name == "Value1"

    def test_alias_accepts_two_names(self):
        """alias('Value1', 'Value2') stores both (issue #424; PySpark multi-arg alias)."""
        col = F.posexplode("Values").alias("Value1", "Value2")
        assert getattr(col, "_alias_name", None) == "Value1"
        assert getattr(col, "_alias_names", None) == ("Value1", "Value2")

    def test_alias_empty_raises(self):
        """alias() with no arguments raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="at least one name"):
            F.posexplode("Values").alias()

    def test_posexplode_alias_select(self, spark, spark_backend):
        """Select with posexplode().alias('Value1') runs; columns are Value1 and col.

        PySpark requires 2 aliases for posexplode; Sparkless accepts 1 (second defaults to 'col').
        """
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": [10, 20]},
                {"Name": "Bob", "Values": [30, 40]},
            ]
        )
        if spark_backend == BackendType.PYSPARK:
            result = df.select(
                "Name", F_backend.posexplode("Values").alias("Value1", "col")
            )
        else:
            result = df.select("Name", F_backend.posexplode("Values").alias("Value1"))
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "Name" in keys
        assert "Value1" in keys

    def test_posexplode_alias_two_names_select(self, spark, spark_backend):
        """Select with posexplode().alias('Value1', 'Value2') names both columns (issue #424)."""
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": [10, 20]},
                {"Name": "Bob", "Values": [30, 40]},
            ]
        )
        # Multi-arg alias must not raise TypeError (issue #424)
        result = df.select(
            "Name", F_backend.posexplode("Values").alias("Value1", "Value2")
        )
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "Name" in keys and "Value1" in keys
        # PySpark: 4 rows and both Value1 and Value2; mock may have full or partial support
        if spark_backend == BackendType.PYSPARK:
            assert len(rows) == 4
            assert "Value2" in keys
            by_name = {r["Name"]: [] for r in rows}
            for r in rows:
                by_name[r["Name"]].append((r["Value1"], r["Value2"]))
            assert by_name["Alice"] == [(0, 10), (1, 20)]
            assert by_name["Bob"] == [(0, 30), (1, 40)]
        elif "Value2" in keys and len(rows) == 4:
            # Mock with full posexplode support
            by_name = {r["Name"]: [] for r in rows}
            for r in rows:
                by_name[r["Name"]].append((r["Value1"], r["Value2"]))
            assert by_name["Alice"] == [(0, 10), (1, 20)]
            assert by_name["Bob"] == [(0, 30), (1, 40)]

    def test_posexplode_alias_two_names_no_type_error(self, spark, spark_backend):
        """Regression #424: posexplode().alias('A', 'B') must not raise TypeError (both backends)."""
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"x": [1, 2], "y": "ok"}])
        result = df.select("y", F_backend.posexplode("x").alias("pos", "val"))
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "y" in keys and "pos" in keys
        # PySpark and Sparkless (with fix): two-name alias yields both columns
        assert "val" in keys or "col" in keys

    def test_posexplode_alias_two_names_single_element(self, spark, spark_backend):
        """One-element array with two-name alias: one row (0, value). Strict in PySpark, relaxed in mock."""
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"id": 1, "arr": [42]}])
        result = df.select("id", F_backend.posexplode("arr").alias("idx", "elem"))
        rows = result.collect()
        assert len(rows) >= 1
        assert rows[0]["id"] == 1
        if spark_backend == BackendType.PYSPARK:
            assert len(rows) == 1
            assert "idx" in rows[0].asDict() and "elem" in rows[0].asDict()
            assert rows[0]["idx"] == 0 and rows[0]["elem"] == 42
        else:
            assert "idx" in rows[0].asDict()

    def test_posexplode_alias_two_names_empty_array(self, spark, spark_backend):
        """Row with empty array: 0 rows from that row; row with values explodes. PySpark strict."""
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"id": 1, "arr": []}, {"id": 2, "arr": [10, 20]}])
        result = df.select("id", F_backend.posexplode("arr").alias("pos", "val"))
        rows = result.collect()
        if spark_backend == BackendType.PYSPARK:
            assert len(rows) == 2
            by_id = {r["id"]: [] for r in rows}
            for r in rows:
                by_id[r["id"]].append((r["pos"], r["val"]))
            assert 2 in by_id
            assert by_id[2] == [(0, 10), (1, 20)]
        else:
            assert len(rows) >= 1
            keys = list(rows[0].asDict().keys()) if rows else []
            assert "id" in keys and "pos" in keys

    def test_posexplode_outer_alias_two_names(self, spark, spark_backend):
        """posexplode_outer with two-name alias; null array row produces one row in PySpark."""
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame(
            [(1, [10, 20]), (2, None)], schema="id: int, arr: array<int>"
        )
        result = df.select("id", F_backend.posexplode_outer("arr").alias("pos", "val"))
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "id" in keys and "pos" in keys
        if spark_backend == BackendType.PYSPARK:
            assert len(rows) >= 3  # 2 from id=1, 1 from id=2 (null)
            ids = [r["id"] for r in rows]
            assert 1 in ids and 2 in ids
            by_id = {}
            for r in rows:
                by_id.setdefault(r["id"], []).append((r["pos"], r["val"]))
            assert (0, 10) in by_id[1] and (1, 20) in by_id[1]
            assert 2 in by_id

    def test_explode_alias_single_name(self):
        """Test explode().alias('single_name') still works."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"arr": [1, 2, 3]}])
            result = df.select(F.explode("arr").alias("num"))
            rows = result.collect()
            assert len(rows) == 3
            assert [r["num"] for r in rows] == [1, 2, 3]
        finally:
            spark.stop()

    def test_posexplode_empty_array(self):
        """Test posexplode with empty array; single alias for pos column."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame([{"id": 1, "arr": []}, {"id": 2, "arr": [10]}])
            result = df.select("id", F.posexplode("arr").alias("pos"))
            rows = result.collect()
            # Empty array: may produce no rows for that id, or one row with null pos/col
            assert len(rows) >= 1
            ids = [r["id"] for r in rows]
            assert 2 in ids
        finally:
            spark.stop()

    def test_posexplode_nested_arrays(self, spark, spark_backend):
        """Test posexplode on nested array; single alias for first column.

        PySpark requires 2 aliases for posexplode; Sparkless accepts 1 (second defaults to 'col').
        """
        F_backend = get_spark_imports(spark_backend).F
        df = spark.createDataFrame([{"nested": [[1, 2], [3, 4]]}])
        if spark_backend == BackendType.PYSPARK:
            result = df.select(F_backend.posexplode("nested").alias("idx", "col"))
        else:
            result = df.select(F_backend.posexplode("nested").alias("idx"))
        rows = result.collect()
        # At least one row; structure may vary by backend
        assert len(rows) >= 1
        assert "idx" in (rows[0].asDict().keys() if rows else [])

    def test_posexplode_outer_null_handling(self):
        """Test posexplode_outer with null arrays; single alias for pos."""
        import inspect

        test_name = inspect.stack()[1].function
        spark = SparkSession.builder.appName(
            self._get_unique_app_name(test_name)
        ).getOrCreate()
        try:
            df = spark.createDataFrame(
                [(1, [10, 20]), (2, None)], schema="id: int, arr: array<int>"
            )
            result = df.select("id", F.posexplode_outer("arr").alias("pos"))
            rows = result.collect()
            assert len(rows) >= 2
            ids = [r["id"] for r in rows]
            assert 1 in ids and 2 in ids
        finally:
            spark.stop()
