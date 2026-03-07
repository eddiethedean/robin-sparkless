"""Test issue #366: posexplode().alias(name) (PySpark API parity).

PySpark posexplode().alias("Value1", "Value2") names both columns.
Uses get_spark_imports only; same logic for both backends.
"""

import pytest

from tests.fixtures.spark_imports import get_spark_imports

_imports = get_spark_imports()
SparkSession = _imports.SparkSession
F = _imports.F
class TestIssue366AliasPosexplode:
    """Test alias(name) for posexplode (PySpark: two names for two columns)."""

    def _get_unique_app_name(self, test_name: str) -> str:
        import os
        import threading

        thread_id = threading.current_thread().ident
        process_id = os.getpid()
        return f"{test_name}_{process_id}_{thread_id}"

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_posexplode_alias_two_names_select(self, spark):
        """Select with posexplode().alias('Value1', 'Value2') names both columns (PySpark API)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": [10, 20]},
                {"Name": "Bob", "Values": [30, 40]},
            ]
        )
        result = df.select("Name", F.posexplode("Values").alias("Value1", "Value2"))
        rows = result.collect()
        assert len(rows) == 4
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "Name" in keys and "Value1" in keys and "Value2" in keys
        by_name = {r["Name"]: [] for r in rows}
        for r in rows:
            by_name[r["Name"]].append((r["Value1"], r["Value2"]))
        assert by_name["Alice"] == [(0, 10), (1, 20)]
        assert by_name["Bob"] == [(0, 30), (1, 40)]

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_posexplode_alias_two_names_no_type_error(self, spark):
        """posexplode().alias('pos', 'val') must not raise TypeError (PySpark API)."""
        df = spark.createDataFrame([{"x": [1, 2], "y": "ok"}])
        result = df.select("y", F.posexplode("x").alias("pos", "val"))
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "y" in keys and "pos" in keys and "val" in keys

    def test_posexplode_alias_two_names_single_element(self, spark):
        """One-element array with two-name alias: one row (0, value)."""
        df = spark.createDataFrame([{"id": 1, "arr": [42]}])
        result = df.select("id", F.posexplode("arr").alias("idx", "elem"))
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert rows[0]["idx"] == 0 and rows[0]["elem"] == 42

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_posexplode_alias_two_names_empty_array(self, spark):
        """Row with empty array yields 0 rows; row with values explodes."""
        df = spark.createDataFrame([{"id": 1, "arr": []}, {"id": 2, "arr": [10, 20]}])
        result = df.select("id", F.posexplode("arr").alias("pos", "val"))
        rows = result.collect()
        assert len(rows) == 2
        by_id = {r["id"]: [] for r in rows}
        for r in rows:
            by_id[r["id"]].append((r["pos"], r["val"]))
        assert 2 in by_id
        assert by_id[2] == [(0, 10), (1, 20)]

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_posexplode_outer_alias_two_names(self, spark):
        """posexplode_outer with two-name alias; null array row produces one row."""
        df = spark.createDataFrame(
            [(1, [10, 20]), (2, None)], schema="id: int, arr: array<int>"
        )
        result = df.select("id", F.posexplode_outer("arr").alias("pos", "val"))
        rows = result.collect()
        assert len(rows) >= 3
        ids = [r["id"] for r in rows]
        assert 1 in ids and 2 in ids
        by_id = {}
        for r in rows:
            by_id.setdefault(r["id"], []).append((r["pos"], r["val"]))
        assert (0, 10) in by_id[1] and (1, 20) in by_id[1]
        assert 2 in by_id

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_posexplode_alias_select_two_names(self, spark):
        """Select with posexplode().alias('Value1', 'col') runs (PySpark requires 2 aliases)."""
        df = spark.createDataFrame(
            [
                {"Name": "Alice", "Values": [10, 20]},
                {"Name": "Bob", "Values": [30, 40]},
            ]
        )
        result = df.select("Name", F.posexplode("Values").alias("Value1", "col"))
        rows = result.collect()
        assert len(rows) >= 1
        keys = list(rows[0].asDict().keys()) if rows else []
        assert "Name" in keys and "Value1" in keys

    @pytest.mark.skip(reason="Issue #366: unskip when fixing posexplode alias")
    def test_alias_empty_raises(self, spark):
        """alias() with no arguments works (PySpark behavior: keeps default names)."""
        df = spark.createDataFrame([{"Values": [1, 2]}])
        result = df.select(F.posexplode("Values").alias())
        rows = result.collect()
        assert len(rows) == 2
        # Default column names are 'pos' and 'col' in PySpark.
        assert result.columns == ["pos", "col"]

    def test_explode_alias_single_name(self, spark):
        """explode().alias('num') works."""
        df = spark.createDataFrame([{"arr": [1, 2, 3]}])
        result = df.select(F.explode("arr").alias("num"))
        rows = result.collect()
        assert len(rows) == 3
        assert [r["num"] for r in rows] == [1, 2, 3]

    def test_posexplode_nested_arrays(self, spark):
        """posexplode on nested array with two aliases."""
        df = spark.createDataFrame([{"nested": [[1, 2], [3, 4]]}])
        result = df.select(F.posexplode("nested").alias("idx", "col"))
        rows = result.collect()
        assert len(rows) >= 1
        assert "idx" in (rows[0].asDict().keys() if rows else [])
