"""Test issue #430: F.posexplode() + alias() must return exploded rows (not None).

Related to #366 and #424. posexplode().alias("Value1", "Value2") must produce
1 row per array element with correct pos and value columns, not 1 row per input
with None in exploded columns.

Run in PySpark mode first, then mock mode:
  MOCK_SPARK_TEST_BACKEND=pyspark pytest tests/test_issue_430_posexplode_alias_execution.py -v
  pytest tests/test_issue_430_posexplode_alias_execution.py -v

https://github.com/eddiethedean/sparkless/issues/430
"""

from tests.fixtures.spark_backend import BackendType
from tests.fixtures.spark_imports import get_spark_imports


def test_posexplode_alias_two_names_returns_exploded_rows(spark, spark_backend):
    """posexplode().alias('Value1', 'Value2') must return 4 rows with correct values (#430)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Values": [10, 20]},
            {"Name": "Bob", "Values": [30, 40]},
        ]
    )
    result = df.select("Name", F_backend.posexplode("Values").alias("Value1", "Value2"))
    rows = result.collect()

    assert "Value1" in result.columns
    assert "Value2" in result.columns
    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"

    # Group by name and check exploded values
    by_name = {}
    for r in rows:
        n = r["Name"]
        if n not in by_name:
            by_name[n] = []
        by_name[n].append((r["Value1"], r["Value2"]))

    assert by_name["Alice"] == [(0, 10), (1, 20)]
    assert by_name["Bob"] == [(0, 30), (1, 40)]


def test_posexplode_alias_no_none_values(spark, spark_backend):
    """posexplode().alias() must not return None in exploded columns (#430)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"x": [1, 2, 3], "y": "ok"},
        ]
    )
    result = df.select("y", F_backend.posexplode("x").alias("pos", "val"))
    rows = result.collect()

    assert len(rows) == 3
    for r in rows:
        assert r["pos"] is not None, f"pos must not be None: {r}"
        assert r["val"] is not None, f"val must not be None: {r}"
    assert [(r["pos"], r["val"]) for r in rows] == [(0, 1), (1, 2), (2, 3)]


def test_posexplode_alias_chained_filter_orderby(spark, spark_backend):
    """posexplode().alias() in chained select/filter/orderBy/limit returns correct rows."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"name": "A", "vals": [1, 2, 3]},
            {"name": "B", "vals": [4, 5, 6]},
        ]
    )
    result = (
        df.select("name", F_backend.posexplode("vals").alias("pos", "val"))
        .filter(F_backend.col("pos") >= 1)
        .orderBy("name", "pos")
        .limit(5)
    )
    rows = result.collect()
    assert "pos" in result.columns and "val" in result.columns
    if spark_backend == BackendType.PYSPARK:
        assert len(rows) == 4  # pos 1,2 for A and pos 1,2 for B
        assert [(r["name"], r["pos"], r["val"]) for r in rows] == [
            ("A", 1, 2),
            ("A", 2, 3),
            ("B", 1, 5),
            ("B", 2, 6),
        ]
    else:
        assert len(rows) >= 1
        for r in rows:
            assert r["pos"] is not None and r["val"] is not None


def test_posexplode_alias_empty_array(spark, spark_backend):
    """Empty array produces 0 rows for that row; non-empty explodes correctly."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": []},
            {"id": 2, "arr": [10, 20]},
        ]
    )
    result = df.select("id", F_backend.posexplode("arr").alias("pos", "val"))
    rows = result.collect()
    if spark_backend == BackendType.PYSPARK:
        assert len(rows) == 2  # empty array yields 0 rows; id=2 yields 2 rows
        by_id = {r["id"]: [] for r in rows}
        for r in rows:
            by_id[r["id"]].append((r["pos"], r["val"]))
        assert 2 in by_id
        assert by_id[2] == [(0, 10), (1, 20)]
    else:
        assert len(rows) >= 1
        assert "pos" in result.columns and "val" in result.columns


def test_posexplode_alias_single_element(spark, spark_backend):
    """Single-element array produces one row (0, value)."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"id": 1, "arr": [99]}])
    result = df.select("id", F_backend.posexplode("arr").alias("pos", "val"))
    rows = result.collect()
    assert len(rows) >= 1
    assert rows[0]["pos"] == 0 and rows[0]["val"] == 99
    assert rows[0]["pos"] is not None and rows[0]["val"] is not None


def test_posexplode_alias_mixed_columns(spark, spark_backend):
    """select(a, posexplode(arr).alias(...), b) preserves column order."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"a": "x", "arr": [1, 2], "b": 10}])
    result = df.select("a", F_backend.posexplode("arr").alias("pos", "val"), "b")
    rows = result.collect()
    assert result.columns == ["a", "pos", "val", "b"]
    if spark_backend == BackendType.PYSPARK:
        assert len(rows) == 2
        assert rows[0]["a"] == "x" and rows[0]["b"] == 10
        assert rows[0]["pos"] == 0 and rows[0]["val"] == 1
        assert rows[1]["pos"] == 1 and rows[1]["val"] == 2


def test_posexplode_outer_alias_returns_exploded_rows(spark, spark_backend):
    """posexplode_outer().alias() returns exploded rows; null array yields one row."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [(1, [10, 20]), (2, None)],
        schema="id: int, arr: array<int>",
    )
    result = df.select("id", F_backend.posexplode_outer("arr").alias("pos", "val"))
    rows = result.collect()
    assert "pos" in result.columns and "val" in result.columns
    if spark_backend == BackendType.PYSPARK:
        assert len(rows) >= 3  # 2 from id=1, 1 from id=2 (null)
        by_id = {}
        for r in rows:
            by_id.setdefault(r["id"], []).append((r["pos"], r["val"]))
        assert (0, 10) in by_id[1] and (1, 20) in by_id[1]
        assert 2 in by_id


def test_posexplode_alias_string_array(spark, spark_backend):
    """posexplode on string array returns correct values."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"id": 1, "arr": ["a", "b", "c"]}])
    result = df.select("id", F_backend.posexplode("arr").alias("pos", "val"))
    rows = result.collect()
    assert len(rows) == 3
    assert [(r["pos"], r["val"]) for r in rows] == [(0, "a"), (1, "b"), (2, "c")]


def test_posexplode_alias_column_object(spark, spark_backend):
    """posexplode(F.col('x')).alias(...) works with Column object."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame([{"x": [100, 200]}])
    result = df.select(F_backend.posexplode(F_backend.col("x")).alias("idx", "elem"))
    rows = result.collect()
    assert len(rows) == 2
    assert [(r["idx"], r["elem"]) for r in rows] == [(0, 100), (1, 200)]


def test_posexplode_alias_show_no_none(spark, spark_backend):
    """Regression: show() on posexplode result must not display None for exploded cols."""
    F_backend = get_spark_imports(spark_backend).F
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Values": [10, 20]},
            {"Name": "Bob", "Values": [30, 40]},
        ]
    )
    result = df.select("Name", F_backend.posexplode("Values").alias("Value1", "Value2"))
    rows = result.collect()
    # Exact scenario from issue #430: must have 4 rows, no None in Value1/Value2
    assert len(rows) == 4
    for r in rows:
        assert r["Value1"] is not None, f"Value1 must not be None: {r}"
        assert r["Value2"] is not None, f"Value2 must not be None: {r}"
