"""Test issue #448: dropDuplicates() with list/array column raises TypeError.

Sparkless previously raised:
  TypeError: unhashable type: 'list'
when calling dropDuplicates() on a DataFrame containing list-typed columns.
The fix uses _make_hashable to convert unhashable values before set membership.

Tests are written PySpark-first: run with PySpark, then mock:
  SPARKLESS_TEST_MODE=pyspark pytest tests/test_issue_448_drop_duplicates_list_column.py -v
  pytest tests/test_issue_448_drop_duplicates_list_column.py -v

https://github.com/eddiethedean/sparkless/issues/448
"""

from sparkless.testing import get_imports


def _row_val(row, key):
    """Get value from Row or dict (PySpark returns Row, mock may return dict)."""
    if hasattr(row, "__getitem__"):
        return row[key]
    return getattr(row, key, None)


# --- Exact issue scenario and core list-column tests ---


def test_drop_duplicates_with_list_column_exact_issue_448(spark, spark_mode):
    """Exact scenario from #448 - dropDuplicates with list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Bob", "Value": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    names = {_row_val(r, "Name") for r in rows}
    assert names == {"Alice", "Bob"}
    for r in rows:
        assert list(_row_val(r, "Value")) == [1, 2, 3]


def test_distinct_with_list_column(spark, spark_mode):
    """distinct() with list column - same fix path."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [10, 20]},
            {"id": 1, "arr": [10, 20]},
            {"id": 2, "arr": [10, 20]},
        ]
    )
    result = df.distinct()
    rows = result.collect()

    assert len(rows) == 2
    ids = {_row_val(r, "id") for r in rows}
    assert ids == {1, 2}


def test_drop_duplicates_subset_excludes_list_column(spark, spark_mode):
    """dropDuplicates(subset) when subset excludes the list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [4, 5, 6]},
            {"Name": "Bob", "Value": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates(subset=["Name"])
    rows = result.collect()

    assert len(rows) == 2
    names = {_row_val(r, "Name") for r in rows}
    assert names == {"Alice", "Bob"}


def test_drop_duplicates_subset_includes_list_column(spark, spark_mode):
    """dropDuplicates(subset) when subset includes the list column."""
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [1, 2, 3]},
            {"Name": "Alice", "Value": [4, 5, 6]},
        ]
    )
    result = df.dropDuplicates(subset=["Name", "Value"])
    rows = result.collect()

    assert len(rows) == 2
    alice_rows = [r for r in rows if _row_val(r, "Name") == "Alice"]
    assert len(alice_rows) == 2
    values = [list(_row_val(r, "Value")) for r in alice_rows]
    assert [1, 2, 3] in values
    assert [4, 5, 6] in values


def test_drop_duplicates_empty_list_column_explicit_schema(spark, spark_mode):
    """dropDuplicates with empty list values - explicit schema (PySpark can't infer [])."""
    imports = get_imports(spark_mode)
    StructType = imports.StructType
    StructField = imports.StructField
    StringType = imports.StringType
    ArrayType = imports.ArrayType
    IntegerType = imports.IntegerType

    schema = StructType(
        [
            StructField("Name", StringType()),
            StructField("Value", ArrayType(IntegerType())),
        ]
    )
    df = spark.createDataFrame(
        [
            {"Name": "Alice", "Value": []},
            {"Name": "Alice", "Value": []},
            {"Name": "Bob", "Value": []},
        ],
        schema=schema,
    )
    result = df.dropDuplicates()
    rows = result.collect()

    assert len(rows) == 2
    names = {_row_val(r, "Name") for r in rows}
    assert names == {"Alice", "Bob"}
    for r in rows:
        assert list(_row_val(r, "Value")) == []


# --- Additional robust tests (PySpark-compatible) ---


def test_drop_duplicates_alias_lowercase(spark, spark_mode):
    """drop_duplicates() alias works same as dropDuplicates()."""
    df = spark.createDataFrame(
        [
            {"k": 1, "v": [1, 2]},
            {"k": 1, "v": [1, 2]},
            {"k": 2, "v": [1, 2]},
        ]
    )
    result = df.drop_duplicates()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "k") for r in rows} == {1, 2}


def test_drop_duplicates_string_arrays(spark, spark_mode):
    """dropDuplicates with string array column."""
    df = spark.createDataFrame(
        [
            {"name": "A", "tags": ["x", "y"]},
            {"name": "A", "tags": ["x", "y"]},
            {"name": "B", "tags": ["x", "y"]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "name") for r in rows} == {"A", "B"}
    for r in rows:
        assert list(_row_val(r, "tags")) == ["x", "y"]


def test_drop_duplicates_multiple_array_columns(spark, spark_mode):
    """dropDuplicates with multiple array columns."""
    df = spark.createDataFrame(
        [
            {"id": 1, "a": [1, 2], "b": ["x", "y"]},
            {"id": 1, "a": [1, 2], "b": ["x", "y"]},
            {"id": 2, "a": [1, 2], "b": ["x", "y"]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {1, 2}


def test_drop_duplicates_subset_single_column(spark, spark_mode):
    """dropDuplicates(subset=[single_col]) with array in other column."""
    df = spark.createDataFrame(
        [
            {"id": 1, "name": "Alice", "arr": [1, 2, 3]},
            {"id": 2, "name": "Alice", "arr": [4, 5, 6]},
            {"id": 3, "name": "Bob", "arr": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates(subset=["name"])
    rows = result.collect()
    assert len(rows) == 2
    names = {_row_val(r, "name") for r in rows}
    assert names == {"Alice", "Bob"}


def test_distinct_after_select_with_array(spark, spark_mode):
    """distinct after select preserves array column and deduplicates."""
    df = spark.createDataFrame(
        [
            {"id": 1, "vals": [10, 20]},
            {"id": 1, "vals": [10, 20]},
            {"id": 2, "vals": [10, 20]},
        ]
    )
    result = df.select("id", "vals").distinct()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {1, 2}


def test_drop_duplicates_all_unique_rows(spark, spark_mode):
    """dropDuplicates when no duplicates - returns same rows."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [1]},
            {"id": 2, "arr": [2]},
            {"id": 3, "arr": [3]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 3
    assert {_row_val(r, "id") for r in rows} == {1, 2, 3}


def test_drop_duplicates_all_duplicate_rows(spark, spark_mode):
    """dropDuplicates when all rows identical - returns one row."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [1, 2, 3]},
            {"id": 1, "arr": [1, 2, 3]},
            {"id": 1, "arr": [1, 2, 3]},
        ]
    )
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 1
    assert _row_val(rows[0], "id") == 1
    assert list(_row_val(rows[0], "arr")) == [1, 2, 3]


def test_drop_duplicates_with_nulls_in_array(spark, spark_mode):
    """dropDuplicates when array contains None - explicit schema for nullable element."""
    imports = get_imports(spark_mode)
    StructType = imports.StructType
    StructField = imports.StructField
    StringType = imports.StringType
    ArrayType = imports.ArrayType
    IntegerType = imports.IntegerType

    # PySpark: ArrayType(IntegerType(), True) = containsNull; Sparkless: nullable for array
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("arr", ArrayType(IntegerType())),
        ]
    )
    df = spark.createDataFrame(
        [
            {"name": "A", "arr": [1, None, 3]},
            {"name": "A", "arr": [1, None, 3]},
            {"name": "B", "arr": [1, None, 3]},
        ],
        schema=schema,
    )
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "name") for r in rows} == {"A", "B"}


def test_drop_duplicates_empty_dataframe(spark, spark_mode):
    """dropDuplicates on empty DataFrame with array schema."""
    imports = get_imports(spark_mode)
    StructType = imports.StructType
    StructField = imports.StructField
    StringType = imports.StringType
    ArrayType = imports.ArrayType
    IntegerType = imports.IntegerType

    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("arr", ArrayType(IntegerType())),
        ]
    )
    df = spark.createDataFrame([], schema=schema)
    result = df.dropDuplicates()
    rows = result.collect()
    assert len(rows) == 0


def test_distinct_then_filter(spark, spark_mode):
    """distinct then filter - order of operations."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [1, 2]},
            {"id": 1, "arr": [1, 2]},
            {"id": 2, "arr": [1, 2]},
            {"id": 3, "arr": [1, 2]},
        ]
    )
    F = get_imports(spark_mode).F
    result = df.distinct().filter(F.col("id") > 1)
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {2, 3}


def test_filter_then_drop_duplicates(spark, spark_mode):
    """filter then dropDuplicates."""
    df = spark.createDataFrame(
        [
            {"id": 1, "arr": [1]},
            {"id": 1, "arr": [1]},
            {"id": 2, "arr": [2]},
        ]
    )
    F = get_imports(spark_mode).F
    result = df.filter(F.col("id") >= 1).dropDuplicates()
    rows = result.collect()
    assert len(rows) == 2
    assert {_row_val(r, "id") for r in rows} == {1, 2}
