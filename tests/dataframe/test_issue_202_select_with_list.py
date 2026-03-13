import pytest
from sparkless.testing import get_imports

_imports = get_imports()
SparkSession = _imports.SparkSession
StructType = _imports.StructType
StructField = _imports.StructField
StringType = _imports.StringType
LongType = _imports.LongType


def _assert_schema_consistent(result_schema, expected_schema, allow_int_long_swap=True):
    """Assert result schema is consistent with expected (same field names, compatible types)."""
    assert len(result_schema.fields) == len(expected_schema.fields)
    for i, (r_f, e_f) in enumerate(zip(result_schema.fields, expected_schema.fields)):
        assert r_f.name == e_f.name, f"field {i}: name {r_f.name!r} != {e_f.name!r}"
        r_s = getattr(
            r_f.dataType, "simpleString", lambda: type(r_f.dataType).__name__
        )()
        e_s = getattr(
            e_f.dataType, "simpleString", lambda: type(e_f.dataType).__name__
        )()
        if allow_int_long_swap and r_s in ("int", "long") and e_s in ("int", "long"):
            continue
        assert r_s == e_s, f"field {i} {r_f.name}: type {r_s!r} != {e_s!r}"


class TestIssue202SelectWithList:
    """Test cases for issue #202: DataFrame.select() with list of column names."""

    def test_select_with_list_of_column_names(self, spark):
        """
        Test that select() correctly handles a list of column names,
        matching PySpark's behavior.
        """
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
                {"name": "Charlie", "dept": "IT", "salary": 70000},
            ]
        )

        columns_to_select = ["name", "dept"]

        result = df.select(columns_to_select)

        # Verify schema (allow IntegerType/LongType for inferred int columns)
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("dept", StringType(), True),
            ]
        )
        _assert_schema_consistent(result.schema, expected_schema)
        assert len(result.schema.fields) == 2

        # Verify data
        assert result.count() == 3
        rows = result.collect()
        assert len(rows) == 3
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].name == "Bob"
        assert rows[1].dept == "HR"
        assert rows[2].name == "Charlie"
        assert rows[2].dept == "IT"

    def test_select_with_tuple_of_column_names_raises(self, spark):
        """
        PySpark select() does not accept a tuple of column names; it raises.
        Use a list for multiple columns: select(["name", "salary"]).
        """
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
            ]
        )

        columns_to_select = ("name", "salary")

        with pytest.raises(Exception):
            df.select(columns_to_select).collect()

    def test_select_with_single_column_list(self, spark):
        """
        Test that select() handles a list with a single column name.
        """
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT"},
                {"name": "Bob", "dept": "HR"},
            ]
        )

        result = df.select(["name"])

        # Verify schema
        expected_schema = StructType([StructField("name", StringType(), True)])
        _assert_schema_consistent(result.schema, expected_schema)
        assert len(result.schema.fields) == 1

        # Verify data
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[1].name == "Bob"

    def test_select_with_multiple_args_still_works(self, spark):
        """
        Ensure that the existing behavior of select() with multiple arguments
        is not regressed.
        """
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT", "salary": 50000},
                {"name": "Bob", "dept": "HR", "salary": 60000},
            ]
        )

        # This should still work as before
        result = df.select("name", "dept")

        assert len(result.schema.fields) == 2
        assert result.count() == 2
        rows = result.collect()
        assert rows[0].name == "Alice"
        assert rows[0].dept == "IT"
        assert rows[1].name == "Bob"
        assert rows[1].dept == "HR"

    def test_select_star_with_list_does_not_unpack(self, spark):
        """
        Test that select(["*"]) is not unpacked (should select all columns).
        """
        df = spark.createDataFrame(
            [
                {"name": "Alice", "dept": "IT"},
                {"name": "Bob", "dept": "HR"},
            ]
        )

        # When list contains "*", treat it as selecting all columns (PySpark-compatible).
        result = df.select(["*"])

        assert len(result.schema.fields) == 2
        assert result.count() == 2
