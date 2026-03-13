"""
Tests for sparkless.testing module.

This file tests the public API of sparkless.testing to ensure it works correctly
for both internal use and external projects.
"""

from __future__ import annotations

import os
import pytest
from datetime import date, datetime
from unittest import mock


# =============================================================================
# Mode Tests
# =============================================================================


class TestMode:
    """Tests for sparkless.testing.mode module."""

    def test_mode_enum_values(self):
        """Mode enum has correct values."""
        from sparkless.testing import Mode

        assert Mode.SPARKLESS.value == "sparkless"
        assert Mode.PYSPARK.value == "pyspark"

    def test_get_mode_default_is_sparkless(self):
        """get_mode() returns SPARKLESS when no env var is set."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SPARKLESS_TEST_MODE", None)
            assert get_mode() == Mode.SPARKLESS

    def test_get_mode_sparkless_explicit(self):
        """get_mode() returns SPARKLESS when env var is 'sparkless'."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "sparkless"}):
            assert get_mode() == Mode.SPARKLESS

    def test_get_mode_pyspark(self):
        """get_mode() returns PYSPARK when env var is 'pyspark'."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "pyspark"}):
            assert get_mode() == Mode.PYSPARK

    def test_get_mode_case_insensitive(self):
        """get_mode() is case insensitive."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "PYSPARK"}):
            assert get_mode() == Mode.PYSPARK

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "PySpark"}):
            assert get_mode() == Mode.PYSPARK

    def test_get_mode_with_whitespace(self):
        """get_mode() strips whitespace from env var."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "  pyspark  "}):
            assert get_mode() == Mode.PYSPARK

    def test_get_mode_invalid_value_defaults_to_sparkless(self):
        """get_mode() returns SPARKLESS for invalid values."""
        from sparkless.testing import Mode, get_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "invalid"}):
            assert get_mode() == Mode.SPARKLESS

    def test_is_pyspark_mode(self):
        """is_pyspark_mode() returns True only in pyspark mode."""
        from sparkless.testing import is_pyspark_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "pyspark"}):
            assert is_pyspark_mode() is True

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "sparkless"}):
            assert is_pyspark_mode() is False

    def test_is_sparkless_mode(self):
        """is_sparkless_mode() returns True only in sparkless mode."""
        from sparkless.testing import is_sparkless_mode

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "sparkless"}):
            assert is_sparkless_mode() is True

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "pyspark"}):
            assert is_sparkless_mode() is False

    def test_set_mode(self):
        """set_mode() sets the environment variable."""
        from sparkless.testing import Mode, set_mode, get_mode

        original = os.environ.get("SPARKLESS_TEST_MODE")
        try:
            set_mode(Mode.PYSPARK)
            assert os.environ.get("SPARKLESS_TEST_MODE") == "pyspark"
            assert get_mode() == Mode.PYSPARK

            set_mode(Mode.SPARKLESS)
            assert os.environ.get("SPARKLESS_TEST_MODE") == "sparkless"
            assert get_mode() == Mode.SPARKLESS
        finally:
            if original is not None:
                os.environ["SPARKLESS_TEST_MODE"] = original
            else:
                os.environ.pop("SPARKLESS_TEST_MODE", None)

    def test_parse_mode_none(self):
        """_parse_mode returns SPARKLESS for None."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode(None) == Mode.SPARKLESS

    def test_parse_mode_sparkless(self):
        """_parse_mode returns SPARKLESS for 'sparkless'."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode("sparkless") == Mode.SPARKLESS

    def test_parse_mode_pyspark(self):
        """_parse_mode returns PYSPARK for 'pyspark'."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode("pyspark") == Mode.PYSPARK

    def test_parse_mode_case_insensitive(self):
        """_parse_mode is case insensitive."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode("PYSPARK") == Mode.PYSPARK
        assert _parse_mode("PySpArK") == Mode.PYSPARK

    def test_parse_mode_with_whitespace(self):
        """_parse_mode strips whitespace."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode("  pyspark  ") == Mode.PYSPARK

    def test_parse_mode_invalid(self):
        """_parse_mode returns SPARKLESS for invalid values."""
        from sparkless.testing.mode import _parse_mode, Mode

        assert _parse_mode("invalid") == Mode.SPARKLESS
        assert _parse_mode("") == Mode.SPARKLESS


# =============================================================================
# Imports Tests
# =============================================================================


class TestImports:
    """Tests for sparkless.testing.imports module."""

    def test_get_imports_sparkless(self):
        """get_imports() returns sparkless imports in sparkless mode."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)

        assert imports.mode == Mode.SPARKLESS
        assert imports.SparkSession is not None
        assert imports.F is not None
        assert imports.Window is not None
        assert imports.Row is not None

    def test_get_imports_all_types(self):
        """get_imports() provides all expected data types."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)

        # Check all types
        assert imports.StringType is not None
        assert imports.IntegerType is not None
        assert imports.LongType is not None
        assert imports.DoubleType is not None
        assert imports.FloatType is not None
        assert imports.BooleanType is not None
        assert imports.DateType is not None
        assert imports.TimestampType is not None
        assert imports.ArrayType is not None
        assert imports.MapType is not None
        assert imports.StructType is not None
        assert imports.StructField is not None
        assert imports.DecimalType is not None
        assert imports.BinaryType is not None

    def test_get_imports_has_functions_alias(self):
        """SparkImports has both F and functions attributes."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)
        assert imports.F is imports.functions

    def test_get_imports_has_dataframereader(self):
        """SparkImports has DataFrameReader."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)
        assert imports.DataFrameReader is not None

    def test_spark_imports_from_env(self):
        """get_imports() uses env var when mode not specified."""
        from sparkless.testing import Mode, get_imports

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "sparkless"}):
            imports = get_imports()
            assert imports.mode == Mode.SPARKLESS

    def test_spark_imports_has_native_for_sparkless(self):
        """SparkImports._native is set for sparkless mode."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)
        assert imports._native is not None


# =============================================================================
# Session Tests
# =============================================================================


class TestSession:
    """Tests for sparkless.testing.session module."""

    def test_create_session_sparkless(self):
        """create_session() creates a sparkless session."""
        from sparkless.testing import Mode, create_session

        session = create_session(app_name="test_sparkless", mode=Mode.SPARKLESS)
        try:
            assert session is not None
            df = session.createDataFrame([(1, "a")], ["id", "val"])
            assert df.count() == 1
        finally:
            session.stop()

    def test_create_session_from_env(self):
        """create_session() uses env var when mode not specified."""
        from sparkless.testing import create_session

        with mock.patch.dict(os.environ, {"SPARKLESS_TEST_MODE": "sparkless"}):
            session = create_session(app_name="test_env")
            try:
                assert session is not None
                df = session.createDataFrame([(1,)], ["id"])
                assert df.count() == 1
            finally:
                session.stop()

    def test_create_session_with_custom_app_name(self):
        """create_session() uses the provided app name."""
        from sparkless.testing import Mode, create_session

        session = create_session(app_name="my_custom_app", mode=Mode.SPARKLESS)
        try:
            assert session is not None
        finally:
            session.stop()


# =============================================================================
# Comparison Tests
# =============================================================================


class TestComparisonRowsEqual:
    """Tests for assert_rows_equal function."""

    def test_assert_rows_equal_matching(self):
        """assert_rows_equal passes for matching rows."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        expected = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_order_matters(self):
        """assert_rows_equal fails when order differs and order_matters=True."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 2, "val": "b"}, {"id": 1, "val": "a"}]
        expected = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected, order_matters=True)

    def test_assert_rows_equal_order_not_matters(self):
        """assert_rows_equal passes when order differs and order_matters=False."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 2, "val": "b"}, {"id": 1, "val": "a"}]
        expected = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        assert_rows_equal(actual, expected, order_matters=False)

    def test_assert_rows_equal_count_mismatch(self):
        """assert_rows_equal fails when row counts differ."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1}]
        expected = [{"id": 1}, {"id": 2}]

        with pytest.raises(AssertionError, match="Row count mismatch"):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_value_mismatch(self):
        """assert_rows_equal fails when values differ."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1, "val": "a"}]
        expected = [{"id": 1, "val": "b"}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_float_tolerance(self):
        """assert_rows_equal handles float tolerance."""
        from sparkless.testing import assert_rows_equal

        actual = [{"val": 1.0000001}]
        expected = [{"val": 1.0}]
        assert_rows_equal(actual, expected, tolerance=1e-6)

    def test_assert_rows_equal_float_outside_tolerance(self):
        """assert_rows_equal fails when floats differ beyond tolerance."""
        from sparkless.testing import assert_rows_equal

        actual = [{"val": 1.1}]
        expected = [{"val": 1.0}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected, tolerance=1e-6)

    def test_assert_rows_equal_nan_values(self):
        """assert_rows_equal handles NaN values."""
        from sparkless.testing import assert_rows_equal

        actual = [{"val": float("nan")}]
        expected = [{"val": float("nan")}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_none_values(self):
        """assert_rows_equal handles None values."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1, "val": None}]
        expected = [{"id": 1, "val": None}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_one_none_fails(self):
        """assert_rows_equal fails when only one value is None."""
        from sparkless.testing import assert_rows_equal

        actual = [{"val": None}]
        expected = [{"val": 1}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_missing_key(self):
        """assert_rows_equal fails when key is missing in actual."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1}]
        expected = [{"id": 1, "val": "a"}]

        with pytest.raises(AssertionError, match="missing key"):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_extra_key(self):
        """assert_rows_equal fails when extra key in actual."""
        from sparkless.testing import assert_rows_equal

        actual = [{"id": 1, "val": "a", "extra": "x"}]
        expected = [{"id": 1, "val": "a"}]

        with pytest.raises(AssertionError, match="extra key"):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_nested_list(self):
        """assert_rows_equal handles nested lists."""
        from sparkless.testing import assert_rows_equal

        actual = [{"arr": [1, 2, 3]}]
        expected = [{"arr": [1, 2, 3]}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_nested_list_different_length(self):
        """assert_rows_equal fails for lists of different lengths."""
        from sparkless.testing import assert_rows_equal

        actual = [{"arr": [1, 2]}]
        expected = [{"arr": [1, 2, 3]}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected)

    def test_assert_rows_equal_nested_dict(self):
        """assert_rows_equal handles nested dicts."""
        from sparkless.testing import assert_rows_equal

        actual = [{"nested": {"a": 1, "b": 2}}]
        expected = [{"nested": {"a": 1, "b": 2}}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_nested_dict_different_keys(self):
        """assert_rows_equal fails for dicts with different keys."""
        from sparkless.testing import assert_rows_equal

        actual = [{"nested": {"a": 1}}]
        expected = [{"nested": {"a": 1, "b": 2}}]

        with pytest.raises(AssertionError):
            assert_rows_equal(actual, expected)


class TestComparisonDateHandling:
    """Tests for date/datetime comparison handling."""

    def test_assert_rows_equal_date_objects(self):
        """assert_rows_equal handles date objects."""
        from sparkless.testing import assert_rows_equal

        actual = [{"dt": date(2024, 1, 15)}]
        expected = [{"dt": date(2024, 1, 15)}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_datetime_objects(self):
        """assert_rows_equal handles datetime objects."""
        from sparkless.testing import assert_rows_equal

        actual = [{"dt": datetime(2024, 1, 15, 10, 30, 0)}]
        expected = [{"dt": datetime(2024, 1, 15, 10, 30, 0)}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_date_string(self):
        """assert_rows_equal handles date strings."""
        from sparkless.testing import assert_rows_equal

        actual = [{"dt": "2024-01-15"}]
        expected = [{"dt": "2024-01-15"}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_datetime_string_with_t(self):
        """assert_rows_equal handles datetime strings with T separator."""
        from sparkless.testing import assert_rows_equal

        actual = [{"dt": "2024-01-15T10:30:00"}]
        expected = [{"dt": "2024-01-15T10:30:00"}]
        assert_rows_equal(actual, expected)

    def test_assert_rows_equal_datetime_string_with_space(self):
        """assert_rows_equal handles datetime strings with space separator."""
        from sparkless.testing import assert_rows_equal

        actual = [{"dt": "2024-01-15 10:30:00"}]
        expected = [{"dt": "2024-01-15 10:30:00"}]
        assert_rows_equal(actual, expected)

    def test_is_date_like_short_string(self):
        """_is_date_like returns False for short strings."""
        from sparkless.testing.comparison import _is_date_like

        assert _is_date_like("2024") is False
        assert _is_date_like("short") is False

    def test_is_date_like_invalid_format(self):
        """_is_date_like returns False for invalid format."""
        from sparkless.testing.comparison import _is_date_like

        assert _is_date_like("20240115abc") is False
        assert _is_date_like("abcd-ef-gh") is False

    def test_normalize_date_like_not_date(self):
        """_normalize_date_like returns (None, None) for non-date values."""
        from sparkless.testing.comparison import _normalize_date_like

        assert _normalize_date_like("not a date") == (None, None)
        assert _normalize_date_like(123) == (None, None)


class TestComparisonDataFrames:
    """Tests for DataFrame comparison functions."""

    def test_compare_dataframes_result(self, spark):
        """compare_dataframes returns ComparisonResult."""
        from sparkless.testing import compare_dataframes, ComparisonResult

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "a")], ["id", "val"])

        result = compare_dataframes(df1, df2)
        assert isinstance(result, ComparisonResult)
        assert result.equivalent is True
        assert result.errors == []
        assert result.row_count_match is True
        assert result.schema_match is True
        assert result.column_match is True

    def test_compare_dataframes_different_values(self, spark):
        """compare_dataframes detects value differences."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "b")], ["id", "val"])

        result = compare_dataframes(df1, df2)
        assert result.equivalent is False
        assert len(result.errors) > 0

    def test_compare_dataframes_different_row_count(self, spark):
        """compare_dataframes detects row count differences."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1,)], ["id"])
        df2 = spark.createDataFrame([(1,), (2,)], ["id"])

        result = compare_dataframes(df1, df2)
        assert result.equivalent is False
        assert result.row_count_match is False

    def test_compare_dataframes_with_dict_expected(self, spark):
        """compare_dataframes accepts dict as expected."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        expected = {"data": [{"id": 1, "val": "a"}]}

        result = compare_dataframes(df1, expected)
        assert result.equivalent is True

    def test_compare_dataframes_with_empty_dict_data(self, spark):
        """compare_dataframes handles empty dict data with column mismatch."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([], "id: int")
        expected = {"data": []}

        # Empty expected data has no columns, so there's a column mismatch
        result = compare_dataframes(df1, expected)
        assert result.row_count_match is True
        assert result.column_match is False  # No columns in empty expected

    def test_compare_dataframes_both_empty(self, spark):
        """compare_dataframes handles two empty DataFrames."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([], "id: int")
        df2 = spark.createDataFrame([], "id: int")

        result = compare_dataframes(df1, df2)
        assert result.row_count_match is True
        assert result.equivalent is True

    def test_compare_dataframes_check_order_false(self, spark):
        """compare_dataframes with check_order=False ignores order."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "val"])

        result = compare_dataframes(df1, df2, check_order=False)
        assert result.equivalent is True

    def test_compare_dataframes_check_schema_false(self, spark):
        """compare_dataframes with check_schema=False skips schema check."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "a")], ["id", "val"])

        result = compare_dataframes(df1, df2, check_schema=False)
        assert result.equivalent is True

    def test_assert_dataframes_equal_matching(self, spark):
        """assert_dataframes_equal passes for matching DataFrames."""
        from sparkless.testing import assert_dataframes_equal

        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        assert_dataframes_equal(df1, df2)

    def test_assert_dataframes_equal_with_custom_msg(self, spark):
        """assert_dataframes_equal includes custom message in error."""
        from sparkless.testing import assert_dataframes_equal

        df1 = spark.createDataFrame([(1, "a")], ["id", "val"])
        df2 = spark.createDataFrame([(1, "b")], ["id", "val"])

        with pytest.raises(AssertionError, match="Custom message"):
            assert_dataframes_equal(df1, df2, msg="Custom message")

    def test_assert_dataframes_equal_check_order_false(self, spark):
        """assert_dataframes_equal with check_order=False ignores order."""
        from sparkless.testing import assert_dataframes_equal

        df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        df2 = spark.createDataFrame([(2, "b"), (1, "a")], ["id", "val"])
        assert_dataframes_equal(df1, df2, check_order=False)


class TestComparisonInternals:
    """Tests for internal comparison functions."""

    def test_row_to_dict_with_dict(self):
        """_row_to_dict handles plain dict."""
        from sparkless.testing.comparison import _row_to_dict

        d = {"id": 1, "val": "a"}
        assert _row_to_dict(d) == {"id": 1, "val": "a"}

    def test_row_to_dict_with_iterable_value(self):
        """_row_to_dict converts iterable values to lists."""
        from sparkless.testing.comparison import _row_to_dict

        d = {"arr": (1, 2, 3)}
        result = _row_to_dict(d)
        assert result["arr"] == [1, 2, 3]

    def test_row_to_dict_preserves_strings(self):
        """_row_to_dict doesn't convert strings to lists."""
        from sparkless.testing.comparison import _row_to_dict

        d = {"val": "hello"}
        result = _row_to_dict(d)
        assert result["val"] == "hello"

    def test_row_to_dict_preserves_bytes(self):
        """_row_to_dict doesn't convert bytes to lists."""
        from sparkless.testing.comparison import _row_to_dict

        d = {"val": b"hello"}
        result = _row_to_dict(d)
        assert result["val"] == b"hello"

    def test_row_to_dict_preserves_nested_dict(self):
        """_row_to_dict doesn't convert nested dicts."""
        from sparkless.testing.comparison import _row_to_dict

        d = {"nested": {"a": 1}}
        result = _row_to_dict(d)
        assert result["nested"] == {"a": 1}

    def test_sort_key_value_none(self):
        """_sort_key_value handles None."""
        from sparkless.testing.comparison import _sort_key_value

        assert _sort_key_value(None) == (0, "")

    def test_sort_key_value_non_none(self):
        """_sort_key_value handles non-None values."""
        from sparkless.testing.comparison import _sort_key_value

        result = _sort_key_value(42)
        assert result[0] == 1
        assert "int" in result[1]

    def test_compare_schemas_different_field_count(self):
        """_compare_schemas returns False for different field counts."""
        from sparkless.testing.comparison import _compare_schemas

        class MockField:
            def __init__(self, name, dataType):
                self.name = name
                self.dataType = dataType

        class MockSchema:
            def __init__(self, fields):
                self.fields = fields

        schema1 = MockSchema([MockField("a", str)])
        schema2 = MockSchema([MockField("a", str), MockField("b", str)])

        assert _compare_schemas(schema1, schema2) is False

    def test_compare_schemas_different_field_names(self):
        """_compare_schemas returns False for different field names."""
        from sparkless.testing.comparison import _compare_schemas

        class MockField:
            def __init__(self, name, dataType):
                self.name = name
                self.dataType = dataType

        class MockSchema:
            def __init__(self, fields):
                self.fields = fields

        schema1 = MockSchema([MockField("a", str)])
        schema2 = MockSchema([MockField("b", str)])

        assert _compare_schemas(schema1, schema2) is False

    def test_compare_types_both_none(self):
        """_compare_types returns True when both are None."""
        from sparkless.testing.comparison import _compare_types

        assert _compare_types(None, None) is True

    def test_compare_types_one_none(self):
        """_compare_types returns False when one is None."""
        from sparkless.testing.comparison import _compare_types

        assert _compare_types(None, str) is False
        assert _compare_types(str, None) is False


# =============================================================================
# Fixtures Tests
# =============================================================================


class TestFixtures:
    """Tests for sparkless.testing fixtures."""

    def test_spark_fixture_creates_session(self, spark):
        """The spark fixture creates a working session."""
        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        assert df.count() == 1

    def test_spark_fixture_supports_operations(self, spark):
        """The spark fixture supports DataFrame operations."""
        df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "val"])
        filtered = df.filter(df.id > 1)
        assert filtered.count() == 2

    def test_spark_mode_fixture(self, spark_mode):
        """The spark_mode fixture returns current mode."""
        from sparkless.testing import Mode

        assert spark_mode in (Mode.SPARKLESS, Mode.PYSPARK)

    def test_spark_imports_fixture(self, spark_imports):
        """The spark_imports fixture provides imports."""
        assert spark_imports.SparkSession is not None
        assert spark_imports.F is not None
        assert spark_imports.Window is not None

    def test_spark_imports_fixture_has_types(self, spark_imports):
        """The spark_imports fixture provides data types."""
        assert spark_imports.StringType is not None
        assert spark_imports.IntegerType is not None
        assert spark_imports.LongType is not None

    def test_isolated_session_fixture(self, isolated_session):
        """The isolated_session fixture creates an isolated session."""
        df = isolated_session.createDataFrame([(1,)], ["id"])
        assert df.count() == 1

    def test_table_prefix_fixture(self, table_prefix):
        """The table_prefix fixture returns a unique prefix."""
        assert table_prefix.startswith("t_")
        assert len(table_prefix) > 10  # Has UUID suffix

    def test_table_prefix_fixture_unique(self, table_prefix, request):
        """The table_prefix fixture generates unique prefixes."""
        import re
        import uuid as uuid_module

        # Extract the UUID part
        match = re.search(r"_([a-f0-9]{6})$", table_prefix)
        assert match is not None


class TestSharedSession:
    """Tests for shared session functionality."""

    def test_use_shared_session_default_false(self):
        """_use_shared_session returns False by default."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(os.environ, {}, clear=True):
            os.environ.pop("SPARKLESS_SHARED_SESSION", None)
            os.environ.pop("PYTEST_XDIST_WORKER", None)
            assert _use_shared_session() is False

    def test_use_shared_session_enabled(self):
        """_use_shared_session returns True when enabled."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(
            os.environ,
            {"SPARKLESS_SHARED_SESSION": "1", "SPARKLESS_TEST_MODE": "sparkless"},
        ):
            os.environ.pop("PYTEST_XDIST_WORKER", None)
            assert _use_shared_session() is True

    def test_use_shared_session_enabled_with_true(self):
        """_use_shared_session returns True when set to 'true'."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(
            os.environ,
            {"SPARKLESS_SHARED_SESSION": "true", "SPARKLESS_TEST_MODE": "sparkless"},
        ):
            os.environ.pop("PYTEST_XDIST_WORKER", None)
            assert _use_shared_session() is True

    def test_use_shared_session_enabled_with_yes(self):
        """_use_shared_session returns True when set to 'yes'."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(
            os.environ,
            {"SPARKLESS_SHARED_SESSION": "yes", "SPARKLESS_TEST_MODE": "sparkless"},
        ):
            os.environ.pop("PYTEST_XDIST_WORKER", None)
            assert _use_shared_session() is True

    def test_use_shared_session_disabled_in_xdist(self):
        """_use_shared_session returns False in pytest-xdist workers."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(
            os.environ,
            {
                "SPARKLESS_SHARED_SESSION": "1",
                "PYTEST_XDIST_WORKER": "gw0",
                "SPARKLESS_TEST_MODE": "sparkless",
            },
        ):
            assert _use_shared_session() is False

    def test_use_shared_session_disabled_in_pyspark_mode(self):
        """_use_shared_session returns False in PySpark mode."""
        from sparkless.testing.fixtures import _use_shared_session

        with mock.patch.dict(
            os.environ,
            {"SPARKLESS_SHARED_SESSION": "1", "SPARKLESS_TEST_MODE": "pyspark"},
        ):
            os.environ.pop("PYTEST_XDIST_WORKER", None)
            assert _use_shared_session() is False


class TestSharedSessionWrapper:
    """Tests for _SharedSessionWrapper class."""

    def test_wrapper_stop_is_noop(self):
        """_SharedSessionWrapper.stop() does nothing."""
        from sparkless.testing.fixtures import _SharedSessionWrapper

        class MockSession:
            stopped = False

            def stop(self):
                self.stopped = True

        mock_session = MockSession()
        wrapper = _SharedSessionWrapper(mock_session)

        wrapper.stop()
        assert mock_session.stopped is False  # stop should not be called

    def test_wrapper_delegates_attributes(self):
        """_SharedSessionWrapper delegates attribute access."""
        from sparkless.testing.fixtures import _SharedSessionWrapper

        class MockSession:
            some_attr = "test_value"

            def some_method(self):
                return "method_result"

        mock_session = MockSession()
        wrapper = _SharedSessionWrapper(mock_session)

        assert wrapper.some_attr == "test_value"
        assert wrapper.some_method() == "method_result"

    def test_wrapper_with_real_session(self, spark):
        """_SharedSessionWrapper works with real session."""
        from sparkless.testing.fixtures import _SharedSessionWrapper

        wrapper = _SharedSessionWrapper(spark)

        # Should be able to create DataFrames through wrapper
        df = wrapper.createDataFrame([(1, "a")], ["id", "val"])
        assert df.count() == 1

        # stop() should be no-op
        wrapper.stop()
        # Session should still work
        df2 = wrapper.createDataFrame([(2,)], ["x"])
        assert df2.count() == 1


class TestGetSharedSession:
    """Tests for _get_shared_session function."""

    def test_get_shared_session_creates_session(self):
        """_get_shared_session creates a session if not exists."""
        from sparkless.testing import Mode
        from sparkless.testing.fixtures import _get_shared_session, _shared_sessions

        # Clear any existing shared sessions
        _shared_sessions.clear()

        session = _get_shared_session(Mode.SPARKLESS)
        assert session is not None

        # Session should be cached
        assert Mode.SPARKLESS in _shared_sessions
        assert _shared_sessions[Mode.SPARKLESS] is session

        # Clean up
        try:
            session.stop()
        except Exception:
            pass
        _shared_sessions.clear()

    def test_get_shared_session_returns_cached(self):
        """_get_shared_session returns cached session."""
        from sparkless.testing import Mode
        from sparkless.testing.fixtures import _get_shared_session, _shared_sessions

        # Clear any existing shared sessions
        _shared_sessions.clear()

        session1 = _get_shared_session(Mode.SPARKLESS)
        session2 = _get_shared_session(Mode.SPARKLESS)

        assert session1 is session2

        # Clean up
        try:
            session1.stop()
        except Exception:
            pass
        _shared_sessions.clear()


# =============================================================================
# Plugin Tests
# =============================================================================


class TestPlugin:
    """Tests for sparkless.testing pytest plugin."""

    def test_plugin_exports_fixtures(self):
        """Plugin exports expected fixtures."""
        from sparkless.testing import plugin

        assert hasattr(plugin, "spark")
        assert hasattr(plugin, "spark_mode")
        assert hasattr(plugin, "spark_imports")
        assert hasattr(plugin, "isolated_session")
        assert hasattr(plugin, "table_prefix")
        assert hasattr(plugin, "cleanup_after_each_test")

    def test_plugin_has_pytest_hooks(self):
        """Plugin has pytest hooks."""
        from sparkless.testing import plugin

        assert hasattr(plugin, "pytest_configure")
        assert hasattr(plugin, "pytest_collection_modifyitems")
        assert hasattr(plugin, "pytest_report_header")

    def test_plugin_all_exports(self):
        """Plugin __all__ contains expected items."""
        from sparkless.testing import plugin

        expected = [
            "spark",
            "spark_mode",
            "spark_imports",
            "isolated_session",
            "table_prefix",
            "cleanup_after_each_test",
            "pytest_configure",
            "pytest_collection_modifyitems",
            "pytest_report_header",
        ]
        for item in expected:
            assert item in plugin.__all__


# =============================================================================
# Public API Tests
# =============================================================================


class TestPublicAPI:
    """Tests for sparkless.testing public API exports."""

    def test_all_exports_available(self):
        """All expected exports are available from sparkless.testing."""
        from sparkless.testing import (
            # Mode
            Mode,
            ENV_VAR_NAME,
            get_mode,
            is_pyspark_mode,
            is_sparkless_mode,
            set_mode,
            # Session
            create_session,
            # Imports
            SparkImports,
            get_imports,
            # Comparison
            ComparisonResult,
            compare_dataframes,
            assert_dataframes_equal,
            assert_rows_equal,
            # Fixtures
            spark,
            spark_mode,
            spark_imports,
            isolated_session,
            table_prefix,
        )

        # All imports should be non-None
        assert Mode is not None
        assert ENV_VAR_NAME == "SPARKLESS_TEST_MODE"
        assert get_mode is not None
        assert is_pyspark_mode is not None
        assert is_sparkless_mode is not None
        assert set_mode is not None
        assert create_session is not None
        assert SparkImports is not None
        assert get_imports is not None
        assert ComparisonResult is not None
        assert compare_dataframes is not None
        assert assert_dataframes_equal is not None
        assert assert_rows_equal is not None
        assert spark is not None
        assert spark_mode is not None
        assert spark_imports is not None
        assert isolated_session is not None
        assert table_prefix is not None

    def test_env_var_name_constant(self):
        """ENV_VAR_NAME is the expected constant."""
        from sparkless.testing import ENV_VAR_NAME

        assert ENV_VAR_NAME == "SPARKLESS_TEST_MODE"

    def test_comparison_result_dataclass(self):
        """ComparisonResult is a proper dataclass."""
        from sparkless.testing import ComparisonResult

        result = ComparisonResult(
            equivalent=True,
            errors=[],
            row_count_match=True,
            schema_match=True,
            column_match=True,
        )
        assert result.equivalent is True
        assert result.errors == []
        assert result.row_count_match is True
        assert result.schema_match is True
        assert result.column_match is True

    def test_comparison_result_defaults(self):
        """ComparisonResult has sensible defaults."""
        from sparkless.testing import ComparisonResult

        result = ComparisonResult(equivalent=False)
        assert result.errors == []
        assert result.row_count_match is True
        assert result.schema_match is True
        assert result.column_match is True


# =============================================================================
# Marker Tests
# =============================================================================


@pytest.mark.sparkless_only
class TestSparklessOnlyMarker:
    """Tests that should only run in sparkless mode."""

    def test_sparkless_specific(self, spark):
        """This test only runs in sparkless mode."""
        df = spark.createDataFrame([(1,)], ["id"])
        assert df.count() == 1


class TestMarkerSkipping:
    """Tests for marker-based skipping."""

    def test_sparkless_only_marker_exists(self):
        """sparkless_only marker is recognized."""
        # This test verifies the marker exists and is used
        assert pytest.mark.sparkless_only is not None

    def test_pyspark_only_marker_exists(self):
        """pyspark_only marker is recognized."""
        assert pytest.mark.pyspark_only is not None


# =============================================================================
# Additional Edge Case Tests
# =============================================================================


class TestComparisonEdgeCases:
    """Additional edge case tests for comparison utilities."""

    def test_values_equal_different_types(self):
        """_values_equal handles different types correctly."""
        from sparkless.testing.comparison import _values_equal

        # Different types should not be equal
        assert _values_equal(1, "1") is False
        assert _values_equal([1], (1,)) is False

    def test_values_equal_empty_lists(self):
        """_values_equal handles empty lists."""
        from sparkless.testing.comparison import _values_equal

        assert _values_equal([], []) is True

    def test_values_equal_empty_dicts(self):
        """_values_equal handles empty dicts."""
        from sparkless.testing.comparison import _values_equal

        assert _values_equal({}, {}) is True

    def test_values_equal_nested_float_tolerance(self):
        """_values_equal applies tolerance to nested floats."""
        from sparkless.testing.comparison import _values_equal

        actual = {"nested": [1.0000001, 2.0000001]}
        expected = {"nested": [1.0, 2.0]}
        assert _values_equal(actual, expected, tolerance=1e-5) is True

    def test_row_to_dict_with_row_object(self, spark):
        """_row_to_dict handles Row objects from collect()."""
        from sparkless.testing.comparison import _row_to_dict

        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        rows = df.collect()

        result = _row_to_dict(rows[0])
        assert result == {"id": 1, "val": "a"}

    def test_compare_dataframes_with_tolerance(self, spark):
        """compare_dataframes respects tolerance parameter."""
        from sparkless.testing import compare_dataframes

        df1 = spark.createDataFrame([(1.001,)], ["val"])
        df2 = spark.createDataFrame([(1.0,)], ["val"])

        # Should pass with larger tolerance
        result = compare_dataframes(df1, df2, tolerance=0.01)
        assert result.equivalent is True

        # Should fail with smaller tolerance
        result = compare_dataframes(df1, df2, tolerance=1e-5)
        assert result.equivalent is False

    def test_assert_rows_equal_with_row_objects(self, spark):
        """assert_rows_equal handles Row objects from collect()."""
        from sparkless.testing import assert_rows_equal

        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        rows = df.collect()

        expected = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        assert_rows_equal(rows, expected)

    def test_sort_rows_by_key_with_none_values(self):
        """_sort_rows_by_key handles None values correctly."""
        from sparkless.testing.comparison import _sort_rows_by_key

        rows = [
            {"id": 2, "val": None},
            {"id": 1, "val": "a"},
            {"id": 3, "val": None},
        ]
        sorted_rows = _sort_rows_by_key(rows)

        # Should not raise, and should have consistent ordering
        assert len(sorted_rows) == 3


class TestImportsEdgeCases:
    """Additional edge case tests for imports."""

    def test_spark_imports_mode_attribute(self):
        """SparkImports has mode attribute."""
        from sparkless.testing import Mode, get_imports

        imports = get_imports(mode=Mode.SPARKLESS)
        assert imports.mode == Mode.SPARKLESS

    def test_get_imports_creates_new_instance(self):
        """get_imports creates a new instance each time."""
        from sparkless.testing import Mode, get_imports

        imports1 = get_imports(mode=Mode.SPARKLESS)
        imports2 = get_imports(mode=Mode.SPARKLESS)

        # Should be different instances
        assert imports1 is not imports2


class TestSessionEdgeCases:
    """Additional edge case tests for session creation."""

    def test_create_session_multiple_times(self):
        """create_session can create multiple sessions."""
        from sparkless.testing import Mode, create_session

        session1 = create_session(app_name="test1", mode=Mode.SPARKLESS)
        session2 = create_session(app_name="test2", mode=Mode.SPARKLESS)

        try:
            # Both sessions should work
            df1 = session1.createDataFrame([(1,)], ["id"])
            df2 = session2.createDataFrame([(2,)], ["id"])

            assert df1.count() == 1
            assert df2.count() == 1
        finally:
            session1.stop()
            session2.stop()


class TestPluginEdgeCases:
    """Additional edge case tests for pytest plugin."""

    def test_pytest_report_header(self):
        """pytest_report_header returns mode info."""
        from sparkless.testing.plugin import pytest_report_header

        class MockConfig:
            pass

        headers = pytest_report_header(MockConfig())
        assert isinstance(headers, list)
        assert len(headers) == 1
        assert "sparkless.testing mode:" in headers[0]

    def test_plugin_env_var_name(self):
        """Plugin uses correct ENV_VAR_NAME."""
        from sparkless.testing import ENV_VAR_NAME

        assert ENV_VAR_NAME == "SPARKLESS_TEST_MODE"
