"""Tests for #1356: DataType.simpleString() PySpark parity.

Sparkless sql.types must match PySpark: same simpleString() values
(e.g. PySpark uses 'bigint' for LongType, 'int' for IntegerType).
"""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()


def test_integer_type_simple_string() -> None:
    assert _imports.IntegerType().simpleString() == "int"


def test_long_type_simple_string() -> None:
    # PySpark uses "bigint" for LongType; sparkless must match.
    assert _imports.LongType().simpleString() == "bigint"


def test_string_type_simple_string() -> None:
    assert _imports.StringType().simpleString() == "string"


def test_double_type_simple_string() -> None:
    assert _imports.DoubleType().simpleString() == "double"


def test_float_type_simple_string() -> None:
    assert _imports.FloatType().simpleString() == "float"


def test_boolean_type_simple_string() -> None:
    assert _imports.BooleanType().simpleString() == "boolean"


def test_date_type_simple_string() -> None:
    assert _imports.DateType().simpleString() == "date"


def test_timestamp_type_simple_string() -> None:
    assert _imports.TimestampType().simpleString() == "timestamp"


def test_array_type_simple_string() -> None:
    assert _imports.ArrayType(_imports.StringType()).simpleString() == "array<string>"
    assert _imports.ArrayType(_imports.IntegerType()).simpleString() == "array<int>"


def test_struct_type_simple_string() -> None:
    st = _imports.StructType(
        [
            _imports.StructField("a", _imports.StringType()),
            _imports.StructField("b", _imports.IntegerType()),
        ]
    )
    result = st.simpleString()
    assert "a" in result and "b" in result
    assert "string" in result and "int" in result
