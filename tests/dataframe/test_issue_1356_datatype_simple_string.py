"""Tests for #1356: DataType.simpleString() PySpark parity.

Sparkless sql.types types (IntegerType, LongType, StringType, etc.) must expose
simpleString() returning the same strings as PySpark (e.g. 'int', 'long', 'string').
"""

from __future__ import annotations

import pytest

# Use sparkless types so we test the implementation under test
from sparkless.sql import types as T


def test_integer_type_simple_string() -> None:
    assert T.IntegerType().simpleString() == "int"


def test_long_type_simple_string() -> None:
    assert T.LongType().simpleString() == "long"


def test_string_type_simple_string() -> None:
    assert T.StringType().simpleString() == "string"


def test_double_type_simple_string() -> None:
    assert T.DoubleType().simpleString() == "double"


def test_float_type_simple_string() -> None:
    assert T.FloatType().simpleString() == "float"


def test_boolean_type_simple_string() -> None:
    assert T.BooleanType().simpleString() == "boolean"


def test_date_type_simple_string() -> None:
    assert T.DateType().simpleString() == "date"


def test_timestamp_type_simple_string() -> None:
    assert T.TimestampType().simpleString() == "timestamp"


def test_array_type_simple_string() -> None:
    assert T.ArrayType(T.StringType()).simpleString() == "array<string>"
    assert T.ArrayType(T.IntegerType()).simpleString() == "array<int>"


def test_struct_type_simple_string() -> None:
    st = T.StructType(
        [
            T.StructField("a", T.StringType()),
            T.StructField("b", T.IntegerType()),
        ]
    )
    result = st.simpleString()
    assert "a" in result and "b" in result
    assert "string" in result and "int" in result
