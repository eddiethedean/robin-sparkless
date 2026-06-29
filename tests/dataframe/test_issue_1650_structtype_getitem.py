"""Issue #1650: StructType supports subscript access like PySpark."""

from __future__ import annotations

from sparkless.testing import get_imports

T = get_imports()


def test_struct_type_getitem_by_name() -> None:
    """schema['field_name'] returns the StructField."""
    schema = T.StructType(
        [
            T.StructField("col1", T.StringType()),
            T.StructField("col2", T.IntegerType()),
        ]
    )
    field = schema["col1"]
    assert field.name == "col1"
    assert isinstance(field.dataType, T.StringType)


def test_struct_type_getitem_by_index() -> None:
    """schema[0] returns the StructField at that position."""
    schema = T.StructType(
        [
            T.StructField("col1", T.StringType()),
            T.StructField("col2", T.IntegerType()),
        ]
    )
    assert schema[0].name == "col1"
    assert schema[1].name == "col2"


def test_struct_type_getitem_by_slice() -> None:
    """schema[1:3] returns a new StructType with the selected fields."""
    schema = T.StructType(
        [
            T.StructField("a", T.StringType()),
            T.StructField("b", T.IntegerType()),
            T.StructField("c", T.BooleanType()),
        ]
    )
    subset = schema[1:3]
    assert isinstance(subset, T.StructType)
    assert [f.name for f in subset] == ["b", "c"]


def test_struct_type_getitem_missing_name_raises() -> None:
    schema = T.StructType([T.StructField("col1", T.StringType())])
    try:
        schema["missing"]
        raise AssertionError("expected KeyError")
    except KeyError as exc:
        assert "No StructField named missing" in str(exc)


def test_struct_type_getitem_bad_index_raises() -> None:
    schema = T.StructType([T.StructField("col1", T.StringType())])
    try:
        schema[5]
        raise AssertionError("expected IndexError")
    except IndexError as exc:
        assert "StructType index out of range" in str(exc)
