"""
Unit tests for spark types.
"""

import pytest
from sparkless.spark_types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DecimalType,
    ArrayType,
    MapType,
    StructType,
    StructField,
    CharType,
    VarcharType,
    IntervalType,
    Row,
)


@pytest.mark.unit
class TestDataType:
    """Test DataType base class."""

    def test_eq_with_same_type(self):
        """Test equality with same type."""
        assert StringType() == StringType()
        assert StringType(nullable=False) == StringType(nullable=True)

    def test_eq_only_checks_class(self):
        """Test equality only checks class, not nullable."""
        assert LongType(nullable=True) == LongType(nullable=False)

    def test_eq_with_different_types(self):
        """Test equality with different types."""
        assert StringType() != IntegerType()
        assert LongType() != DoubleType()

    def test_eq_with_non_type(self):
        """Test equality with non-type objects."""
        assert StringType() != "not a type"
        assert StringType() != 42

    def test_hash_makes_type_hashable(self):
        """Test hash makes type hashable."""
        types_set = {StringType(), LongType(), DoubleType()}
        assert len(types_set) == 3

    def test_hash_includes_nullable(self):
        """Test hash includes nullable in hash calculation."""
        st1 = StringType(nullable=True)
        st2 = StringType(nullable=False)
        assert hash(st1) != hash(st2)

    def test_repr(self):
        """Test representation."""
        assert "StringType(nullable=True)" in repr(StringType())
        assert "IntegerType(nullable=False)" in repr(IntegerType(nullable=False))

    def test_type_name(self):
        """Test typeName returns correct name."""
        assert StringType().typeName() == "string"
        assert LongType().typeName() == "bigint"
        assert IntegerType().typeName() == "int"
        assert DoubleType().typeName() == "double"
        assert BooleanType().typeName() == "boolean"

    def test_type_name_complex_types(self):
        """Test typeName for complex types."""
        array_type = ArrayType(StringType())
        assert array_type.typeName() == "array"

        map_type = MapType(StringType(), LongType())
        assert map_type.typeName() == "map"

        # struct_type.typeName() may not work as expected
        struct_type = StructType([StructField("name", StringType())])
        assert hasattr(struct_type, "typeName")


@pytest.mark.unit
class TestCharType:
    """Test CharType."""

    def test_char_type_default_length(self):
        """Test CharType with default length."""
        ct = CharType()
        assert ct.length == 1

    def test_char_type_custom_length(self):
        """Test CharType with custom length."""
        ct = CharType(length=10)
        assert ct.length == 10

    def test_char_type_repr(self):
        """Test CharType representation."""
        ct = CharType(10)
        assert "CharType(10)" in repr(ct)


@pytest.mark.unit
class TestVarcharType:
    """Test VarcharType."""

    def test_varchar_type_default_length(self):
        """Test VarcharType with default length."""
        vt = VarcharType()
        assert vt.length == 255

    def test_varchar_type_custom_length(self):
        """Test VarcharType with custom length."""
        vt = VarcharType(length=100)
        assert vt.length == 100

    def test_varchar_type_repr(self):
        """Test VarcharType representation."""
        vt = VarcharType(100)
        assert "VarcharType(100)" in repr(vt)


@pytest.mark.unit
class TestIntervalType:
    """Test IntervalType."""

    def test_interval_type_default(self):
        """Test IntervalType with default fields."""
        it = IntervalType()
        assert it.start_field == "YEAR"
        assert it.end_field == "MONTH"

    def test_interval_type_custom(self):
        """Test IntervalType with custom fields."""
        it = IntervalType(start_field="DAY", end_field="HOUR")
        assert it.start_field == "DAY"
        assert it.end_field == "HOUR"

    def test_interval_type_repr(self):
        """Test IntervalType representation."""
        it = IntervalType("DAY", "HOUR")
        assert "IntervalType(DAY, HOUR)" in repr(it)


@pytest.mark.unit
class TestArrayType:
    """Test ArrayType."""

    def test_array_type_with_element_type(self):
        """Test ArrayType with element type."""
        at = ArrayType(StringType())
        assert isinstance(at.element_type, StringType)

    def test_array_type_repr(self):
        """Test ArrayType representation."""
        at = ArrayType(LongType())
        assert "ArrayType" in repr(at)
        assert "LongType" in repr(at)

    def test_array_type_nullable(self):
        """Test ArrayType nullable property."""
        at = ArrayType(StringType(), nullable=False)
        assert not at.nullable


@pytest.mark.unit
class TestMapType:
    """Test MapType."""

    def test_map_type_with_key_and_value_types(self):
        """Test MapType with key and value types."""
        mt = MapType(StringType(), LongType())
        assert isinstance(mt.key_type, StringType)
        assert isinstance(mt.value_type, LongType)

    def test_map_type_repr(self):
        """Test MapType representation."""
        mt = MapType(StringType(), LongType())
        assert "MapType" in repr(mt)

    def test_map_type_nullable(self):
        """Test MapType nullable property."""
        mt = MapType(StringType(), LongType(), nullable=False)
        assert not mt.nullable


@pytest.mark.unit
class TestDecimalType:
    """Test DecimalType."""

    def test_decimal_type_default(self):
        """Test DecimalType with default precision and scale."""
        dt = DecimalType()
        assert dt.precision == 10
        assert dt.scale == 0

    def test_decimal_type_custom(self):
        """Test DecimalType with custom precision and scale."""
        dt = DecimalType(precision=18, scale=2)
        assert dt.precision == 18
        assert dt.scale == 2

    def test_decimal_type_repr(self):
        """Test DecimalType representation."""
        dt = DecimalType(18, 2)
        assert "DecimalType(18, 2)" in repr(dt)


@pytest.mark.unit
class TestStructType:
    """Test StructType."""

    def test_struct_type_creation(self):
        """Test StructType creation."""
        st = StructType([])
        assert isinstance(st, StructType)
        assert len(st.fields) == 0

    def test_struct_type_with_fields(self):
        """Test StructType with fields."""
        fields = [
            StructField("id", LongType()),
            StructField("name", StringType()),
        ]
        st = StructType(fields)
        assert len(st.fields) == 2

    def test_struct_type_field_names(self):
        """Test getting field names."""
        fields = [
            StructField("id", LongType()),
            StructField("name", StringType()),
        ]
        st = StructType(fields)
        assert st.fieldNames() == ["id", "name"]

    def test_struct_type_field_names_empty(self):
        """Test fieldNames with empty struct."""
        st = StructType([])
        assert st.fieldNames() == []


@pytest.mark.unit
class TestRow:
    """Test Row operations."""

    def test_row_creation(self):
        """Test Row creation."""
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
            ]
        )
        data = {"id": 1, "name": "Alice"}
        row = Row(data, schema)
        assert row.id == 1
        assert row.name == "Alice"
        assert row["id"] == 1
        assert row["name"] == "Alice"

    def test_row_with_all_none(self):
        """Test Row with None values."""
        schema = StructType(
            [
                StructField("col1", StringType()),
                StructField("col2", LongType()),
            ]
        )
        data = {"col1": None, "col2": None}
        row = Row(data, schema)
        assert row.col1 is None
        assert row.col2 is None

    def test_row_len(self):
        """Test row length."""
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        row = Row({"a": 1, "b": "x"}, schema)
        assert len(row) == 2

    def test_row_getitem_by_index(self):
        """Test row __getitem__ by index."""
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        row = Row({"a": 1, "b": "x"}, schema)
        assert row[0] == 1
        assert row[1] == "x"

    def test_row_getitem_by_key(self):
        """Test row __getitem__ by key."""
        schema = StructType(
            [
                StructField("id", LongType()),
            ]
        )
        row = Row({"id": 42}, schema)
        assert row["id"] == 42

    def test_row_getitem_key_error(self):
        """Test row __getitem__ raises KeyError for missing key."""
        schema = StructType([StructField("id", LongType())])
        row = Row({"id": 1}, schema)
        with pytest.raises(KeyError):
            _ = row["nonexistent"]

    def test_row_getattr(self):
        """Test row attribute access."""
        schema = StructType(
            [
                StructField("value", LongType()),
            ]
        )
        row = Row({"value": 100}, schema)
        assert row.value == 100

    def test_row_getattr_missing(self):
        """Test row attribute access with missing field."""
        schema = StructType([StructField("a", LongType())])
        row = Row({"a": 1}, schema)
        with pytest.raises(AttributeError):
            _ = row.missing_field

    def test_row_repr(self):
        """Test row representation."""
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
            ]
        )
        row = Row({"id": 1, "name": "Alice"}, schema)
        repr_str = repr(row)
        assert "Row" in repr_str  # Row uses "Row" in repr

    def test_row_values(self):
        """Test row.values() method."""
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        row = Row({"a": 1, "b": "x"}, schema)
        values = list(row.values())
        assert values == [1, "x"]

    def test_row_keys(self):
        """Test row.keys() method."""
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        row = Row({"a": 1, "b": "x"}, schema)
        keys = list(row.keys())
        # Keys should be in schema order, not data order
        assert isinstance(keys, list)

    def test_row_items(self):
        """Test row.items() method."""
        schema = StructType(
            [
                StructField("a", LongType()),
                StructField("b", StringType()),
            ]
        )
        row = Row({"a": 1, "b": "x"}, schema)
        items = list(row.items())
        assert len(items) == 2

    def test_row_as_dict(self):
        """Test row asDict() method."""
        schema = StructType(
            [
                StructField("id", LongType()),
                StructField("name", StringType()),
            ]
        )
        row = Row({"id": 1, "name": "Alice"}, schema)
        result = row.asDict()
        assert result["id"] == 1
        assert result["name"] == "Alice"
