"""Issue #1572: DataType.typeName() PySpark parity."""

from __future__ import annotations

from sparkless.testing import get_imports

_imports = get_imports()


def test_schema_field_datatype_type_name(spark) -> None:
    df = spark.createDataFrame([{"name": "Alice"}])
    types = [sf.dataType.typeName() for sf in df.schema]
    assert types == ["string"]


def test_string_type_type_name() -> None:
    assert _imports.StringType().typeName() == "string"


def test_integer_type_name_differs_from_simple_string() -> None:
    it = _imports.IntegerType()
    assert it.typeName() == "integer"
    assert it.simpleString() == "int"


def test_long_type_name_differs_from_simple_string() -> None:
    lt = _imports.LongType()
    assert lt.typeName() == "long"
    assert lt.simpleString() == "bigint"
