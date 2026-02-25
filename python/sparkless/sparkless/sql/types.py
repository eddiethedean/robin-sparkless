# PySpark-style types for schema and Row. Used by createDataFrame(schema=StructType(...)).


class DataType:
    def simpleString(self) -> str:
        return "string"


class StringType(DataType):
    def simpleString(self) -> str:
        return "string"


class CharType(DataType):
    """Char(n) type; we treat as string for schema parsing."""
    def simpleString(self) -> str:
        return "string"


class VarcharType(DataType):
    """Varchar(n) type; we treat as string for schema parsing."""
    def simpleString(self) -> str:
        return "string"


class IntervalType(DataType):
    """Interval type; we treat as string for schema parsing."""
    def simpleString(self) -> str:
        return "interval"


class IntegerType(DataType):
    def simpleString(self) -> str:
        return "int"


class LongType(DataType):
    def simpleString(self) -> str:
        return "long"


class DoubleType(DataType):
    def simpleString(self) -> str:
        return "double"


class FloatType(DataType):
    def simpleString(self) -> str:
        return "float"


class BooleanType(DataType):
    def simpleString(self) -> str:
        return "boolean"


class DateType(DataType):
    def simpleString(self) -> str:
        return "date"


class TimestampType(DataType):
    def simpleString(self) -> str:
        return "timestamp"


class ArrayType(DataType):
    def __init__(self, elementType, containsNull=True):
        self.elementType = elementType
        self.containsNull = containsNull

    def simpleString(self) -> str:
        try:
            inner = self.elementType.simpleString()
        except Exception:
            inner = "string"
        return f"array<{inner}>"


class MapType(DataType):
    def __init__(self, keyType, valueType, valueContainsNull=True):
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    def simpleString(self) -> str:
        try:
            k = self.keyType.simpleString()
        except Exception:
            k = "string"
        try:
            v = self.valueType.simpleString()
        except Exception:
            v = "string"
        return f"map<{k},{v}>"


class DecimalType(DataType):
    def simpleString(self) -> str:
        return "decimal"


class StructField:
    def __init__(self, name, dataType, nullable=True, metadata=None):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}


class StructType(DataType):
    def __init__(self, fields=None):
        self.fields = list(fields or [])

    def simpleString(self) -> str:
        if not self.fields:
            return "struct<>"
        parts = []
        for f in self.fields:
            dt = getattr(f, "dataType", None)
            try:
                dt_s = dt.simpleString() if dt is not None else "string"
            except Exception:
                dt_s = "string"
            parts.append(f"{f.name}:{dt_s}")
        return "struct<" + ",".join(parts) + ">"


class Row(tuple):
    """Minimal Row type for collect() compatibility; can be extended."""

    def __new__(cls, *args, **kwargs):
        if kwargs:
            return super().__new__(cls, list(kwargs.values()))
        return super().__new__(cls, args)

    def __getitem__(self, item):
        # PySpark parity: Row supports both positional and name-based indexing.
        if isinstance(item, str):
            fields = self.__dict__.get("_fields", [])
            if item in fields:
                return super().__getitem__(fields.index(item))
            # Case-insensitive fallback (common in tests)
            lowered = {f.lower(): i for i, f in enumerate(fields)}
            if item.lower() in lowered:
                return super().__getitem__(lowered[item.lower()])
            raise KeyError(item)
        return super().__getitem__(item)

    def __contains__(self, item):
        if isinstance(item, str):
            fields = self.__dict__.get("_fields", [])
            if item in fields:
                return True
            return item.lower() in {f.lower() for f in fields}
        return super().__contains__(item)

    def __getattr__(self, name):
        try:
            idx = self.__dict__["_fields"].index(name)
            return self[idx]
        except (KeyError, ValueError):
            raise AttributeError(name)

    def asDict(self):
        return dict(zip(self.__dict__.get("_fields", []), self))

    def __eq__(self, other):
        # Allow direct comparison to dicts/mappings in tests.
        from collections.abc import Mapping

        if isinstance(other, Mapping):
            return self.asDict() == dict(other)
        return super().__eq__(other)

    # Make Row behave like a mapping for test helpers that expect dict-like rows.
    def keys(self):
        return list(self.__dict__.get("_fields", []))

    def items(self):
        fields = self.__dict__.get("_fields", [])
        return [(name, self[i]) for i, name in enumerate(fields)]

    def values(self):
        return list(self)

    def __iter__(self):
        # Iterate over keys so that set(Row) and dict(Row) behave mapping-like in tests.
        return iter(self.__dict__.get("_fields", []))


__all__ = [
    "StructType",
    "StructField",
    "StringType",
    "CharType",
    "VarcharType",
    "IntervalType",
    "IntegerType",
    "LongType",
    "DoubleType",
    "FloatType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "ArrayType",
    "MapType",
    "DecimalType",
    "Row",
    "DataType",
]
