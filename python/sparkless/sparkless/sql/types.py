# PySpark-style types for schema and Row. Used by createDataFrame(schema=StructType(...)).


class DataType:
    def simpleString(self) -> str:
        return "string"


class StringType(DataType):
    def simpleString(self) -> str:
        return "string"


class CharType(DataType):
    """Char(n) type; we treat as string for schema parsing."""

    def __init__(self, length: int = 1):
        self.length = length

    def simpleString(self) -> str:
        return "string"


class VarcharType(DataType):
    """Varchar(n) type; we treat as string for schema parsing."""

    def __init__(self, length: int = 1):
        self.length = length

    def simpleString(self) -> str:
        return "string"


class IntervalType(DataType):
    """Interval type; we treat as string for schema parsing."""

    def __init__(self, start_field: str = "month", end_field: str = "month"):
        self.start_field = start_field
        self.end_field = end_field

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
    def __init__(
        self,
        elementType=None,
        containsNull=True,
        *,
        element_type=None,
        nullable=None,
    ):
        if elementType is not None and element_type is not None:
            raise TypeError("Cannot specify both elementType and element_type")
        elem = elementType if elementType is not None else element_type
        if elem is None:
            raise TypeError("elementType or element_type is required")
        self.elementType = elem
        self.containsNull = containsNull if nullable is None else nullable
        # PySpark alias: nullable as property
        self.nullable = self.containsNull

    @property
    def element_type(self):
        """PySpark parity: element type of array (alias for elementType)."""
        return self.elementType

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

    @property
    def key_type(self):
        """PySpark parity: key type of map (alias for keyType)."""
        return self.keyType

    @property
    def value_type(self):
        """PySpark parity: value type of map (alias for valueType)."""
        return self.valueType

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
    def __init__(self, precision: int = 10, scale: int = 0):
        self.precision = precision
        self.scale = scale

    def simpleString(self) -> str:
        try:
            return f"decimal({self.precision},{self.scale})"
        except AttributeError:
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

    def fieldNames(self):
        """PySpark parity: returns all field names in a list."""
        return [f.name for f in self.fields]

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
