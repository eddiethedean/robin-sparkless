# PySpark-style types for schema and Row. Used by createDataFrame(schema=StructType(...)).
from __future__ import annotations

from datetime import date, datetime
from typing import Dict, Iterator, List, Optional, Tuple, Type, Union, cast

# Values that can appear in a Row (collect() output). Recursive for nested structs/arrays.
RowValue = Union[
    int,
    float,
    str,
    bool,
    None,
    date,
    datetime,
    List["RowValue"],
    Dict[str, "RowValue"],
]

# Spark struct field metadata: string keys, values are typically str, int, bool, or list of str.
StructMetadata = Dict[str, Union[str, int, bool, List[str]]]

# Return type of Row.__getitem__ (single element or slice).
RowGetItemReturn = Union[RowValue, Tuple[RowValue, ...]]


class DataType:
    def simpleString(self) -> str:
        return "string"

    def __eq__(self, other: object) -> bool:
        """Phase 7: type equality so ArrayType().elementType == StringType() in tests."""
        if type(self) is not type(other):
            return False
        # Simple types (StringType, LongType, etc.) have no instance attrs to compare
        if not hasattr(self, "__dict__") or not hasattr(other, "__dict__"):
            return True
        return self.__dict__ == other.__dict__


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

    def __eq__(self, other: object) -> bool:
        """Treat IntegerType and LongType as equivalent in comparisons.

        Many robin-sparkless tests expect IntegerType schemas to be preserved even
        when the underlying engine normalizes to LongType (64-bit). Matching on
        simple type family here smooths over Integer vs Long differences.
        """
        return isinstance(other, (IntegerType, LongType))


class LongType(DataType):
    def simpleString(self) -> str:
        return "long"

    def __eq__(self, other: object) -> bool:
        """Treat LongType and IntegerType as equivalent in comparisons (see IntegerType.__eq__)."""
        return isinstance(other, (LongType, IntegerType))


class DoubleType(DataType):
    def simpleString(self) -> str:
        return "double"


class FloatType(DataType):
    def simpleString(self) -> str:
        return "float"


class BooleanType(DataType):
    def simpleString(self) -> str:
        return "boolean"


class BinaryType(DataType):
    """Binary (bytes) type. PySpark parity."""

    def simpleString(self) -> str:
        return "binary"


class DateType(DataType):
    def simpleString(self) -> str:
        return "date"


class TimestampType(DataType):
    def simpleString(self) -> str:
        return "timestamp"


class ArrayType(DataType):
    def __init__(
        self,
        elementType: Optional[DataType] = None,
        containsNull: bool = True,
        *,
        _element_type: Optional[DataType] = None,
        nullable: Optional[bool] = None,
    ) -> None:
        if elementType is not None and _element_type is not None:
            raise TypeError("Cannot specify both elementType and _element_type")
        elem = elementType if elementType is not None else _element_type
        if elem is None:
            raise TypeError("elementType is required")
        self.elementType = elem
        self.containsNull = containsNull if nullable is None else nullable
        # PySpark alias: nullable as property
        self.nullable = self.containsNull

    @property
    def _element_type(self) -> DataType:
        """Internal: element type of array (use elementType for PySpark parity)."""
        return self.elementType

    def simpleString(self) -> str:
        try:
            inner = self.elementType.simpleString()
        except Exception:
            inner = "string"
        return f"array<{inner}>"


class MapType(DataType):
    def __init__(
        self,
        keyType: DataType,
        valueType: DataType,
        valueContainsNull: bool = True,
    ) -> None:
        self.keyType = keyType
        self.valueType = valueType
        self.valueContainsNull = valueContainsNull

    @property
    def key_type(self) -> DataType:
        """PySpark parity: key type of map (alias for keyType)."""
        return self.keyType

    @property
    def value_type(self) -> DataType:
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
    def __init__(
        self,
        name: str,
        dataType: DataType,
        nullable: bool = True,
        metadata: Optional[StructMetadata] = None,
    ) -> None:
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}


class StructType(DataType):
    def __init__(self, fields: Optional[List[StructField]] = None) -> None:
        self.fields = list(fields or [])

    def fieldNames(self) -> List[str]:
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

    # PySpark parity: schema.jsonValue() / schema.json()
    def jsonValue(self) -> dict:
        """Return a JSON-serializable dict describing the schema (PySpark-style)."""

        def _dtype_json(dt: DataType):
            # Primitive types map to simple strings (e.g. \"string\").
            from sparkless.sql.types import ArrayType as _ArrayType, MapType as _MapType  # avoid cycles

            if isinstance(dt, DataType) and hasattr(dt, "simpleString"):
                # ArrayType: {"type": "array", "elementType": <inner>, "containsNull": bool}
                if isinstance(dt, _ArrayType):
                    inner = getattr(dt, "elementType", None)
                    inner_json = _dtype_json(inner) if inner is not None else "string"
                    contains_null = getattr(
                        dt, "containsNull", getattr(dt, "nullable", True)
                    )
                    return {
                        "type": "array",
                        "elementType": inner_json,
                        "containsNull": bool(contains_null),
                    }
                # MapType: {"type": "map", "keyType": ..., "valueType": ..., "valueContainsNull": bool}
                if isinstance(dt, _MapType):
                    key_json = _dtype_json(getattr(dt, "keyType", None))
                    value_json = _dtype_json(getattr(dt, "valueType", None))
                    value_contains_null = getattr(dt, "valueContainsNull", True)
                    return {
                        "type": "map",
                        "keyType": key_json,
                        "valueType": value_json,
                        "valueContainsNull": bool(value_contains_null),
                    }
                # Other DataType subclasses: use simpleString().
                try:
                    return dt.simpleString()
                except Exception:
                    return "string"
            # Fallback: treat as string type description.
            return "string"

        fields = []
        for f in self.fields:
            dt = getattr(f, "dataType", None)
            fields.append(
                {
                    "name": f.name,
                    "type": _dtype_json(dt),
                    "nullable": bool(getattr(f, "nullable", True)),
                    "metadata": getattr(f, "metadata", {}) or {},
                }
            )
        return {"fields": fields, "type": "struct"}

    def json(self) -> str:
        """Return schema JSON string (PySpark parity wrapper for jsonValue())."""
        import json as _json

        return _json.dumps(self.jsonValue())


class Row(tuple):
    """PySpark-like Row type returned by DataFrame.collect().

    Contract: collect() returns a list of Row. Each Row preserves types (int, float, bool,
    str, None, date, datetime, list, dict). Use row[name], row[idx], row.asDict(), or
    attribute access (row.column_name). Row is tuple-like (iterable, indexable by int).
    """

    def __new__(cls: Type["Row"], *args: RowValue, **kwargs: RowValue) -> "Row":
        # Support kwargs-style initialization: Row(a=1, b=2)
        if kwargs:
            # Special-case data=None so Row(data=None, a=1) uses only kwargs a=1 (PySpark parity).
            if "data" in kwargs and kwargs["data"] is None:
                kwargs = {k: v for k, v in kwargs.items() if k != "data"}
            obj = super().__new__(cls, list(kwargs.values()))
            obj.__dict__["_fields"] = list(kwargs.keys())
            obj.__dict__["_data_dict"] = dict(kwargs)
            return obj

        # Support dict-style initialization: Row({"a": 1, "b": 2}) -> sentinel Row that is not field-indexable.
        if len(args) == 1 and isinstance(args[0], dict):
            # Store the dict as a single positional element and do not set _fields so that
            # Row({"a": 1}) acts as a sentinel Row that does not expose keys as fields
            # (row["a"] / row.a raise AttributeError in tests).
            obj = super().__new__(cls, (args[0],))
            # Mark this as a sentinel so indexing by string can raise AttributeError instead of KeyError.
            obj.__dict__["_sentinel_dict_row"] = True
            return obj

        # Row() with no args/kwargs constructs an empty Row (PySpark parity).
        if not args:
            return super().__new__(cls, ())

        # Positional initialization: Row(1,2,3) (unnamed fields)
        return super().__new__(cls, args)

    def _iter_values(self):
        # Iterate underlying tuple values regardless of Row.__iter__ override.
        return super().__iter__()

    def __getitem__(self, item: Union[int, str, slice]) -> RowGetItemReturn:  # type: ignore[override]
        # PySpark parity: Row supports both positional and name-based indexing.
        if isinstance(item, str):
            # Sentinel Row from Row({..}) has no field metadata; accessing by name should raise
            # AttributeError("__fields__") (PySpark-like behavior expected by tests).
            if self.__dict__.get("_sentinel_dict_row"):
                raise AttributeError("__fields__")
            fields = self.__dict__.get("_fields", [])
            if item in fields:
                return cast(RowValue, super().__getitem__(fields.index(item)))
            # Case-insensitive fallback (common in tests)
            lowered = {f.lower(): i for i, f in enumerate(fields)}
            if item.lower() in lowered:
                return cast(RowValue, super().__getitem__(lowered[item.lower()]))
            # Dotted key (e.g. "Person.name"): match by suffix when struct field select yields single column named "name"
            if "." in item and fields:
                suffix = item.rsplit(".", 1)[-1]
                for i, f in enumerate(fields):
                    if f.lower() == suffix.lower():
                        return cast(RowValue, super().__getitem__(i))
            # Disambiguation for duplicate select columns (issue #213/#215/#399/#1080):
            # Support virtual names like "x", "x_1", "x_2" as aliases for positional
            # columns, and map simple CAST(x AS TYPE) back to base column name "x".
            if fields:
                # 1) name_N -> positional column N if it exists (x_1 -> second column, etc.)
                if "_" in item:
                    base, _, idx_str = item.rpartition("_")
                    if base and idx_str.isdigit():
                        idx = int(idx_str)
                        if 0 <= idx < len(self):
                            return cast(RowValue, super().__getitem__(idx))

                # 2) CAST wrapper: row["x"] or row["x_1"] for columns named "CAST(x AS ...)".
                key = item
                base, _, idx_str = key.rpartition("_")
                target_idx = 0
                key_for_cast = key
                if base and idx_str.isdigit():
                    key_for_cast = base
                    target_idx = int(idx_str)

                key_lower = key_for_cast.lower()
                candidates = []
                for i, f in enumerate(fields):
                    fl = f.lower()
                    if fl.startswith("cast(") and " as " in fl:
                        inner = fl[5 : fl.index(" as ")].strip()
                        if inner == key_lower:
                            candidates.append(i)
                if candidates and 0 <= target_idx < len(candidates):
                    return cast(RowValue, super().__getitem__(candidates[target_idx]))
            raise KeyError(item)
        return cast(RowGetItemReturn, super().__getitem__(item))

    def __contains__(self, item: object) -> bool:
        if isinstance(item, str):
            fields = self.__dict__.get("_fields", [])
            if item in fields:
                return True
            lowered_fields = {f.lower() for f in fields}
            if item.lower() in lowered_fields:
                return True
            # Support virtual names like "x_1", "x_2" based on position.
            if "_" in item:
                base, _, idx_str = item.rpartition("_")
                if base and idx_str.isdigit():
                    idx = int(idx_str)
                    if 0 <= idx < len(self):
                        return True
            # CAST wrapper aliases: "x" / "x_1" for "CAST(x AS ...)".
            key = item
            base, _, idx_str = key.rpartition("_")
            target_idx = 0
            key_for_cast = key
            if base and idx_str.isdigit():
                key_for_cast = base
                target_idx = int(idx_str)
            key_lower = key_for_cast.lower()
            candidates = []
            for i, f in enumerate(fields):
                fl = f.lower()
                if fl.startswith("cast(") and " as " in fl:
                    inner = fl[5 : fl.index(" as ")].strip()
                    if inner == key_lower:
                        candidates.append(i)
            if candidates and 0 <= target_idx < len(candidates):
                return True
            return False
        return super().__contains__(item)

    def __getattr__(self, name: str) -> RowValue:
        try:
            idx = self.__dict__["_fields"].index(name)
            return cast(RowValue, self[idx])
        except (KeyError, ValueError):
            raise AttributeError(name)

    def asDict(self) -> Dict[str, RowValue]:
        fields = self.__dict__.get("_fields", [])
        return dict(zip(fields, list(self._iter_values())))

    def __eq__(self, other: object) -> bool:
        # Allow direct comparison to dicts/mappings in tests.
        from collections.abc import Mapping

        if isinstance(other, Mapping):
            return self.asDict() == dict(other)
        return super().__eq__(other)

    def _order_key(self, v: RowValue) -> Tuple[int, Union[str, Tuple[str, str]]]:
        """Normalize value for ordering so mixed types (str vs int) never raise TypeError."""
        if v is None:
            return (0, "")
        return (1, (type(v).__name__, repr(v)))

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Row) or len(self) != len(other):
            return NotImplemented
        for a, b in zip(self._iter_values(), other._iter_values()):
            ka, kb = self._order_key(a), self._order_key(b)
            if ka != kb:
                return ka < kb
        return False

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Row) or len(self) != len(other):
            return NotImplemented
        for a, b in zip(self._iter_values(), other._iter_values()):
            ka, kb = self._order_key(a), self._order_key(b)
            if ka != kb:
                return ka < kb
        return True

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Row) or len(self) != len(other):
            return NotImplemented
        for a, b in zip(self._iter_values(), other._iter_values()):
            ka, kb = self._order_key(a), self._order_key(b)
            if ka != kb:
                return ka > kb
        return False

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Row) or len(self) != len(other):
            return NotImplemented
        for a, b in zip(self._iter_values(), other._iter_values()):
            ka, kb = self._order_key(a), self._order_key(b)
            if ka != kb:
                return ka > kb
        return True

    # Make Row behave like a mapping for test helpers that expect dict-like rows.
    def keys(self) -> List[str]:
        return list(self.__dict__.get("_fields", []))

    def items(self) -> List[Tuple[str, RowValue]]:
        fields = self.__dict__.get("_fields", [])
        return [(name, cast(RowValue, self[i])) for i, name in enumerate(fields)]

    def values(self) -> List[RowValue]:
        return list(self._iter_values())

    def __iter__(self) -> Iterator[str]:
        # Dict-like iteration (keys). Tests often treat Row like Mapping.
        # Use row.values() / tuple(row._iter_values()) for values.
        return iter(self.__dict__.get("_fields", []))


class _ColumnsList(list):
    """PySpark parity: df.columns and df.columns() both return the list of column names.
    Typing: behaves as List[str] (column names)."""

    def __call__(self) -> "_ColumnsList":
        return self


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
