"""Python providers for Sparkless external catalog reads and table writes.

Attach a provider with ``spark.set_external_catalog_provider(provider)``. Its
``resolve_ownership(identifier)`` method returns ``None`` for local identifiers or
an ``ExternalCatalogOwnership`` for identifiers owned by the provider. The separate
``resolve_table(identifier)`` method returns an ExternalTableDescriptor or None.
Writable providers implement ``write_table_batches(request)`` to consume bounded
PyArrow RecordBatches on demand. Callbacks are synchronous and run with the Python GIL.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterator, List, Optional, Protocol, Tuple, Union

from sparkless.errors import SparklessExternalCatalogError
from sparkless.sql import types as T

__all__ = [
    "ExternalCatalogOwnership",
    "ExternalCatalogProvider",
    "ArrowIpcStream",
    "ExternalTableFormat",
    "ExternalTableSaveMode",
    "ExternalTableDescriptor",
    "ExternalTableWriteRequest",
    "ExternalTableWriteResult",
    "BatchWritableExternalCatalogProvider",
    "SparklessExternalCatalogError",
]


class ExternalTableFormat(str, Enum):
    """Storage formats supported by the native external catalog contract."""

    DELTA = "delta"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ExternalCatalogOwnership:
    """Owner/capabilities for an identifier; return None when the local catalog owns it."""

    owner_id: str
    read: bool = True
    write: bool = False
    batch_write: bool = False


class ExternalTableSaveMode(str, Enum):
    """Save modes accepted by DataFrameWriter.mode()."""

    ERROR_IF_EXISTS = "errorifexists"
    OVERWRITE = "overwrite"
    APPEND = "append"
    IGNORE = "ignore"

    def __str__(self) -> str:
        return self.value


class ArrowIpcStream(bytes):
    """Arrow IPC stream bytes, compatible with Python's buffer protocol.

    This is a bytes subclass so existing ``pyarrow.ipc.open_stream(data)`` calls
    keep working. ``data.bytes`` exposes the Rust payload's named field as well.
    """

    @property
    def bytes(self) -> bytes:
        return bytes(self)


@dataclass
class ExternalTableDescriptor:
    """Catalog identity, logical schema, and optional pinned Delta version."""

    table_id: str
    canonical_location: str
    schema: T.StructType
    provider: str
    version: Optional[int] = None
    permissions_context: Optional[str] = None
    format: Union[ExternalTableFormat, str] = ExternalTableFormat.DELTA

    def validate(self) -> None:
        """Run the native descriptor validator without reading storage."""
        from sparkless_native._native import _validate_external_table_descriptor

        _validate_external_table_descriptor(self)


@dataclass
class ExternalTableWriteRequest:
    """A resolved write using a lazy sequence of bounded Arrow batches.

    ``batches`` is a lazy iterator of PyArrow RecordBatches. Closing or exhausting
    it releases the producer; a callback may stop early by closing it. Options are ordered pairs
    so duplicate keys survive. A successful callback must commit the data it
    accepts before returning its receipt. A callback failure after it starts may have an
    unknown commit outcome and is retained as such by the native layer.
    """

    operation_id: str
    identifier: str
    table: ExternalTableDescriptor
    mode: Union[ExternalTableSaveMode, str]
    format: Optional[str]
    options: List[Tuple[str, str]]
    partition_by: List[str]
    batches: Iterator[Any]


@dataclass
class ExternalTableWriteResult:
    """Descriptor and provider-specific identity of a completed commit."""

    table: ExternalTableDescriptor
    committed_version: Optional[int] = None
    provider_commit_id: Optional[str] = None

    def validate_against(self, previous: ExternalTableDescriptor) -> None:
        """Run the native identity and committed-version receipt checks."""
        from sparkless_native._native import _validate_external_table_write_result

        _validate_external_table_write_result(self, previous)


class ExternalCatalogProvider(Protocol):
    """Python catalog callback surface used to build separate Rust capabilities.

    ``resolve_ownership`` is called before table lookup. Return None only when Sparkless'
    local catalog should handle the identifier. Writable providers implement
    ``write_table_batches``; read-only objects may omit it.
    """

    def resolve_ownership(
        self, identifier: str
    ) -> Optional[ExternalCatalogOwnership]: ...

    def resolve_table(self, identifier: str) -> Optional[ExternalTableDescriptor]: ...


class BatchWritableExternalCatalogProvider(ExternalCatalogProvider, Protocol):
    """Provider interface that consumes bounded PyArrow RecordBatches lazily."""

    def write_table_batches(
        self, request: ExternalTableWriteRequest
    ) -> ExternalTableWriteResult: ...


# Use the engine's schema wire format rather than StructType.jsonValue(), whose
# PySpark compatibility rules can normalize IntegerType and LongType together.
_PRIMITIVES = {
    T.StringType: "String",
    T.IntegerType: "Integer",
    T.ShortType: "Short",
    T.LongType: "Long",
    T.FloatType: "Float",
    T.DoubleType: "Double",
    T.BooleanType: "Boolean",
    T.DateType: "Date",
    T.TimestampType: "Timestamp",
    T.TimestampNTZType: "TimestampNtz",
    T.BinaryType: "Binary",
}


def _field_to_wire(field):
    if not isinstance(field, T.StructField):
        raise TypeError("external catalog schema fields must be StructField objects")
    return {
        "name": field.name,
        "data_type": _type_to_wire(field.dataType),
        "nullable": field.nullable,
    }


def _type_to_wire(dtype):
    primitive = _PRIMITIVES.get(type(dtype))
    if primitive is not None:
        return primitive
    if isinstance(dtype, T.DecimalType):
        return {"Decimal": [dtype.precision, dtype.scale]}
    if isinstance(dtype, T.ArrayType):
        return {"Array": _type_to_wire(dtype.elementType)}
    if isinstance(dtype, T.MapType):
        return {"Map": [_type_to_wire(dtype.keyType), _type_to_wire(dtype.valueType)]}
    if isinstance(dtype, T.StructType):
        return {"Struct": [_field_to_wire(field) for field in dtype.fields]}
    raise TypeError(
        "unsupported external catalog schema type: %s" % type(dtype).__name__
    )


def _field_from_wire(field):
    return T.StructField(
        field["name"], _type_from_wire(field["data_type"]), field["nullable"]
    )


def _type_from_wire(value):
    if isinstance(value, str):
        for cls, name in _PRIMITIVES.items():
            if value == name:
                return cls()
    elif "Decimal" in value:
        return T.DecimalType(*value["Decimal"])
    elif "Array" in value:
        return T.ArrayType(_type_from_wire(value["Array"]))
    elif "Map" in value:
        return T.MapType(*[_type_from_wire(item) for item in value["Map"]])
    elif "Struct" in value:
        return T.StructType([_field_from_wire(field) for field in value["Struct"]])
    raise ValueError("unsupported external catalog schema wire type: %r" % (value,))


def _timestamp_timezone_free_type(data_type, arrow_type, pa):
    """Match PySpark's timezone-free Arrow encoding for Python timestamp schemas."""
    if isinstance(data_type, (T.TimestampType, T.TimestampNTZType)):
        if pa.types.is_timestamp(arrow_type) and arrow_type.tz is not None:
            return pa.timestamp(arrow_type.unit)
        return arrow_type
    if isinstance(data_type, T.ArrayType) and pa.types.is_list(arrow_type):
        value_field = arrow_type.value_field
        value_type = _timestamp_timezone_free_type(
            data_type.elementType, value_field.type, pa
        )
        return pa.list_(
            pa.field(
                value_field.name,
                value_type,
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    if isinstance(data_type, T.ArrayType) and pa.types.is_large_list(arrow_type):
        value_field = arrow_type.value_field
        value_type = _timestamp_timezone_free_type(
            data_type.elementType, value_field.type, pa
        )
        return pa.large_list(
            pa.field(
                value_field.name,
                value_type,
                nullable=value_field.nullable,
                metadata=value_field.metadata,
            )
        )
    if isinstance(data_type, T.StructType) and pa.types.is_struct(arrow_type):
        fields = []
        for field in arrow_type:
            try:
                declared = data_type[field.name].dataType
            except KeyError:
                fields.append(field)
                continue
            child_type = _timestamp_timezone_free_type(declared, field.type, pa)
            fields.append(
                pa.field(
                    field.name,
                    child_type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        return pa.struct(fields)
    if isinstance(data_type, T.MapType) and pa.types.is_map(arrow_type):
        key_type = _timestamp_timezone_free_type(
            data_type.keyType, arrow_type.key_type, pa
        )
        item_type = _timestamp_timezone_free_type(
            data_type.valueType, arrow_type.item_type, pa
        )
        return pa.map_(key_type, item_type, keys_sorted=arrow_type.keys_sorted)
    return arrow_type


def _normalize_python_provider_timestamp_ipc(payload, table):
    """Keep Python provider Arrow timestamps consistent with PySpark's naive datetimes."""
    if not any(
        _timestamp_schema_contains(field.dataType) for field in table.schema.fields
    ):
        return payload
    try:
        import pyarrow as pa
    except ImportError:
        return payload

    reader = pa.ipc.open_stream(payload)
    source = reader.read_all()
    arrays = []
    fields = []
    changed = False
    for arrow_field in source.schema:
        try:
            declared = table.schema[arrow_field.name].dataType
        except KeyError:
            declared = None
        array = source.column(arrow_field.name)
        arrow_type = (
            _timestamp_timezone_free_type(declared, arrow_field.type, pa)
            if declared is not None
            else arrow_field.type
        )
        if arrow_type != arrow_field.type:
            array = array.cast(arrow_type, safe=True)
            changed = True
        arrays.append(array)
        fields.append(
            pa.field(
                arrow_field.name,
                arrow_type,
                nullable=arrow_field.nullable,
                metadata=arrow_field.metadata,
            )
        )
    if not changed:
        return payload

    schema = pa.schema(fields, metadata=source.schema.metadata)
    normalized = pa.Table.from_arrays(arrays, schema=schema)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema) as writer:
        if normalized.num_rows == 0:
            # write_table emits no record batches for an empty table. Keep one
            # schema-bearing batch so batch providers still receive the input
            # schema for empty writes.
            writer.write_batch(
                pa.RecordBatch.from_arrays(
                    [array.combine_chunks() for array in arrays], schema=schema
                )
            )
        else:
            writer.write_table(normalized)
    return sink.getvalue().to_pybytes()


def _timestamp_schema_contains(data_type):
    if isinstance(data_type, (T.TimestampType, T.TimestampNTZType)):
        return True
    if isinstance(data_type, T.ArrayType):
        return _timestamp_schema_contains(data_type.elementType)
    if isinstance(data_type, T.MapType):
        return _timestamp_schema_contains(
            data_type.keyType
        ) or _timestamp_schema_contains(data_type.valueType)
    if isinstance(data_type, T.StructType):
        return any(
            _timestamp_schema_contains(field.dataType) for field in data_type.fields
        )
    return False


def _descriptor_to_json(table):
    if not isinstance(table, ExternalTableDescriptor):
        raise TypeError("provider must return ExternalTableDescriptor or None")
    if not isinstance(table.schema, T.StructType):
        raise TypeError("external catalog schema must be a StructType")
    return json.dumps(
        {
            "table_id": table.table_id,
            "canonical_location": table.canonical_location,
            "schema": {
                "fields": [_field_to_wire(field) for field in table.schema.fields]
            },
            "version": table.version,
            "provider": table.provider,
            "permissions_context": table.permissions_context,
            "format": table.format,
        }
    )


def _descriptor_from_json(value):
    value = json.loads(value)
    value["format"] = ExternalTableFormat(value["format"])
    value["schema"] = T.StructType(
        [_field_from_wire(field) for field in value["schema"]["fields"]]
    )
    return ExternalTableDescriptor(**value)


def _write_result_to_parts(result):
    if not isinstance(result, ExternalTableWriteResult):
        raise TypeError(
            "provider.write_table_batches must return ExternalTableWriteResult"
        )
    if result.committed_version is not None and type(result.committed_version) is not int:
        raise TypeError("committed_version must be an integer or None")
    if result.provider_commit_id is not None and not isinstance(
        result.provider_commit_id, str
    ):
        raise TypeError("provider_commit_id must be a string or None")
    if result.committed_version is None and result.provider_commit_id is None:
        raise ValueError("write receipt must include a version or provider_commit_id")
    return (
        _descriptor_to_json(result.table),
        result.committed_version,
        result.provider_commit_id,
    )


def _ownership_to_json(ownership):
    if ownership is None:
        return json.dumps({"kind": "local"})
    if not isinstance(ownership, ExternalCatalogOwnership):
        raise TypeError(
            "resolve_ownership must return ExternalCatalogOwnership or None"
        )
    if not isinstance(ownership.owner_id, str) or not ownership.owner_id.strip():
        raise ValueError("external owner_id must not be empty")
    for capability in (ownership.read, ownership.write, ownership.batch_write):
        if not isinstance(capability, bool):
            raise TypeError("external ownership capabilities must be bool values")
    if ownership.batch_write != ownership.write:
        raise ValueError("external writes require batch_write=True")
    return json.dumps(
        {
            "kind": "external",
            "owner_id": ownership.owner_id,
            "capabilities": {
                "read": ownership.read,
                "write": ownership.write,
                "batch_write": ownership.batch_write,
            },
        }
    )
