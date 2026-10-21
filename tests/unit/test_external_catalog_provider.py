"""Python providers exercise the same native catalog paths as Rust providers."""

from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
import gc
import weakref
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from sparkless import SparklessCancelledError, SparkSession
from sparkless.external_catalog import (
    ArrowIpcStream,
    ExternalTableDescriptor,
    ExternalTableFormat,
    ExternalTableSaveMode,
    ExternalTableWriteRequest,
    ExternalTableWriteResult,
    _descriptor_from_json,
    _descriptor_to_json,
)
from sparkless.sql import types as T

pytestmark = pytest.mark.sparkless_only
IDENTIFIER = "catalog.schema.orders"


@pytest.fixture
def session():
    spark = SparkSession("python-external-catalog")
    yield spark
    spark.stop()


class Provider:
    def __init__(self, table):
        self.table = table
        self.lookups = []
        self.writes = []

    def resolve_table(self, identifier):
        self.lookups.append(identifier)
        return self.table if identifier == IDENTIFIER else None

    def write_table(self, request):
        self.writes.append(request)
        self.table = replace(request.table, version=(request.table.version or 0) + 1)
        return ExternalTableWriteResult(self.table, self.table.version)


def descriptor(schema, location="/unused"):
    return ExternalTableDescriptor(
        table_id="python-table",
        canonical_location=str(location),
        schema=schema,
        provider="python-test",
        version=0,
        permissions_context="caller-context",
    )


def test_python_writer_passes_typed_ipc_and_exact_options(session):
    pa = pytest.importorskip("pyarrow")
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), False),
            T.StructField("amount", T.DecimalType(12, 2)),
            T.StructField("day", T.DateType()),
            T.StructField("instant", T.TimestampType()),
            T.StructField("payload", T.BinaryType()),
            T.StructField("items", T.ArrayType(T.LongType())),
            T.StructField(
                "details", T.StructType([T.StructField("label", T.StringType())])
            ),
        ]
    )
    frame = session.createDataFrame(
        [
            (
                2**54 + 3,
                Decimal("12.34"),
                date(2026, 10, 20),
                datetime(2026, 10, 20, 12, 34, 56),
                b"\x00\xff",
                [1, 2],
                ("nested",),
            ),
            (4, None, None, None, None, None, (None,)),
        ],
        schema,
    )
    provider = Provider(descriptor(schema))
    session.set_external_catalog_provider(provider)
    (
        frame.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .option("mergeSchema", "true")
        .partitionBy("ID")
        .saveAsTable(IDENTIFIER)
    )

    assert provider.lookups == [IDENTIFIER]
    request = provider.writes[0]
    assert isinstance(request, ExternalTableWriteRequest)
    assert request.identifier == IDENTIFIER
    assert request.mode == "append"
    assert request.format == "delta"
    assert request.options == [("mergeSchema", "false"), ("mergeSchema", "true")]
    assert request.partition_by == ["id"]
    assert request.table.table_id == "python-table"
    assert request.table.permissions_context == "caller-context"
    assert type(request.table.schema["id"].dataType) is T.LongType
    assert request.table.schema["id"].nullable is False
    assert isinstance(request.data, bytes)
    assert isinstance(request.data, ArrowIpcStream)
    assert request.data.bytes == bytes(request.data)
    assert request.mode is ExternalTableSaveMode.APPEND
    assert request.table.format is ExternalTableFormat.DELTA
    table = pa.ipc.open_stream(request.data).read_all()
    assert table["id"].to_pylist() == [2**54 + 3, 4]
    assert table["amount"].type == pa.decimal128(12, 2)
    assert table["amount"].to_pylist() == [Decimal("12.34"), None]
    assert table["day"].to_pylist() == [date(2026, 10, 20), None]
    assert table["instant"].to_pylist() == [datetime(2026, 10, 20, 12, 34, 56), None]
    assert table["payload"].to_pylist() == [b"\x00\xff", None]
    assert table["items"].to_pylist() == [[1, 2], None]
    assert table["details"].to_pylist() == [{"label": "nested"}, {"label": None}]


def test_python_batch_writer_receives_bounded_record_batches(session):
    pytest.importorskip("pyarrow")
    schema = T.StructType([T.StructField("id", T.LongType(), False)])
    table = descriptor(schema)

    class BatchProvider:
        received = None

        def resolve_table(self, identifier):
            return table

        def write_table_batches(self, request):
            assert request.data is None
            self.received = list(request.batches)
            updated = replace(request.table, version=1)
            return ExternalTableWriteResult(updated, 1)

    provider = BatchProvider()
    session.set_external_catalog_provider(provider)
    frame = session.createDataFrame([(row,) for row in range(7)], schema)
    (
        frame.write.mode("append")
        .option("sparkless.writer.maxRowsPerBatch", "3")
        .option("sparkless.writer.maxBytesPerBatch", "1024")
        .saveAsTable(IDENTIFIER)
    )

    assert [batch.num_rows for batch in provider.received] == [3, 3, 1]
    assert all(batch.nbytes <= 1024 for batch in provider.received)
    assert [batch.column(0).to_pylist() for batch in provider.received] == [
        [0, 1, 2],
        [3, 4, 5],
        [6],
    ]


@pytest.mark.parametrize("ingress", ["python_rows", "pyarrow"])
def test_external_writer_preserves_null_struct_parent_validity(session, ingress):
    pa = pytest.importorskip("pyarrow")
    nested = T.StructType([T.StructField("value", T.StringType())])
    details = T.StructType(
        [T.StructField("label", T.StringType()), T.StructField("inner", nested)]
    )
    item = T.StructType(
        [T.StructField("label", T.StringType()), T.StructField("inner", nested)]
    )
    schema = T.StructType(
        [T.StructField("details", details), T.StructField("items", T.ArrayType(item))]
    )
    expected = [
        {
            "details": {"label": "present", "inner": {"value": "deep"}},
            "items": [
                {"label": "item", "inner": {"value": "nested"}},
                None,
                {"label": None, "inner": None},
                {"label": None, "inner": {"value": None}},
            ],
        },
        {
            "details": None,
            "items": [None, {"label": None, "inner": {"value": None}}],
        },
        {"details": {"label": None, "inner": None}, "items": None},
    ]
    if ingress == "pyarrow":
        arrow_schema = pa.schema(
            [
                (
                    "details",
                    pa.struct(
                        [
                            ("label", pa.string()),
                            ("inner", pa.struct([("value", pa.string())])),
                        ]
                    ),
                ),
                (
                    "items",
                    pa.list_(
                        pa.struct(
                            [
                                ("label", pa.string()),
                                ("inner", pa.struct([("value", pa.string())])),
                            ]
                        )
                    ),
                ),
            ]
        )
        data = pa.Table.from_pylist(expected, schema=arrow_schema)
    else:
        data = [(row["details"], row["items"]) for row in expected]

    class CaptureThenFail:
        received = None

        def resolve_table(self, identifier):
            return descriptor(schema)

        def write_table(self, request):
            self.received = pa.ipc.open_stream(request.data).read_all().to_pylist()
            raise RuntimeError("intentional capture stop before commit")

    provider = CaptureThenFail()
    session.set_external_catalog_provider(provider)
    frame = session.createDataFrame(data, schema)
    with pytest.raises(Exception, match="intentional capture stop before commit"):
        frame.write.mode("append").saveAsTable(IDENTIFIER)

    assert provider.received == expected


def test_schema_bridge_preserves_recursive_types_and_integer_widths():
    schema = T.StructType(
        [
            T.StructField("int", T.IntegerType(), False),
            T.StructField("long", T.LongType()),
            T.StructField("short", T.ShortType()),
            T.StructField(
                "nested",
                T.ArrayType(
                    T.MapType(
                        T.StringType(),
                        T.StructType(
                            [
                                T.StructField("decimal", T.DecimalType(18, 4), False),
                                T.StructField("boolean", T.BooleanType()),
                                T.StructField("double", T.DoubleType()),
                            ]
                        ),
                    )
                ),
            ),
        ]
    )
    table = _descriptor_from_json(_descriptor_to_json(descriptor(schema)))
    assert type(table.schema["int"].dataType) is T.IntegerType
    assert type(table.schema["long"].dataType) is T.LongType
    assert type(table.schema["short"].dataType) is T.ShortType
    assert table.schema["int"].nullable is False
    nested = table.schema["nested"].dataType.elementType.valueType
    assert nested["decimal"].dataType.precision == 18
    assert nested["decimal"].dataType.scale == 4
    assert nested["decimal"].nullable is False
    assert type(nested["boolean"].dataType) is T.BooleanType
    assert type(nested["double"].dataType) is T.DoubleType


@pytest.mark.parametrize(
    "mode,expected_calls", [("ignore", 0), ("error", 0), ("overwrite", 1)]
)
def test_python_provider_save_modes(session, mode, expected_calls):
    frame = session.createDataFrame([(1,)], ["id"])
    provider = Provider(descriptor(frame.schema))
    session.setExternalCatalogProvider(provider)
    if mode == "error":
        with pytest.raises(Exception, match="already exists"):
            frame.write.mode(mode).saveAsTable(IDENTIFIER)
    else:
        frame.write.mode(mode).saveAsTable(IDENTIFIER)
    assert len(provider.writes) == expected_calls
    if expected_calls:
        assert provider.writes[0].mode == mode


def test_python_provider_errors_are_surfaced_without_local_write(session):
    frame = session.createDataFrame([(1,)], ["id"])

    class Failing(Provider):
        def resolve_table(self, identifier):
            self.lookups.append(identifier)
            raise LookupError("catalog unavailable")

    provider = Failing(descriptor(frame.schema))
    session.set_external_catalog_provider(provider)
    for read in (session.table, session.read_delta, session.read_delta_with_version):
        with pytest.raises(Exception, match="catalog unavailable"):
            read(IDENTIFIER)
    assert provider.lookups == [IDENTIFIER] * 3
    with pytest.raises(Exception, match="catalog unavailable"):
        frame.write.mode("overwrite").saveAsTable(IDENTIFIER)
    session.set_external_catalog_provider(None)
    with pytest.raises(Exception, match="not found"):
        session.table(IDENTIFIER)


def test_read_only_provider_and_invalid_receipts_fail_explicitly(session):
    frame = session.createDataFrame([(1,)], ["id"])
    table = descriptor(frame.schema)

    class ReadOnly:
        def resolve_table(self, identifier):
            return table

    session.set_external_catalog_provider(ReadOnly())
    with pytest.raises(Exception, match="does not support table writes"):
        frame.write.mode("append").saveAsTable(IDENTIFIER)

    class InvalidReceipt(Provider):
        def write_table(self, request):
            return ExternalTableWriteResult(replace(request.table, version=9), 10)

    session.set_external_catalog_provider(InvalidReceipt(table))
    with pytest.raises(Exception, match="committed version"):
        frame.write.mode("append").saveAsTable(IDENTIFIER)
    session.set_external_catalog_provider(None)
    with pytest.raises(Exception, match="not found"):
        session.table(IDENTIFIER)


@pytest.mark.parametrize(
    "failure", ["exception", "missing", "identity", "boolean_version"]
)
def test_python_write_failures_do_not_fall_back_to_local(session, failure):
    frame = session.createDataFrame([(1,)], ["id"])

    class InvalidWrite(Provider):
        def write_table(self, request):
            if failure == "exception":
                raise RuntimeError("native commit failed")
            if failure == "missing":
                return None
            if failure == "identity":
                return ExternalTableWriteResult(
                    replace(request.table, table_id="different", version=1), 1
                )
            return ExternalTableWriteResult(replace(request.table, version=1), True)

    session.set_external_catalog_provider(InvalidWrite(descriptor(frame.schema)))
    with pytest.raises(
        Exception, match="commit failed|ExternalTableWriteResult|identity|integer"
    ):
        frame.write.mode("append").saveAsTable(IDENTIFIER)
    session.set_external_catalog_provider(None)
    with pytest.raises(Exception, match="not found"):
        session.table(IDENTIFIER)


@pytest.mark.parametrize("result", [{}, "bad descriptor", False])
def test_invalid_python_descriptors_do_not_become_misses(session, result):
    class Invalid:
        def resolve_table(self, identifier):
            return result

    session.set_external_catalog_provider(Invalid())
    with pytest.raises(Exception, match="ExternalTableDescriptor"):
        session.table(IDENTIFIER)


def test_callback_lifetime_new_session_and_removal(session):
    frame = session.createDataFrame([(1,)], ["id"])
    provider = Provider(descriptor(frame.schema))
    reference = weakref.ref(provider)
    session.set_external_catalog_provider(provider)
    del provider
    gc.collect()
    assert reference() is not None
    child = session.newSession()
    try:
        with pytest.raises(Exception, match="not found"):
            child.table(IDENTIFIER)
        assert reference().lookups == []
    finally:
        child.stop()
    session.set_external_catalog_provider(None)
    gc.collect()
    assert reference() is None


def test_stop_releases_python_callback(session):
    provider = Provider(descriptor(T.StructType()))
    reference = weakref.ref(provider)
    session.set_external_catalog_provider(provider)
    del provider
    session.stop()
    gc.collect()
    assert reference() is None


def test_detaching_callback_allows_finalizer_to_reenter_session(session):
    finalized = []

    class FinalizingProvider(Provider):
        def __del__(self):
            session.set_external_catalog_provider(None)
            finalized.append(True)

    provider = FinalizingProvider(descriptor(T.StructType()))
    session.set_external_catalog_provider(provider)
    del provider
    session.set_external_catalog_provider(None)
    assert finalized == [True]


@pytest.mark.delta
def test_python_provider_native_reads_and_committed_writes(session, tmp_path):
    pa = pytest.importorskip("pyarrow")
    path = str(tmp_path / "orders")
    seed = session.createDataFrame([(1, "first")], ["id", "name"])
    seed.write.format("delta").mode("overwrite").save(path)

    class CommittingProvider(Provider):
        def write_table(self, request):
            table = pa.ipc.open_stream(request.data).read_all()
            session.createDataFrame(table).write.format("delta").mode(
                request.mode
            ).save(path)
            return super().write_table(request)

    provider = CommittingProvider(descriptor(seed.schema, path))
    session.set_external_catalog_provider(provider)
    reader = session.read
    assert session.table(IDENTIFIER).collect()[0]["name"] == "first"
    assert reader.table(IDENTIFIER).count() == 1
    assert session.sql(f"SELECT id FROM {IDENTIFIER}").collect()[0]["id"] == 1
    session.sql(f"INSERT INTO {IDENTIFIER} VALUES (2, 'second')")
    assert provider.writes[-1].mode == "append"
    assert session.table(IDENTIFIER).count() == 2
    assert session.read.format("delta").load(path).count() == 2
    session.createDataFrame([(3, "third")], ["id", "name"]).write.mode(
        "append"
    ).saveAsTable(IDENTIFIER)
    assert session.table(IDENTIFIER).count() == 3
    assert provider.table.version == 2
    session.set_external_catalog_provider(None)
    with pytest.raises(Exception, match="not found"):
        reader.table(IDENTIFIER)


@pytest.mark.delta
def test_python_provider_miss_and_local_precedence(session, tmp_path):
    path = str(tmp_path / "orders")
    external = session.createDataFrame([(1,)], ["id"])
    external.write.format("delta").mode("overwrite").save(path)
    provider = Provider(descriptor(external.schema, path))
    session.set_external_catalog_provider(provider)
    local = session.createDataFrame([(99,)], ["id"])
    local.createOrReplaceTempView(IDENTIFIER)
    assert session.table(IDENTIFIER).collect()[0]["id"] == 99
    assert provider.lookups == []
    local.createOrReplaceTempView("orders")
    with pytest.raises(Exception, match="not found"):
        session.table("another.schema.orders")
    session.catalog.dropTempView(IDENTIFIER)
    assert session.table(IDENTIFIER).collect()[0]["id"] == 1


@pytest.mark.delta
def test_python_sql_local_map_view_shadows_cached_external_columns(session, tmp_path):
    path = str(tmp_path / "external_columns")
    external_schema = T.StructType([T.StructField("other", T.StringType())])
    session.createDataFrame([("external",)], external_schema).write.format(
        "delta"
    ).mode("overwrite").save(path)
    provider = Provider(descriptor(external_schema, path))
    session.set_external_catalog_provider(provider)

    # Populate the descriptor cache before creating a local view with this identifier.
    assert session.table(IDENTIFIER).collect()[0]["other"] == "external"
    local_schema = T.StructType(
        [T.StructField("attrs", T.MapType(T.StringType(), T.LongType()))]
    )
    session.createDataFrame([({"local": 9},)], local_schema).createOrReplaceTempView(
        IDENTIFIER
    )

    result = session.sql(f"SELECT * FROM {IDENTIFIER}")
    assert type(result.schema["attrs"].dataType) is T.MapType
    assert result.collect()[0]["attrs"] == {"local": 9}


def test_python_catalog_existence_checks_surface_provider_errors(session):
    class UnavailableProvider:
        def resolve_table(self, identifier):
            raise RuntimeError("catalog service unavailable")

    session.set_external_catalog_provider(UnavailableProvider())
    with pytest.raises(Exception, match="catalog service unavailable"):
        session.catalog.tableExists(IDENTIFIER)
    with pytest.raises(Exception, match="catalog service unavailable"):
        session.catalog.getTable(IDENTIFIER)


@pytest.mark.delta
def test_python_catalog_retains_map_metadata_for_reads_and_rejects_map_writes(
    session, tmp_path
):
    path = str(tmp_path / "maps")
    schema = T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("attrs", T.MapType(T.StringType(), T.LongType())),
            T.StructField(
                "nested", T.ArrayType(T.MapType(T.StringType(), T.LongType()))
            ),
        ]
    )
    seed = session.createDataFrame([(1, {"key": 3}, [{"nested": 4}])], schema)
    seed.write.format("delta").mode("overwrite").save(path)
    provider = Provider(descriptor(schema, path))
    session.set_external_catalog_provider(provider)
    for frame in (
        session.table(IDENTIFIER),
        session.read.table(IDENTIFIER),
        session.sql(f"SELECT * FROM {IDENTIFIER}"),
        session.read_delta(IDENTIFIER),
    ):
        assert type(frame.schema["attrs"].dataType) is T.MapType
        assert type(frame.schema["nested"].dataType.elementType) is T.MapType
        row = frame.collect()[0]
        assert row["attrs"] == {"key": 3}
        assert row["nested"] == [{"nested": 4}]
    with pytest.raises(Exception, match="logical Map"):
        session.table(IDENTIFIER).write.mode("append").saveAsTable(IDENTIFIER)
    with pytest.raises(Exception, match="logical Map"):
        session.sql(f"INSERT INTO {IDENTIFIER} SELECT * FROM {IDENTIFIER}")
    assert provider.writes == []


@pytest.mark.delta
def test_python_catalog_time_travel_uses_snapshot_map_metadata(session, tmp_path):
    path = str(tmp_path / "evolving_maps")
    map_schema = T.StructType(
        [T.StructField("attrs", T.MapType(T.StringType(), T.LongType()))]
    )
    initial = session.createDataFrame([({"old": 7},)], map_schema)
    initial.write.format("delta").mode("overwrite").save(path)

    latest_schema = T.StructType([T.StructField("attrs", T.StringType())])
    latest = session.createDataFrame([("current",)], latest_schema)
    latest.write.format("delta").option("overwriteSchema", "true").mode(
        "overwrite"
    ).save(path)
    provider = Provider(replace(descriptor(latest_schema, path), version=1))
    session.set_external_catalog_provider(provider)

    historical = session.read_delta_with_version(IDENTIFIER, 0)
    assert type(historical.schema["attrs"].dataType) is T.MapType
    assert historical.collect()[0]["attrs"] == {"old": 7}

    current = session.read_delta_with_version(IDENTIFIER, 1)
    assert type(current.schema["attrs"].dataType) is T.StringType
    assert current.collect()[0]["attrs"] == "current"


@pytest.mark.delta
def test_python_provider_map_metadata_handles_quoted_sql_and_single_lookup(
    session, tmp_path
):
    path = str(tmp_path / "maps")
    schema = T.StructType(
        [T.StructField("attrs", T.MapType(T.StringType(), T.LongType()))]
    )
    seed = session.createDataFrame([({"key": 7},)], schema)
    seed.write.format("delta").mode("overwrite").save(path)
    table = descriptor(schema, path)

    class OneLookupProvider(Provider):
        def resolve_table(self, identifier):
            self.lookups.append(identifier)
            if len(self.lookups) > 1:
                raise RuntimeError("unexpected second lookup")
            return self.table if identifier == IDENTIFIER else None

    provider = OneLookupProvider(table)
    session.set_external_catalog_provider(provider)
    quoted = "SELECT * FROM `catalog`.`schema`.`orders`"
    frame = session.sql(quoted)
    assert type(frame.schema["attrs"].dataType) is T.MapType
    assert frame.collect()[0]["attrs"] == {"key": 7}
    frame.write.format("delta").mode("overwrite").save(str(tmp_path / "copy"))
    copied = session.read.format("delta").load(str(tmp_path / "copy"))
    assert type(copied.schema["attrs"].dataType) is T.MapType
    assert provider.lookups == [IDENTIFIER]


@pytest.mark.delta
def test_python_provider_file_url_is_confined_after_url_normalization(
    session, tmp_path, monkeypatch
):
    base = tmp_path / "base"
    nested = base / "nested" / "orders"
    flattened = base / "orders"
    base.mkdir()
    schema = T.StructType([T.StructField("id", T.LongType())])
    session.createDataFrame([(99,)], schema).write.format("delta").save(str(nested))
    session.createDataFrame([(123,)], schema).write.format("delta").save(str(flattened))
    provider = Provider(descriptor(schema, nested.as_uri()))
    session.set_external_catalog_provider(provider)
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))

    assert session.table(IDENTIFIER).collect()[0]["id"] == 99


def test_public_arrow_stream_supports_bytes_and_buffer_consumers():
    payload = ArrowIpcStream(b"stream-bytes")
    assert payload.bytes == b"stream-bytes"
    assert memoryview(payload).tobytes() == b"stream-bytes"


@pytest.mark.parametrize("selected_format", [None, "delta", "parquet", "csv", "json"])
def test_python_writer_reaches_every_native_format(session, selected_format):
    frame = session.createDataFrame([(1,)], ["id"])
    provider = Provider(descriptor(frame.schema))
    session.set_external_catalog_provider(provider)
    writer = frame.write.mode(ExternalTableSaveMode.APPEND)
    if selected_format is not None:
        writer = writer.format(selected_format)
    writer.save_as_table(IDENTIFIER)
    assert provider.writes[0].format == selected_format


def test_python_bulk_options_retain_order_and_duplicate_keys(session):
    frame = session.createDataFrame([(1,)], ["id"])
    provider = Provider(descriptor(frame.schema))
    session.set_external_catalog_provider(provider)
    frame.write.mode("append").options(
        [("mergeSchema", False), ("mergeSchema", True)], overwriteSchema=False
    ).option("batchSize", 100).saveAsTable(IDENTIFIER)
    assert provider.writes[0].options == [
        ("mergeSchema", "false"),
        ("mergeSchema", "true"),
        ("overwriteSchema", "false"),
        ("batchSize", "100"),
    ]
    frame.write.mode("append").options({"first": 1}, second=2).saveAsTable(IDENTIFIER)
    assert provider.writes[1].options == [("first", "1"), ("second", "2")]


@pytest.mark.parametrize("mode", list(ExternalTableSaveMode))
def test_public_save_mode_enums_work_with_python_writer(session, mode):
    frame = session.createDataFrame([(1,)], ["id"])
    provider = Provider(descriptor(frame.schema))
    session.set_external_catalog_provider(provider)
    writer = frame.write.format(ExternalTableFormat.DELTA).mode(mode)
    if mode is ExternalTableSaveMode.ERROR_IF_EXISTS:
        with pytest.raises(Exception, match="already exists"):
            writer.saveAsTable(IDENTIFIER)
        assert provider.writes == []
    else:
        writer.saveAsTable(IDENTIFIER)
        if mode is ExternalTableSaveMode.IGNORE:
            assert provider.writes == []
        else:
            request = provider.writes[0]
            assert request.mode is mode
            assert request.format == "delta"
            assert str(request.mode) == mode.value


def test_public_validators_accept_all_native_schema_types():
    schema = T.StructType(
        [
            T.StructField("string", T.StringType()),
            T.StructField("integer", T.IntegerType(), False),
            T.StructField("short", T.ShortType()),
            T.StructField("long", T.LongType()),
            T.StructField("double", T.DoubleType()),
            T.StructField("decimal", T.DecimalType(18, 3)),
            T.StructField("boolean", T.BooleanType()),
            T.StructField("date", T.DateType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("binary", T.BinaryType()),
            T.StructField("array", T.ArrayType(T.LongType())),
            T.StructField("map", T.MapType(T.StringType(), T.LongType())),
            T.StructField(
                "struct", T.StructType([T.StructField("child", T.LongType())])
            ),
        ]
    )
    table = descriptor(schema)
    assert table.validate() is None
    receipt = ExternalTableWriteResult(replace(table, version=1), 1)
    assert receipt.validate_against(table) is None


@pytest.mark.parametrize(
    "change,match",
    [
        ({"table_id": " "}, "table_id"),
        ({"canonical_location": ""}, "canonical_location"),
        ({"provider": ""}, "provider"),
        ({"version": -1}, "negative"),
        ({"format": "parquet"}, "variant"),
    ],
)
def test_python_descriptor_validate_uses_native_rules(change, match):
    table = replace(descriptor(T.StructType()), **change)
    with pytest.raises(Exception, match=match):
        table.validate()


@pytest.mark.parametrize(
    "change,committed_version,match",
    [
        ({"table_id": "other"}, 5, "identity"),
        ({"canonical_location": "/other"}, 5, "identity"),
        ({"provider": "other"}, 5, "identity"),
        ({"version": None}, 5, "committed version"),
        ({"version": 6}, 5, "committed version"),
        ({"version": -1}, -1, "negative"),
        ({"version": 3}, 3, "backwards"),
    ],
)
def test_python_receipt_validate_against_uses_native_rules(
    change, committed_version, match
):
    previous = replace(descriptor(T.StructType()), version=4)
    receipt_table = replace(previous, version=5)
    receipt_table = replace(receipt_table, **change)
    receipt = ExternalTableWriteResult(receipt_table, committed_version)
    with pytest.raises(Exception, match=match):
        receipt.validate_against(previous)


@pytest.mark.parametrize("source_kind", ["explicit_widths", "arrow"])
def test_python_ipc_preserves_remaining_native_primitive_types(session, source_kind):
    pa = pytest.importorskip("pyarrow")
    schema = T.StructType(
        [
            T.StructField("short", T.ShortType()),
            T.StructField("integer", T.IntegerType()),
            T.StructField("double", T.DoubleType()),
            T.StructField("boolean", T.BooleanType()),
            T.StructField("string", T.StringType()),
        ]
    )
    values = [(123, 2147483647, 1.25, True, "café"), (None,) * 5]
    if source_kind == "arrow":
        arrow_schema = pa.schema(
            [
                ("short", pa.int16()),
                ("integer", pa.int32()),
                ("double", pa.float64()),
                ("boolean", pa.bool_()),
                ("string", pa.string()),
            ]
        )
        rows = [dict(zip(schema.fieldNames(), row)) for row in values]
        frame = session.createDataFrame(pa.Table.from_pylist(rows, schema=arrow_schema))
    else:
        frame = session.createDataFrame(
            values,
            [
                ("short", "smallint"),
                ("integer", "int32"),
                ("double", "double"),
                ("boolean", "boolean"),
                ("string", "str"),
            ],
        )
    provider = Provider(descriptor(schema))
    session.set_external_catalog_provider(provider)
    frame.write.mode(ExternalTableSaveMode.APPEND).saveAsTable(IDENTIFIER)
    table = pa.ipc.open_stream(provider.writes[0].data.bytes).read_all()
    assert table.schema.types[:4] == [
        pa.int16(),
        pa.int32(),
        pa.float64(),
        pa.bool_(),
    ]
    assert table.to_pylist() == [
        {
            "short": 123,
            "integer": 2147483647,
            "double": 1.25,
            "boolean": True,
            "string": "café",
        },
        {
            "short": None,
            "integer": None,
            "double": None,
            "boolean": None,
            "string": None,
        },
    ]


def test_saved_local_table_shadows_python_provider_for_reads_and_writes(session):
    frame = session.createDataFrame([(99,)], ["id"])
    frame.write.saveAsTable(IDENTIFIER)
    provider = Provider(descriptor(frame.schema))
    session.set_external_catalog_provider(provider)
    frame.write.mode(ExternalTableSaveMode.APPEND).saveAsTable(IDENTIFIER)
    assert session.table(IDENTIFIER).count() == 2
    assert session.read_delta_with_version(IDENTIFIER, 0).count() == 2
    assert session.catalog.tableExists(IDENTIFIER)
    assert provider.lookups == []
    assert provider.writes == []


@pytest.mark.delta
def test_python_reaches_pinned_latest_and_explicit_snapshot_reads(session, tmp_path):
    path = str(tmp_path / "versions")
    seed = session.createDataFrame([(1,)], ["id"])
    seed.write.format("delta").mode("overwrite").save(path)
    for value in (2, 3):
        session.createDataFrame([(value,)], ["id"]).write.format("delta").mode(
            "append"
        ).save(path)
    provider = Provider(descriptor(seed.schema, path))
    session.set_external_catalog_provider(provider)
    assert session.table(IDENTIFIER).count() == 1
    assert session.read.table(IDENTIFIER).count() == 1
    assert session.sql(f"SELECT * FROM {IDENTIFIER}").count() == 1
    assert session.read_delta(IDENTIFIER).count() == 1
    assert session.read_delta_with_version(IDENTIFIER).count() == 1
    assert session.read_delta_with_version(IDENTIFIER, 1).count() == 2
    provider.table = replace(provider.table, version=None)
    assert session.table(IDENTIFIER).count() == 3
    assert session.read_delta_with_version(IDENTIFIER, 0).count() == 1
    local = session.createDataFrame([(99,)], ["id"])
    local.createOrReplaceTempView(IDENTIFIER)
    lookups = list(provider.lookups)
    assert session.read_delta_with_version(IDENTIFIER, 0).collect()[0]["id"] == 99
    assert provider.lookups == lookups
    local.createOrReplaceGlobalTempView("python_catalog_versions")
    try:
        assert (
            session.read_delta_with_version(
                "global_temp.python_catalog_versions", 0
            ).collect()[0]["id"]
            == 99
        )
        assert provider.lookups == lookups
    finally:
        session.catalog.dropGlobalTempView("python_catalog_versions")


@pytest.mark.delta
def test_python_catalog_exists_create_and_select_insert_and_overwrite(
    session, tmp_path
):
    pa = pytest.importorskip("pyarrow")
    path = str(tmp_path / "catalog")
    seed = session.createDataFrame([(1,)], ["id"])
    seed.write.format("delta").mode("overwrite").save(path)

    class Committing(Provider):
        def write_table(self, request):
            table = pa.ipc.open_stream(request.data.bytes).read_all()
            session.createDataFrame(table).write.format(ExternalTableFormat.DELTA).mode(
                request.mode
            ).save(path)
            return super().write_table(request)

    provider = Committing(descriptor(seed.schema, path))
    session.set_external_catalog_provider(provider)
    assert session.catalog.tableExists(IDENTIFIER)
    assert not session.catalog.tableExists("catalog.schema.absent")
    session.sql(f"CREATE TABLE IF NOT EXISTS {IDENTIFIER} (id BIGINT)")
    assert session.table(IDENTIFIER).count() == 1
    with pytest.raises(Exception, match="already exists"):
        session.sql(f"CREATE TABLE {IDENTIFIER} (id BIGINT)")
    session.createDataFrame([(2,)], ["id"]).createOrReplaceTempView(
        "python_catalog_source"
    )
    session.sql(f"INSERT INTO {IDENTIFIER} SELECT id FROM python_catalog_source")
    assert provider.writes[0].mode is ExternalTableSaveMode.APPEND
    assert session.table(IDENTIFIER).count() == 2
    session.createDataFrame([(7,)], ["id"]).write.format(
        ExternalTableFormat.DELTA
    ).mode(ExternalTableSaveMode.OVERWRITE).saveAsTable(IDENTIFIER)
    assert provider.writes[1].mode is ExternalTableSaveMode.OVERWRITE
    assert session.table(IDENTIFIER).collect()[0]["id"] == 7
    assert session.read_delta_with_version(IDENTIFIER, 0).collect()[0]["id"] == 1


@pytest.mark.delta
def test_python_catalog_strict_delta_and_descriptor_schema_checks(session, tmp_path):
    path = tmp_path / "parquet"
    path.mkdir()
    seed = session.createDataFrame([(1,)], ["id"])
    seed.write.parquet(str(path / "part.parquet"))
    provider = Provider(replace(descriptor(seed.schema, path), version=None))
    session.set_external_catalog_provider(provider)
    assert session.read.format("delta").load(str(path)).count() == 1
    with pytest.raises(Exception):
        session.table(IDENTIFIER)
    delta_path = str(tmp_path / "delta")
    seed.write.format("delta").mode("overwrite").save(delta_path)
    provider.table = descriptor(
        T.StructType([T.StructField("wrong", T.LongType())]), delta_path
    )
    with pytest.raises(Exception, match="schema mismatch"):
        session.table(IDENTIFIER)


@pytest.mark.delta
def test_python_catalog_uses_native_path_confinement(session, tmp_path, monkeypatch):
    path = str(tmp_path / "outside")
    seed = session.createDataFrame([(1,)], ["id"])
    seed.write.format("delta").mode("overwrite").save(path)
    session.set_external_catalog_provider(Provider(descriptor(seed.schema, path)))
    assert session.table(IDENTIFIER).count() == 1
    base = tmp_path / "permitted"
    base.mkdir()
    monkeypatch.setenv("SPARKLESS_FILES_BASE", str(base))
    with pytest.raises(Exception, match="outside|escape|base"):
        session.table(IDENTIFIER)


def test_cancel_during_catalog_resolution_prevents_local_save_as_table():
    spark = SparkSession("cancel-write-regression")
    entered_resolve = Event()
    continue_resolve = Event()

    class Provider:
        def resolve_table(self, identifier):
            entered_resolve.set()
            assert continue_resolve.wait(timeout=5)
            return None

    spark.set_external_catalog_provider(Provider())
    frame = spark.createDataFrame([(1,)], ["id"])
    cancellation = spark.cancellation_handle()
    try:

        def cancel_from_control_thread():
            assert entered_resolve.wait(timeout=5)
            assert cancellation.cancel_all_active_actions() >= 1
            continue_resolve.set()

        with ThreadPoolExecutor(max_workers=1) as pool:
            control = pool.submit(cancel_from_control_thread)
            with pytest.raises(SparklessCancelledError):
                frame.write.format("parquet").saveAsTable("cancelled_local")
            control.result(timeout=5)
        with pytest.raises(Exception):
            spark.table("cancelled_local")
    finally:
        continue_resolve.set()
        spark.stop()


def test_cancellation_handle_is_usable_from_another_thread():
    spark = SparkSession("cancel-handle-thread-regression")
    try:
        handle = spark.cancellation_handle()
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(handle.cancel_all_active_actions).result() == 0
    finally:
        spark.stop()


def test_to_arrow_checks_cancellation_during_python_conversion(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    spark = SparkSession("cancel-arrow-regression")
    frame = spark.createDataFrame([(1,)], ["id"])
    original_array = pa.array
    cancelled = False

    def cancel_during_array(*args, **kwargs):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            frame.cancel()
        return original_array(*args, **kwargs)

    monkeypatch.setattr(pa, "array", cancel_during_array)
    try:
        with pytest.raises(SparklessCancelledError):
            frame.toArrow()
    finally:
        spark.stop()


def test_to_pandas_checks_cancellation_during_python_conversion(monkeypatch):
    pd = pytest.importorskip("pandas")
    spark = SparkSession("cancel-pandas-regression")
    frame = spark.createDataFrame([(1,)], ["id"])
    original_dataframe = pd.DataFrame

    def cancel_during_dataframe(*args, **kwargs):
        frame.cancel()
        return original_dataframe(*args, **kwargs)

    monkeypatch.setattr(pd, "DataFrame", cancel_during_dataframe)
    try:
        with pytest.raises(SparklessCancelledError):
            frame.toPandas()
    finally:
        spark.stop()


def test_public_cancellation_exception_is_exported():
    from sparkless import SparklessCancelledError as top_level_error
    from sparkless.core.exceptions import SparklessCancelledError as core_error

    assert top_level_error is core_error


def test_session_cancellation_reaches_aggregate_dataframes(monkeypatch):
    pa = pytest.importorskip("pyarrow")
    from sparkless.sql import functions as F

    spark = SparkSession("cancel-aggregate-regression")
    frame = spark.createDataFrame([(1, "a"), (2, "a")], ["id", "key"])
    aggregate = frame.groupBy("key").agg(F.sum("id"))
    handle = spark.cancellation_handle()
    original_array = pa.array
    cancelled = False

    def cancel_during_array(*args, **kwargs):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            assert handle.cancel_all_active_actions() >= 1
        return original_array(*args, **kwargs)

    monkeypatch.setattr(pa, "array", cancel_during_array)
    try:
        with pytest.raises(SparklessCancelledError):
            aggregate.toArrow()
    finally:
        spark.stop()


@pytest.mark.parametrize("method", ["head", "take", "first"])
def test_convenience_row_actions_use_source_cancellation_registry(method, monkeypatch):
    spark = SparkSession(f"cancel-{method}-regression")
    frame = spark.createDataFrame([(1,), (2,)], ["id"])
    original_row = T.Row
    cancelled = False

    def cancel_during_row(*args, **kwargs):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            assert frame.cancel() >= 1
        return original_row(*args, **kwargs)

    monkeypatch.setattr(T, "Row", cancel_during_row)
    try:
        with pytest.raises(SparklessCancelledError):
            if method == "head":
                frame.head(2)
            elif method == "take":
                frame.take(2)
            else:
                frame.first()
    finally:
        spark.stop()


@pytest.mark.delta
def test_cancellation_during_sql_delta_metadata_processing_is_reported(tmp_path):
    spark = SparkSession("cancel-sql-metadata-regression")
    seed = spark.createDataFrame([(1,)], ["id"])
    path = str(tmp_path / "source")
    seed.write.format("delta").mode("overwrite").save(path)
    handle = spark.cancellation_handle()

    class Provider:
        calls = 0

        def resolve_table(self, identifier):
            self.calls += 1
            if self.calls == 2:
                assert handle.cancel_all_active_actions() >= 1
            return descriptor(seed.schema, path)

    provider = Provider()
    spark.set_external_catalog_provider(provider)
    try:
        with pytest.raises(SparklessCancelledError):
            spark.sql("SELECT * FROM (SELECT id FROM external_source) AS q")
        assert provider.calls == 2
    finally:
        spark.stop()


@pytest.mark.parametrize("operation", ["with_column", "filter_compare", "filter_bool"])
def test_session_cancellation_reaches_python_udf_dataframes(operation, monkeypatch):
    pa = pytest.importorskip("pyarrow")
    from sparkless.sql import functions as F

    spark = SparkSession(f"cancel-udf-{operation}-regression")
    source = spark.createDataFrame([(1,), (2,)], ["id"])
    identity = F.udf(lambda value: value, T.LongType())
    positive = F.udf(lambda value: value > 0, T.BooleanType())
    if operation == "with_column":
        frame = source.withColumn("copy", identity("id"))
    elif operation == "filter_compare":
        frame = source.filter(identity("id") > 0)
    else:
        frame = source.filter(positive("id"))

    handle = spark.cancellation_handle()
    original_array = pa.array
    cancelled = False

    def cancel_during_array(*args, **kwargs):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            assert handle.cancel_all_active_actions() >= 1
        return original_array(*args, **kwargs)

    monkeypatch.setattr(pa, "array", cancel_during_array)
    try:
        with pytest.raises(SparklessCancelledError):
            frame.toArrow()
    finally:
        spark.stop()


def test_save_as_table_registers_with_active_target_session():
    origin = SparkSession("cancel-writer-origin-session")
    frame = origin.createDataFrame([(1,)], ["id"])
    target = origin.newSession()
    handle = target.cancellation_handle()

    class Provider:
        def resolve_table(self, identifier):
            assert handle.cancel_all_active_actions() == 1
            return None

    target.set_external_catalog_provider(Provider())
    try:
        with pytest.raises(SparklessCancelledError):
            frame.write.mode("overwrite").saveAsTable("cancelled_in_target")
        with pytest.raises(Exception):
            target.table("cancelled_in_target")
    finally:
        target.stop()
        origin.stop()


def test_empty_collect_checks_cancellation_after_schema_conversion(monkeypatch):
    spark = SparkSession("cancel-empty-collect-regression")
    frame = spark.range(0)
    original_struct_type = T.StructType
    cancelled = False

    def cancel_during_schema(*args, **kwargs):
        nonlocal cancelled
        if not cancelled:
            cancelled = True
            assert frame.cancel() == 1
        return original_struct_type(*args, **kwargs)

    monkeypatch.setattr(T, "StructType", cancel_during_schema)
    try:
        with pytest.raises(SparklessCancelledError):
            frame.collect()
    finally:
        spark.stop()
