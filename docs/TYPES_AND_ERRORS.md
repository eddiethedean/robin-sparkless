# Types, schemas, and errors

Contract: local Sparkless 4.19.0. For the narrower remote type set, use the
[Connect matrix](SPARK_CONNECT.md) and [HTTP result schema](HTTP_API.md).

## Explicit schema first

Import `types as T` from `sparkless.sql`. Build `T.StructType` from ordered
`T.StructField(name, dataType, nullable=True)` fields. Common scalar types are
StringType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
DoubleType, BinaryType, DateType, TimestampType, and DecimalType. Arrays/maps/
structs have nested element/key/value/field types; use explicit schemas for empty
and null-only input. See [generated type signatures](PYTHON_API_SYMBOLS.md).

Column names alone request inference, not validation of a stable application
schema. Dictionary input order and numeric widening can differ from Spark.
Validate `df.schema` after ingest/casts, especially decimal precision/scale,
nullable fields, timestamps/timezones, nested values, and integer boundaries.
A nullable declaration does not replace your application's data-quality checks.
Test invalid input behavior and round trips through your chosen storage format.

`Row` supports field-name and positional access. Do not assume iteration yields
values: explicitly use `row[field_name]` or indexed access. Results without an
explicit ordering can arrive in any order. Compare schemas as well as rows when
checking parity. `toPandas`/Arrow conversions require their optional libraries;
precision/timezone/conversion semantics must be tested at those boundaries.

## Error boundaries

| Boundary | Public signal / handling |
| --- | --- |
| Python argument/type validation | TypeError/ValueError; fix input or schema |
| Native execution | `sparkless.SparklessError`; inspect cause/message |
| Cooperative cancellation | `sparkless.SparklessCancelledError`; wait for action termination |
| External provider | `SparklessExternalCatalogError` with category, code, operation, retryability and safe context; follow provider contract |
| Unsupported behavior | Explicit error, NotImplementedError, or absent API; consult support matrix |
| Server startup/control | `sparkless_connect.SparklessServerError`; include bounded diagnostics |
| HTTP | Check HTTP status **and** JSON `status`; SQL errors can use HTTP200 |
| Connect | gRPC status; client/session/operation identity constraints apply |

Rust `EngineError` is a structured type rather than an enum. Branch on `category()`
and `code()`; inspect `detail()` for operation, retryability, metadata and cause.
Python exceptions expose the corresponding `error_detail` object when the lower
boundary has structured error information. HTTP engine failures include the same
detail object in the response. Connect maps categories to gRPC status codes and
includes the stable engine code in the status message.

Exact error wording and classes can differ from PySpark. Avoid matching complete
messages as an application protocol. Use structured categories and codes where
available ([embedding](EMBEDDING.md)).

Cancellation is cooperative, not a hard execution deadline. Python UDF callbacks
and native materialization can finish before a cancellation safe point. For
writes, an ambiguous client timeout is not proof of rollback. Read
[cancellation](EXECUTION_CANCELLATION.md), [provider write receipts](EXTERNAL_CATALOGS.md),
and [HTTP mutation recovery](HTTP_API.md) before retrying effects.
