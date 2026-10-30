# Python API contracts — 4.18.0

This page covers the most-used installed local APIs. The generated
[symbol/signature reference](PYTHON_API_SYMBOLS.md) inventories the public runtime
surface, including native method documentation. A signature is not a guarantee
of full PySpark equivalence; consult the [support matrix](SUPPORT_MATRIX.md).
All examples use `from sparkless.sql import SparkSession, functions as F, types as T`.

## Session and schema

| API | Parameters/defaults | Returns / behavior |
| --- | --- | --- |
| `SparkSession.builder.app_name(name).get_or_create()` | `name`: application label; builder is a property, not callable | Local session; stop it in `finally` |
| `createDataFrame(data, schema=None, **kwargs)` | Rows/dicts/tuples; names or explicit `T.StructType`; inference when omitted | DataFrame; explicit schema recommended for null/empty/mixed inputs |
| `range(start, end=None, step=None)` | One argument means 0..start; end exclusive; default step 1 | DataFrame with long `id`; row cap applies |
| `sql(query)` | SQL string; SQL feature required | Lazy query DataFrame; supported DDL/DML may mutate session/catalog |
| `table(name)` | Temp/saved/warehouse name | DataFrame; missing name raises an error |
| `spark.read`, `spark.catalog` | Properties | Reader/catalog; do not call `catalog()` |
| `spark.conf` | Runtime configuration interface | Read supported keys; not all Spark keys have engine effects |
| `stop()` | None | Signals active actions and clears session state |

```python
from sparkless.sql import SparkSession, types as T
spark = SparkSession.builder.app_name("schema-example").get_or_create()
try:
    schema = T.StructType([T.StructField("id", T.IntegerType(), True)])
    df = spark.createDataFrame([(1,), (None,)], schema)
    assert df.count() == 2
finally:
    spark.stop()
```

## Transformations

Transformations return a new DataFrame. Most are lazy; Python UDF boundaries can
materialize. Invalid names/types can fail either during planning or an action.

| API | Parameters/defaults | Result / caveat |
| --- | --- | --- |
| `select(*cols)` | Column names/expressions; alias expressions explicitly | Projection; use `selectExpr` for SQL strings |
| `filter(condition)` / `where(condition)` | Boolean Column or supported SQL predicate | Matching rows; null predicate is not true |
| `withColumn(name, col)` | Output name and Column | Adds/replaces a column |
| `join(other, on=None, how='inner', left_on=None, right_on=None)` | Other DataFrame; name/list or supported condition; optional distinct left/right keys | Joined DataFrame; alias ambiguous fields; no key may imply cross join and hit row limits |
| `groupBy(*cols)` | Names/expressions, or none for global aggregation | GroupedData; `agg(*exprs)` returns DataFrame |
| `orderBy(*cols, ascending=None)` | Names/expressions; default ascending; bool/list overrides | Sorted DataFrame; add tie-breakers for deterministic order |
| `unionByName(other, **kwargs)` | `allowMissingColumns=True` permits absent fields | Concatenation by name; validate types/schema |
| `limit(n)` | Nonnegative count | At most n rows; unordered input has no stable selection |
| `createOrReplaceTempView(name)` | Session-local name | Registration side effect; query through same session |

```python
from sparkless.sql import SparkSession, functions as F
spark = SparkSession.builder.app_name("join-example").get_or_create()
try:
    orders = spark.createDataFrame([(1, 10), (1, 20)], ["customer_id", "amount"])
    customers = spark.createDataFrame([(1, "East")], ["customer_id", "region"])
    result = orders.join(customers, on="customer_id", how="inner").groupBy("region").agg(F.sum("amount").alias("total"))
    assert result.collect()[0]["total"] == 30
finally:
    spark.stop()
```

Common join forms: inner, left/right/full outer, left semi, left anti, and cross.
See [join differences](PYSPARK_DIFFERENCES.md#join) for semantics and limits.
GroupedData supports `count`, `sum`, `avg`, `min`, `max`, `agg`, and `pivot`;
aggregate return schema depends on expression aliases and input types.

## Actions and Arrow

| API | Returns | Limits / dependencies |
| --- | --- | --- |
| `collect()` | List of Rows, supporting name/position access | Materializes complete result |
| `count()` | Integer row count | Executes the plan |
| `show(n=20, truncate=True)` | Prints formatted rows | Inspection only; not a data export |
| `toPandas()` | pandas DataFrame | Requires pandas; complete materialization |
| `toArrow()` | PyArrow Table | Requires PyArrow; complete materialization |
| `toArrowBatches(max_rows=10000, max_bytes=67108864)` | Context-managed RecordBatch iterator | Positive per-batch bounds; oversized row raises ValueError |

```python
from sparkless.sql import SparkSession
spark = SparkSession.builder.app_name("arrow").get_or_create()
try:
    with spark.range(100).toArrowBatches(max_rows=16, max_bytes=1048576) as batches:
        assert sum(batch.num_rows for batch in batches) == 100
finally:
    spark.stop()
```

Close batch iterators on early exit. Row/byte bounds apply to each output batch,
not query intermediate memory or process RSS. See [cancellation](EXECUTION_CANCELLATION.md).

## Readers, writers, and catalog

`spark.read.format(fmt).option(key, value).load(path)` returns a DataFrame.
Shortcuts `csv(path)`, `json(path)`, `parquet(path)` select the format. `options(opts)`
takes a dictionary in this API. A reader `schema(...)` method is not part of the
current installed reader surface; cast columns or use explicit createDataFrame
schemas instead. Paths are local filesystem paths unless a specific connector
supports another scheme. Format options are a subset of Spark's.

`df.write` is a property. Chain `format(fmt)`, `mode(mode)`, `option(key,value)`,
and supported `partitionBy(*cols)`, then call `save(path)` or a format shortcut.
Writing is an action with filesystem effects. Save modes: `error`/`errorifexists`
(default), `overwrite`, `append`, `ignore`. Existing destinations, incompatible
schemas, unsupported formats/options, or IO failures raise errors. `overwrite`
can replace data; isolate output directories.

`saveAsTable(name, mode=None)` uses the configured warehouse when present;
otherwise saved tables are process/session scoped. See [persistence](PERSISTENCE_GUIDE.md).
`catalog.listTables(dbName=None)` returns names, not PySpark Table objects;
`tableExists(name)` returns bool; `dropTempView(name)` and `dropTable(name)` remove
registrations (do not assume all external data is deleted).

`read.jdbc(url, table, properties=None)` and
`write.jdbc(url, table, properties, mode=None)` require compiled drivers. Python
extras do not enable them. See [features](INSTALLATION.md) and [JDBC](JDBC_TESTING.md).

## Functions, types, providers, and launchers

`F.col(name)` references a field; `F.lit(value)` creates a literal. Strings passed
to many functions mean column names, not literal strings. `F.when(condition,value)`
builds a conditional; aggregates such as `F.sum("amount")` build expressions for
`agg`/`select`. Window functions need `from sparkless.sql.window import Window`
and `.over(window)`. See generated signatures for the full current namespace.

`F.udf(f=None, returnType=None)` returns a scalar callable; use explicit DataType
objects and handle nulls. [UDF guide](UDF_GUIDE.md) defines supported contexts.
[Types and errors](TYPES_AND_ERRORS.md) covers schema and conversion boundaries.
[External catalogs](EXTERNAL_CATALOGS.md) defines provider callbacks and receipts.

`SparklessServer(host='127.0.0.1', port=0, executable_path=None,
startup_timeout=30.0, shutdown_timeout=10.0, kill_timeout=5.0, env=None,
connect_host='127.0.0.1', connect_port=0)` owns one child process. Import it from
`sparkless.server`; use a `with` block. Zero ports allocate ephemeral listeners;
`connect_port=None` disables Connect. After readiness, `http_url`,
`spark_connect_url`, and `server_instance_id` identify the live endpoints.
`stop()` drains/shuts down and reaps the child. `AsyncSparklessServer` offers
async start/stop/context management. Startup/protocol/ownership failures raise
`SparklessServerError`; disabled Connect access raises `UnsupportedSparkConnectError`.
Neither launcher implements a general SQL client: use [HTTP](HTTP_API.md) or the
supported [Connect client](SPARK_CONNECT.md).

## Errors and compatibility

Catch specific public exceptions at application boundaries, and preserve their
message/cause for diagnostics. Native execution uses `sparkless.SparklessError`,
with `SparklessCancelledError` for cancellation and external-catalog errors for
provider failures. Validation can raise TypeError/ValueError; unavailable APIs
can raise NotImplementedError or be absent. Exact exception classes/messages are
not universally identical to PySpark. See [types/errors](TYPES_AND_ERRORS.md).
