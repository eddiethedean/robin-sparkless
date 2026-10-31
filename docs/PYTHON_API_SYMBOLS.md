# Python symbol reference — 4.18.1

Generated from the installed package by `scripts/generate_documentation.py --api`.
CI regenerates this against its wheel and fails on drift. This is the runtime
symbol/signature inventory; [API contracts](PYTHON_API_REFERENCE.md) supplies
argument semantics, returns, errors and practical examples for common operations.
Missing native docstrings are stated explicitly; importability alone is not
evidence of Spark parity. Consult the [support contract](SUPPORT_MATRIX.md).

## SparkSession

### `app_name`

Property/descriptor; see the API contracts for semantics.

### `backend_type`

Property/descriptor; see the API contracts for semantics.

### `builder`

Property/descriptor; see the API contracts for semantics.

### `cancelAllActiveJobs(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cancel_all_active_actions(self, /)`

Request cancellation for every active action started through this session.
Actions report cancellation only after reaching an engine safe point.

### `cancellationHandle(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cancellation_handle(self, /)`

Return a thread-safe handle that can be passed to a control thread.

### `catalog`

Property/descriptor; see the API contracts for semantics.

### `conf`

Property/descriptor; see the API contracts for semantics.

### `createDataFrame(self, /, data, schema=None, **kwargs)`

PySpark alias: createDataFrame -> create_dataframe_from_rows. Accepts verify_schema or verifySchema, sampling_ratio or samplingRatio.

### `create_dataframe(self, /, data, column_names)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `create_dataframe_from_rows(self, /, data, schema=None, verify_schema=True, sampling_ratio=None)`

createDataFrame(data, schema=None, verify_schema=True, sampling_ratio=None).
data: list/tuple of dicts, list/tuple of row tuples, iterable of rows, or pandas.DataFrame.
schema: optional list of (name, type_str) or list of column names (types inferred).
verify_schema: when True, validate row values against schema types (PySpark parity).
sampling_ratio: accepted for API parity; ignored for list/dict data.

### `create_or_replace_global_temp_view(self, /, name, df)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `create_or_replace_temp_view(self, /, name, df)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `drop_global_temp_view(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `drop_temp_view(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `getActiveSession()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `newSession(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `range(self, /, start, end=None, step=None)`

PySpark-compatible range:
- range(end)
- range(start, end)
- range(start, end, step)

### `read`

Property/descriptor; see the API contracts for semantics.

### `read_csv(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `read_delta(self, /, name_or_path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `read_delta_with_version(self, /, name_or_path, version=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `read_json(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `read_parquet(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `setExternalCatalogProvider(self, /, provider)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `set_external_catalog_provider(self, /, provider)`

Attach a synchronous Python catalog provider, or pass None to remove it.

### `sparkContext`

Property/descriptor; see the API contracts for semantics.

### `sql(self, /, query)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `stop(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `table(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## DataFrame

### `agg(self, /, *exprs)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `alias(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cache(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cancel(self, /)`

Cancel active actions started through this DataFrame object.
A count is returned so callers can tell whether any work was still active.

### `coalesce(self, /, _n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `collect(self, /)`

Collect all rows as a list of Row objects (PySpark parity).

**Contract:** Returns `list[Row]`. Each Row is tuple-like with attribute and name-based
access; types are preserved (int, float, bool, str, None, date, datetime, list, dict).
Use `row.asDict()` for a dict of column name -> value, or iterate for values in order.
String-typed columns are best-effort coerced to int/float/bool when the value looks like
one (e.g. coalesce() results or mixed-type array elements stringified by the engine).

### `columns`

Property/descriptor; see the API contracts for semantics.

### `count(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `createOrReplaceGlobalTempView(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `createOrReplaceTempView(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `create_or_replace_temp_view(self, /, session, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `crossJoin(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `crosstab(self, /, col1, col2)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cube(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `describe(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `distinct(self, /, subset=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `do_order_by(self, /, cols, ascending=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `do_order_by_impl(self, /, cols, ascending=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `drop(self, /, *columns)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dropDuplicates(self, /, subset=None, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `drop_duplicates(self, /, subset=None, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dropna(self, /, subset=None, how=None, thresh=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dtypes`

Property/descriptor; see the API contracts for semantics.

### `exceptAll(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `explain(self, /, extended=None)`

PySpark: explain() prints to stdout and returns None (PySpark parity).

### `fillna(self, /, value, subset=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `filter(self, /, condition)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `first(self, /)`

Return the first row as a Row, or None if empty (Phase 7.8 / PySpark first() semantics).

### `flatMap(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `flat_map(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `freqItems(self, /, columns, support=0.01)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `groupBy(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `group_by(self, /, column_names)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `groupby(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `head(self, /, n=None)`

PySpark parity: head() returns the first Row (or None if empty); head(n) returns a list of Row.

### `intersect(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isEmpty(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `join(self, /, other, on=None, how='inner', left_on=None, right_on=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `limit(self, /, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `melt(self, /, id_vars, value_vars)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `na`

Property/descriptor; see the API contracts for semantics.

### `orderBy(self, /, *cols, ascending=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `order_by(self, /, *cols, ascending=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `order_by_exprs(self, /, exprs)`

order_by_exprs([col("a").asc(), col("b").desc_nulls_last()]) - order by SortOrder expressions.

### `order_by_names(self, /, column_names, ascending)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `persist(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `printSchema(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `print_schema(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rdd`

Property/descriptor; see the API contracts for semantics.

### `repartition(self, /, _n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `replace(self, /, to_replace, *values, subset=None, **kwargs)`

Replace value in columns. PySpark df.replace(to_replace, value=None, subset=...).
Uses *values and **kwargs so we can distinguish "value not provided" from "value is Python None",
and accept value when passed as keyword (e.g. from Python na.replace(to_replace, value=value, subset=...)).

### `rollup(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `sampleBy(self, /, col, fractions, seed=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `schema`

Property/descriptor; see the API contracts for semantics.

### `select(self, *cols)`

PySpark-compatible DataFrame.select wrapper.

PySpark does not accept a bare tuple of column names as the first/only argument:
df.select(("a", "b")) raises PySparkTypeError[NOT_COLUMN_OR_STR]. Lists are accepted.

### `selectExpr(self, /, *exprs)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `show(self, /, n=20, truncate=True)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `sort(self, *cols, ascending=None)`

PySpark-compatible DataFrame.sort wrapper.

PySpark does not accept an 'ascending' keyword argument; use col("x").asc() or .desc().
PySpark treats a bare tuple of columns passed to sort/orderBy as invalid and raises
PySparkTypeError[NOT_COLUMN_OR_STR]. Lists (including df.columns) are accepted.

### `storage`

Property/descriptor; see the API contracts for semantics.

### `subtract(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `summary(self, /)`

PySpark alias for describe().

### `take(self, /, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `toArrow(self, /)`

Export collected values to a PyArrow Table, retaining decimal precision and scale.

### `toArrowBatches(self, /, max_rows=10000, max_bytes=67108864)`

Yield PyArrow RecordBatches without materializing the full DataFrame.

Each returned batch stays within both limits. If one row alone exceeds
max_bytes, iteration raises ValueError. Call close(), or use the iterator
as a context manager, to stop the query when the consumer is done early.

### `toDF(self, /, *col_names)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `toPandas(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_arrow_batches(self, /, max_rows=10000, max_bytes=67108864)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `union(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `unionAll(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `unionByName(self, /, other, **kwargs)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `union_all(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `unpivot(self, /, ids, values, *, variableColumnName=None, valueColumnName=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `where(self, /, condition)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `withColumn(self, /, name, col)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `withColumnRenamed(self, /, old_name, new_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `withColumns(self, /, cols_map)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `withColumnsRenamed(self, /, cols_map)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `with_column(self, /, name, col_any)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `with_columns_renamed(self, /, cols_map)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `write`

Property/descriptor; see the API contracts for semantics.

### `writeTo(self, /, _table)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## Column

### `alias(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `and_(self, /, other)`

Logical AND of two boolean columns. PySpark and_.

### `array_compact(self, /)`

Remove null elements from list (PySpark array_compact).

### `array_distinct(self, /)`

col[key] -> map get or array getItem. Key: int (array index), str (map key), or Column (map key).

### `asc(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `asc_nulls_first(self, /)`

Ascending sort, nulls first. PySpark asc_nulls_first.

### `asc_nulls_last(self, /)`

Ascending sort, nulls last. PySpark asc_nulls_last.

### `asinh(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `astype(self, /, dtype)`

PySpark astype(dtype): alias for cast(dtype).

### `atanh(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `between(self, /, lower, upper)`

True if column value is between lower and upper (inclusive). PySpark between(low, high).
Accepts Column or scalar (int, float, str, bool, None) for lower/upper.

### `cast(self, /, type_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `contains(self, /, literal)`

String column contains substring (PySpark Column.contains).

### `cosh(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `crc32(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dense_rank(self, /, descending)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `desc(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `descNullsLast(self, /)`

PySpark camelCase alias for desc_nulls_last.

### `desc_nulls_first(self, /)`

Descending sort, nulls first. PySpark desc_nulls_first.

### `desc_nulls_last(self, /)`

Descending sort, nulls last. PySpark desc_nulls_last.

### `endswith(self, /, suffix)`

True if string ends with suffix (PySpark endswith).

### `eq(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `eqNullSafe(self, /, other)`

PySpark camelCase alias for eq_null_safe.

### `eq_null_safe(self, /, other)`

Null-safe equality (NULL <=> NULL returns True). PySpark eqNullSafe.
Accepts Column or scalar (int, float, str, bool, None) so .eqNullSafe(lit(x)) or .eqNullSafe(x) work.

### `exp(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `first_value(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `floor(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `ge(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `getField(self, /, name_or_index)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `getItem(self, /, index_or_key)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `get_field(self, /, name_or_index)`

Get struct field by name or array element by index (PySpark Column.getField).
Accepts int (array index, 0-based) or str (struct field name). Issue #1066.

### `get_udf_call_info(self, /)`

Returns (udf_name, arg_column_names) if this column is a Python UDF call, else None. Used by Python UDF executor.

### `gt(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `hour(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `initcap(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isNotNull(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isNull(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `is_not_null(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `is_null(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isin(self, /, *values)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isnotnull(self, /)`

PySpark-style snake_case alias for isNotNull() (used in some upstream tests).

### `isnull(self, /)`

PySpark-style snake_case alias for isNull() (used in some upstream tests).

### `lag(self, /, offset=1)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `last_day(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `last_value(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `le(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lead(self, /, offset=1)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `length`

Property/descriptor; see the API contracts for semantics.

### `levenshtein(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `like(self, /, pattern)`

SQL LIKE pattern match (PySpark like). Supports % and _ wildcards.

### `lower(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lt(self, /, other)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `ltrim(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `minute(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `months_between(self, /, start, round_off=True)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `name`

Property/descriptor; see the API contracts for semantics.

### `or_(self, /, other)`

Logical OR of two boolean columns. PySpark or_.

### `over(self, /, window)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `posexplode(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rank(self, /, descending)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `regexp_extract(self, /, pattern, idx=0)`

Extract first match of regex pattern. PySpark regexp_extract. idx=0 is full match.

### `regexp_replace(self, /, pattern, replacement)`

Replace all matches of regex pattern. PySpark regexp_replace (global replace).

### `repeat(self, /, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `replace(self, /, to_replace, replacement=None)`

PySpark Column.replace(search, replacement) - literal string replace.
PySpark Column.replace: (search, replacement) or replace({old: new, ...}) or replace([(old, new), ...]).

### `reverse(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rlike(self, /, pattern)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `round(self, /, scale=0)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `row_number(self, /, descending)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rtrim(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `second(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `sinh(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `soundex(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `split(self, /, delimiter, limit=Ellipsis)`

Split string by delimiter. PySpark split. limit=-1 means no limit.

### `startswith(self, /, prefix)`

True if string starts with prefix (PySpark startswith).

### `substr(self, /, start, length=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `timestamp_seconds(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_utc_timestamp(self, /, tz)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `trim(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `try_cast(self, /, _type_name)`

PySpark parity (#1233): Column.try_cast is not supported; raises TypeError like PySpark.

### `upper(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `withField(self, /, name, value)`

PySpark camelCase alias for with_field.

### `with_field(self, /, name, value)`

Add or replace a struct field. PySpark withField.

### `xxhash64(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## GroupedData

### `agg(self, /, *exprs)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `avg(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `count(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `max(self, /, col_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `mean(self, /, *cols)`

PySpark groupBy().mean(*cols). Accepts one or more column names or Columns.

### `min(self, /, col_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `pivot(self, /, pivot_col, values=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `sum(self, /, col_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## DataFrameReader

### `csv(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `delta(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `format(self, /, fmt)`

PySpark: format("parquet"|"csv"|"json"|"delta"). Used by load().

### `jdbc(self, /, url, table, properties=None)`

PySpark: jdbc(url, table, properties=None, ...). Reads from a JDBC data source.

Supports PySpark-compatible options:
- sessionInitStatement: SQL to execute after connecting
- queryTimeout: Query timeout in seconds
- prepareQuery: SQL to execute before main query
- fetchsize: Number of rows to fetch per round trip
- partitionColumn, lowerBound, upperBound, numPartitions: For partitioned reads

### `json(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `load(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `option(self, /, key, value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `options(self, /, opts)`

PySpark: options(**kwargs). opts: dict of key -> value.

### `parquet(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `table(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## DataFrameWriter

### `csv(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `format(self, /, fmt)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `jdbc(self, /, url, table, properties, mode=None)`

PySpark: jdbc(url, table, properties). Properties is a dict of string -> string.

### `json(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `mode(self, /, mode)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `option(self, /, key, value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `options(self, /, opts=None, **kwargs)`

Accept mappings or ordered pairs, preserving duplicate keys like the Rust writer.

### `parquet(self, /, path)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `partitionBy(self, /, *cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `save(self, /, path)`

PySpark: save(path). Uses the selected format (Parquet by default).

### `saveAsTable(self, /, name, mode=None)`

PySpark camelCase alias for save_as_table

### `save_as_table(self, /, name, mode=None)`

PySpark: saveAsTable(name). mode: "error"|"overwrite"|"append"|"ignore".

## SessionBuilder

### `appName(self, /, name)`

PySpark camelCase alias for app_name

### `app_name(self, /, name)`

PySpark parity (#412, #1239): Builder is not callable; SparkSession.builder.appName(...) works,
builder() raises TypeError. We do not implement __call__ so that builder() raises as in PySpark.

### `config(self, /, key, value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `getOrCreate(self, /)`

PySpark camelCase alias for get_or_create

### `get_or_create(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `master(self, /, master)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `option(self, /, key, value)`

PySpark builder.option(key, value); value can be str, bool, int, etc. (stringified).

## Catalog

### `cacheTable(self, /, tableName)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `currentDatabase(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dropGlobalTempView(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dropTable(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dropTempView(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `getTable(self, /, tableName)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isCached(self, /, tableName)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `listDatabases(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `listTables(self, /, dbName=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `setCurrentDatabase(self, /, name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `tableExists(self, /, arg1, arg2=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `uncacheTable(self, /, tableName)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## RuntimeConfig

### `get(key, default=None)`

PySpark: spark.conf.get(key, default=None) returns value or default as string.

### `is_case_sensitive()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `set(key, value)`

PySpark: spark.conf.set(key, value).

## LocalRDD

### `collect(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `collect_elements(self, /)`

Collect elements (rows when from df.rdd, or materialized list).

### `collect_elements_limited(self, /, limit)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `count(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `distinct(self, /)`

PySpark parity: return an RDD with duplicate elements removed (first occurrence kept).

### `filter(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `first(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `flatMap(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isEmpty(self, /)`

PySpark parity: True when the RDD has no elements.

### `map(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `reduce(self, /, func)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `take(self, /, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## CancellationHandle

### `cancelAllActiveJobs(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cancel_all_active_actions(self, /)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## Window

### `currentRow`

Property/descriptor; see the API contracts for semantics.

### `orderBy(*cols: 'object') -> 'WindowSpec'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `partitionBy(*cols: 'object') -> 'WindowSpec'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rangeBetween(start: 'int', end: 'int') -> 'WindowSpec'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `rowsBetween(start: 'int', end: 'int') -> 'WindowSpec'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `unboundedFollowing`

Property/descriptor; see the API contracts for semantics.

### `unboundedPreceding`

Property/descriptor; see the API contracts for semantics.

## functions

### `DataFrame()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `approx_count_distinct(col_or_name, rsd=0.05)`

Approximate distinct count (PySpark approx_count_distinct). rsd is optional; when omitted, behaves like exact count_distinct with PySpark-style column naming.

### `array(*cols)`

Create array column from columns (PySpark array).

### `array_compact(col_or_name: 'ColumnOrName') -> '_ColumnType'`

Remove null elements from array (PySpark array_compact). Output column name matches PySpark: array_compact(col).

### `array_contains(column, value)`

True if array contains value (PySpark array_contains).

- Literal values (int/float/bool/str) are compared directly to array elements.
- Column arguments (Column) use arrays_overlap(array_col, array(value_col)) so join
  conditions like df.filter(F.array_contains("tags", other_col)) work.

### `array_distinct(col_or_name: 'ColumnOrName') -> '_ColumnType'`

Distinct elements in array (PySpark array_distinct). Output column name matches PySpark: array_distinct(col).

### `array_position(col_or_name: 'ColumnOrName', value: 'Union[ColumnOrName, int, float, bool, str]') -> '_ColumnType'`

1-based index of first occurrence of value in list, or 0 if not found. PySpark: F.array_position(col, value).

### `array_remove(col_or_name: 'ColumnOrName', value: 'Union[ColumnOrName, int, float, bool, str]') -> '_ColumnType'`

Remove all elements equal to value from the array. PySpark: F.array_remove(col, element).

### `arrays_overlap(col1, col2)`

True if two array columns have any element in common (PySpark arrays_overlap).

### `asc(col_or_name)`

Sort key for ascending order in Window.orderBy.

### `asinh(column)`

Inverse hyperbolic sine (PySpark asinh).

### `atanh(column)`

Inverse hyperbolic tangent (PySpark atanh).

### `avg(c)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `cast(col_or_name: 'ColumnOrName', data_type: 'object') -> '_ColumnType'`

Module-level cast for PySpark parity.

In real PySpark, `pyspark.sql.functions.cast` does not accept a `DataType` object
(only strings / SQL type names). Some tests assert that passing a `DataType`
fails, so we explicitly raise here to match that behavior.

### `coalesce(*cols)`

Return first non-null value from columns (PySpark coalesce).

### `col(name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `concat(*cols)`

Concatenate columns as strings (PySpark concat).

### `concat_ws(separator, *cols)`

Concatenate columns with separator (PySpark concat_ws).

### `cosh(column)`

Hyperbolic cosine (PySpark cosh).

### `count(c='*')`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `countDistinct(*cols)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `crc32(c: 'ColumnOrName') -> '_ColumnType'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `create_map(*cols: 'Any') -> '_ColumnType'`

Build a map column from alternating key/value expressions (PySpark create_map).

With no args or create_map([]), returns a column of empty maps per row.

### `cume_dist() -> '_CumeDistExpr'`

Window cume_dist() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `current_catalog()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `current_database()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `current_date()`

Return current date as column expression (PySpark parity: no session required at build time).

### `current_schema()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `current_timestamp()`

Return current timestamp as column expression (PySpark parity: no session required at build time).

### `current_user()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `date_add(column, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `date_format(column, fmt)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `date_sub(column, n)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `date_trunc(format, column)`

Truncate date/timestamp to unit (PySpark date_trunc).

### `datediff(end, start)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dayofmonth(column)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dayofweek(column)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `dense_rank() -> '_DenseRankExpr'`

Window dense_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `desc(col_or_name)`

Sort key for descending order in Window.orderBy.

### `explode(col_or_name: 'ColumnOrName') -> '_ColumnType'`

Explode array into one row per element (PySpark explode).

### `expr(sql_expr: 'str')`

F.expr() support: SQL expression string resolved in select() context (PySpark parity).
- REGEXP/RLIKE: "col REGEXP 'pat'" -> col(col).rlike(pat)
- Other expressions (e.g. "upper(x) as up") -> expr_str; resolved when used in df.select().
Requires active SparkSession.

### `first(col_or_name: 'ColumnOrName', ignorenulls: 'bool' = False) -> '_ColumnType'`

First value in group (PySpark first); use in groupBy().agg(). Default ignorenulls=False.

### `first_value(col_or_name)`

Window first_value(col) expression; use with .over(Window.partitionBy(...)).

### `floor(column)`

Floor to nearest integer (PySpark floor).

### `format_string(fmt, *cols)`

Printf-style format (PySpark format_string).

### `from_unixtime(column, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `get_json_object(column, path)`

Extract JSON path from string column (PySpark get_json_object).

### `greatest(*cols)`

Greatest of columns per row (PySpark greatest).

### `hour(column)`

Extract hour from datetime (PySpark hour).

### `input_file_name()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `isnotnull(col_or_name)`

Return a boolean column that is true when the column is not null. PySpark: F.isnotnull(col).

### `isnull(col_or_name)`

Return a boolean column that is true when the column is null. PySpark: F.isnull(col).

### `json_tuple(column: 'ColumnOrName', *keys: 'str') -> 'Union[_ColumnType, Tuple[_ColumnType, ...]]'`

Extract keys from JSON as columns (PySpark json_tuple).

Returns one string column per key, named c0, c1, ... in the order of keys.
With a single key, returns one Column (for withColumn); multiple keys return a tuple
that select() flattens into separate columns.

### `lag(col_or_name, offset=1)`

Window lag(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...)).

### `last_day(column)`

Last day of month for date column (PySpark last_day).

### `last_value(col_or_name)`

Window last_value(col) expression; use with .over(Window.partitionBy(...)).

### `lead(col_or_name, offset=1)`

Window lead(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...)).

### `least(*cols)`

Least of columns per row (PySpark least).

### `length(column)`

String length in characters (PySpark length).

### `levenshtein(column, other)`

Levenshtein distance between two strings (PySpark levenshtein).

### `lit(value)`

Polymorphic lit: dispatches to lit_i64, lit_str, lit_f64, lit_bool based on Python type.

### `lit_bool(value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lit_f64(value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lit_i64(value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lit_null(dtype)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lit_str(value)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `lower(col)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `ltrim(column)`

Trim leading whitespace (PySpark ltrim).

### `max(c)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `mean(col_or_name: 'ColumnOrName') -> '_ColumnType'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `min(c)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `minute(column)`

Extract minute from datetime (PySpark minute).

### `month(column)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `months_between(end, start, round_off=True)`

Months between end and start dates (PySpark months_between).

### `ntile(n: 'int') -> '_NtileExpr'`

Window ntile(n) expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `nullif(col1, col2)`

Return null if col1 equals col2, else col1. PySpark: F.nullif(col1, col2).

### `nvl(col_or_name, replacement)`

Replace null with replacement. PySpark: F.nvl(col, replacement).

### `percent_rank() -> '_PercentRankExpr'`

Window percent_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `posexplode(col)`

Explode array with position (PySpark posexplode). Returns (pos_col, val_col) or wrapper with .alias().

### `printf(fmt, *cols)`

Printf-style format (PySpark format_string).

### `rank() -> '_RankExpr'`

Window rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `regexp_extract(column, pattern, idx=0)`

Extract first regex match (PySpark regexp_extract). idx=0 is full match, 1+ is capture group.

### `regexp_extract_all(column, pattern, idx=0)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `regexp_like(column, pattern)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `regexp_replace(column, pattern, replacement)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `repeat(column, n)`

Repeat string n times (PySpark repeat).

### `round(column, scale=0)`

Round to given decimal places (PySpark round). scale=0 by default.

### `row_number() -> '_RowNumberExpr'`

Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `sinh(column)`

Hyperbolic sine (PySpark sinh).

### `size(column)`

Number of elements in array column (PySpark size).

### `soundex(column)`

American Soundex code (PySpark soundex).

### `split(column, pattern, limit=-1)`

Split string by delimiter (PySpark split). limit=-1 means no limit.

### `stddev(col_or_name)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `struct(*cols)`

Create struct from columns (PySpark struct).

### `substring(column, start, length)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `substring_index(column, delimiter, count)`

Substring before/after nth delimiter (PySpark substring_index). count > 0: before nth from left; count < 0: after nth from right.

### `sum(c)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `timestamp_seconds(column)`

Convert seconds since epoch to timestamp (PySpark timestamp_seconds).

### `to_date(column, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_timestamp(column, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_timestamp_ltz(column, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_timestamp_ntz(column, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `to_utc_timestamp(column, tz)`

Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).

### `translate(column, from_str, to_str)`

Character-by-character translation (PySpark translate).

### `trim(col)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `try_add(left, right)`

Add that returns null on overflow (PySpark try_add).

### `try_cast(column, type_name)`

Cast to type, null on invalid (PySpark try_cast).

### `udf(f=None, returnType=None)`

Create a Python UDF. Use as F.udf(func, returnType) or @udf(returnType) def func ...

When the returned callable is used in withColumn with column(s), the UDF runs row-by-row
(e.g. df.withColumn("y", my_udf(F.col("x")))).

### `unix_timestamp(column=None, fmt=None)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `upper(col)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `when(condition, value)`

PySpark-compatible when(condition, value). Both arguments required. Requires active SparkSession.

### `xxhash64(c: 'ColumnOrName') -> '_ColumnType'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `year(column)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

## types

### `ArrayType(elementType: 'Optional[DataType]' = None, containsNull: 'bool' = True, *, _element_type: 'Optional[DataType]' = None, nullable: 'Optional[bool]' = None) -> 'None'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `BooleanType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `CharType(length: 'int' = 1)`

Char(n) type; we treat as string for schema parsing.

### `DataType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `DateType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `DayTimeIntervalType(start_field: 'str' = 'day', end_field: 'str' = 'second')`

PySpark 4 day-time interval type.

### `DecimalType(precision: 'int' = 10, scale: 'int' = 0)`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `DoubleType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `FloatType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `IntegerType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `IntervalType(start_field: 'str' = 'month', end_field: 'str' = 'month')`

Interval type; we treat as string for schema parsing.

### `LongType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `MapType(keyType: 'DataType', valueType: 'DataType', valueContainsNull: 'bool' = True) -> 'None'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `Row(*args: 'RowValue', **kwargs: 'RowValue') -> "'Row'"`

PySpark-like Row type returned by DataFrame.collect().

Contract: collect() returns a list of Row. Each Row preserves types (int, float, bool,
str, None, date, datetime, list, dict). Use row[name], row[idx], row.asDict(), or
attribute access (row.column_name). Row is tuple-like (iterable, indexable by int).

### `ShortType()`

PySpark ShortType (16-bit integer).

### `StringType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `StructField(name: 'str', dataType: 'DataType', nullable: 'bool' = True, metadata: 'Optional[StructMetadata]' = None) -> 'None'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `StructType(fields: 'Optional[List[StructField]]' = None) -> 'None'`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `TimestampNTZType()`

Timestamp without timezone conversion, matching Spark's TIMESTAMP_NTZ.

### `TimestampType()`

No runtime docstring. See the API contracts and support matrix before relying on this symbol.

### `VarcharType(length: 'int' = 1)`

Varchar(n) type; we treat as string for schema parsing.

### `VariantType()`

PySpark 4 semi-structured VARIANT type (stored as JSON string in Sparkless).

### `YearMonthInterval(months: 'int' = 0)`

Collected year-month interval value (PySpark 4.0 shape).

### `YearMonthIntervalType(start_field: 'str' = 'year', end_field: 'str' = 'month')`

PySpark 4 year-month interval type (collect shape differs from 3.x).

## server

### `AsyncSparklessServer(host: 'str' = '127.0.0.1', port: 'int' = 0, executable_path: 'Optional[str]' = None, startup_timeout: 'float' = 30.0, shutdown_timeout: 'float' = 10.0, kill_timeout: 'float' = 5.0, env: 'Optional[Dict[str, str]]' = None, connect_host: 'str' = '127.0.0.1', connect_port: 'Optional[int]' = 0) -> 'None'`

An asyncio owner for one ``sparkless-server`` child process.

### `SparklessServer(host: 'str' = '127.0.0.1', port: 'int' = 0, executable_path: 'Optional[str]' = None, startup_timeout: 'float' = 30.0, shutdown_timeout: 'float' = 10.0, kill_timeout: 'float' = 5.0, env: 'Optional[Dict[str, str]]' = None, connect_host: 'str' = '127.0.0.1', connect_port: 'Optional[int]' = 0) -> 'None'`

A synchronous owner for one ``sparkless-server`` child process.

### `SparklessServerError(native signature unavailable)`

Raised when the owned native service cannot satisfy its contract.

### `UnsupportedServerPlatformError(native signature unavailable)`

Raised when no packaged server executable exists for this platform.

### `UnsupportedSparkConnectError(native signature unavailable)`

Raised when Spark Connect is disabled or unavailable in the server binary.

## external_catalog

### `ArrowIpcStream(native signature unavailable)`

Arrow IPC stream bytes, compatible with Python's buffer protocol.

This is a bytes subclass so existing ``pyarrow.ipc.open_stream(data)`` calls
keep working. ``data.bytes`` exposes the Rust payload's named field as well.

### `BatchWritableExternalCatalogProvider(*args, **kwargs)`

Provider interface that consumes bounded PyArrow RecordBatches lazily.

### `ExternalCatalogProvider(*args, **kwargs)`

Required provider interface.

Writable providers also implement ``write_table(request:
ExternalTableWriteRequest) -> ExternalTableWriteResult``. Read-only objects
may omit that method.

### `ExternalTableDescriptor(table_id: 'str', canonical_location: 'str', schema: 'T.StructType', provider: 'str', version: 'Optional[int]' = None, permissions_context: 'Optional[str]' = None, format: 'Union[ExternalTableFormat, str]' = <ExternalTableFormat.DELTA: 'delta'>) -> None`

Catalog identity, logical schema, and optional pinned Delta version.

### `ExternalTableFormat(value, names=None, *, module=None, qualname=None, type=None, start=1, boundary=None)`

Storage formats supported by the native external catalog contract.

### `ExternalTableSaveMode(value, names=None, *, module=None, qualname=None, type=None, start=1, boundary=None)`

Save modes accepted by DataFrameWriter.mode().

### `ExternalTableWriteRequest(identifier: 'str', table: 'ExternalTableDescriptor', data: 'Optional[ArrowIpcStream]', mode: 'Union[ExternalTableSaveMode, str]', format: 'Optional[str]', options: 'List[Tuple[str, str]]', partition_by: 'List[str]', batches: 'Optional[Iterator[Any]]' = None) -> None`

A resolved write using either a materialized or lazy Arrow payload.

``write_table`` receives ``data`` as an Arrow IPC stream and ``batches=None``.
``write_table_batches`` receives ``data=None`` and a lazy ``batches`` iterator
of PyArrow RecordBatches. Closing or exhausting that iterator releases the
producer; a callback may stop early by closing it. Options are ordered pairs
so duplicate keys survive. A successful callback must commit the data it
accepts before returning its receipt.

### `ExternalTableWriteResult(table: 'ExternalTableDescriptor', committed_version: 'int') -> None`

Descriptor and exact version of a completed provider commit.

### `SparklessExternalCatalogError(native signature unavailable)`

External catalog provider callback failed

### `WritableExternalCatalogProvider(*args, **kwargs)`

Provider interface including the optional native write hook.
