# Python symbol reference — 4.20.0

Generated from the installed package by `scripts/generate_documentation.py --api`.
CI regenerates this against its wheel and fails on drift. This is the runtime
symbol/signature inventory; [API contracts](PYTHON_API_REFERENCE.md) supplies
argument semantics, returns, errors and practical examples for common operations.
Each entry distinguishes an authored contract, a runtime description, or an
unreviewed symbol. Examples are fragments using an existing `spark`, `df`,
`functions as F`, `types as T`, and `Window` unless stated otherwise.
Importability alone is not evidence of Spark parity. Consult the
[support contract](SUPPORT_MATRIX.md), [file options](FILE_IO.md), and
[SQL dialects](SQL_REFERENCE.md).

## SparkSession

### `app_name`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `backend_type`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `builder`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** SessionBuilder
- **Errors:** Unavailable operations may be absent or raise an error.
- **Example fragment:** `spark.builder`
- **Notes:** A class property; do not call builder().

### `cancelAllActiveJobs(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Number of active session actions signaled.
- **Errors:** Work stops only at cancellation safe points.
- **Example fragment:** `spark.cancelAllActiveJobs()`

### `cancel_all_active_actions(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Number of active session actions signaled.
- **Errors:** Work stops only at cancellation safe points.
- **Example fragment:** `spark.cancelAllActiveJobs()`

### `cancellationHandle(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; obtain on the owning thread.
- **Returns:** Thread-safe control handle for cooperative cancellation.
- **Errors:** A request is not a hard deadline or proof of rollback.
- **Example fragment:** `handle = spark.cancellation_handle()`

### `cancellation_handle(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; obtain on the owning thread.
- **Returns:** Thread-safe control handle for cooperative cancellation.
- **Errors:** A request is not a hard deadline or proof of rollback.
- **Example fragment:** `handle = spark.cancellation_handle()`

### `catalog`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** Catalog
- **Errors:** Unavailable operations may be absent or raise an error.
- **Example fragment:** `spark.catalog`
- **Notes:** A property; listTables returns names rather than PySpark Table objects.

### `conf`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** RuntimeConfig
- **Errors:** Unavailable operations may be absent or raise an error.
- **Example fragment:** `spark.conf`
- **Notes:** Retaining a Spark key does not imply its subsystem exists.

### `createDataFrame(self, /, data, schema=None, **kwargs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** data: rows, dictionaries, tuples or supported pandas/Arrow input; schema: omitted, names or StructType. Prefer explicit types for empty/null/mixed input. verifySchema/verify_schema validates supported schema conversions; samplingRatio is retained for list/dict input without sampling.
- **Returns:** A local DataFrame attached to the session.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `spark.createDataFrame([(1,), (None,)], T.StructType([T.StructField("id", T.LongType())]))`

### `create_dataframe(self, /, data, column_names)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `create_dataframe_from_rows(self, /, data, schema=None, verify_schema=True, sampling_ratio=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** data: rows, dictionaries, tuples or supported pandas/Arrow input; schema: omitted, names or StructType. Prefer explicit types for empty/null/mixed input. verifySchema/verify_schema validates supported schema conversions; samplingRatio is retained for list/dict input without sampling.
- **Returns:** A local DataFrame attached to the session.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `spark.createDataFrame([(1,), (None,)], T.StructType([T.StructField("id", T.LongType())]))`

### `create_or_replace_global_temp_view(self, /, name, df)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `create_or_replace_temp_view(self, /, name, df)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `drop_global_temp_view(self, /, name)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `drop_temp_view(self, /, name)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `getActiveSession()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `newSession(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** A new local session with isolated temporary state and no attached external provider.
- **Errors:** Native session creation errors.
- **Example fragment:** `other = spark.newSession()`

### `range(self, /, start, end=None, step=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** start, optional exclusive end and step; one argument means 0..start; step defaults to 1 and cannot be zero.
- **Returns:** A DataFrame with a long id column.
- **Errors:** Invalid/zero steps or exceeding SPARKLESS_MAX_RANGE_ROWS raises an error.
- **Example fragment:** `spark.range(1, 5, 2).collect()`

### `read`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** DataFrameReader
- **Errors:** Unavailable operations may be absent or raise an error.
- **Example fragment:** `spark.read`
- **Notes:** A property; use spark.read.csv(path), not spark.read().

### `read_csv(self, /, path)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `read_delta(self, /, name_or_path)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `read_delta_with_version(self, /, name_or_path, version=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `read_json(self, /, path)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `read_parquet(self, /, path)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `setExternalCatalogProvider(self, /, provider)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** provider: ownership/metadata provider, optionally with batch writer; None detaches.
- **Returns:** None; session owns a strong provider reference.
- **Errors:** Invalid protocol, descriptor and callback errors; write outcomes may be unknown.
- **Example fragment:** `spark.set_external_catalog_provider(provider)`
- **Notes:** Read the provider tutorial before implementing writes.

### `set_external_catalog_provider(self, /, provider)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** provider: ownership/metadata provider, optionally with batch writer; None detaches.
- **Returns:** None; session owns a strong provider reference.
- **Errors:** Invalid protocol, descriptor and callback errors; write outcomes may be unknown.
- **Example fragment:** `spark.set_external_catalog_provider(provider)`
- **Notes:** Read the provider tutorial before implementing writes.

### `sparkContext`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `sql(self, /, query)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** query: one supported local SQL statement; native SQL feature required.
- **Returns:** Query DataFrame; supported commands may mutate state when submitted.
- **Errors:** SQL/unsupported statement/feature errors; see SQL dialect reference.
- **Example fragment:** `spark.sql("SELECT 1 AS value").collect()`
- **Notes:** Local SQL is separate from remote Connect SQL; do not infer command timing across surfaces.

### `stop(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments. Call only for a session whose lifecycle you own.
- **Returns:** None; signals active actions and clears session state.
- **Errors:** Native cleanup errors can propagate.
- **Example fragment:** `spark.stop()`

### `table(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: exact table/view identifier resolved by this session, its provider and warehouse.
- **Returns:** A DataFrame for the resolved table.
- **Errors:** Missing table, provider failure, unsupported feature or schema mismatch.
- **Example fragment:** `spark.table("orders").count()`

## DataFrame

### `agg(self, /, *exprs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *exprs: aggregate Column expressions, normally explicitly aliased.
- **Returns:** A global aggregate DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.agg(F.sum("amount").alias("total"))`

### `alias(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: relation alias string.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.alias("orders")`

### `cache(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Compatibility arguments shown in the runtime signature.
- **Returns:** Local compatibility result; no Spark executor/storage/shuffle guarantee.
- **Errors:** Unsupported arguments or native failures may raise errors.
- **Example fragment:** `df.cache()`
- **Notes:** Do not use this spelling as evidence of distributed behavior or a performance guarantee.

### `cancel(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

Cancel active actions started through this DataFrame object.
A count is returned so callers can tell whether any work was still active.

### `coalesce(self, /, _n)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Compatibility arguments shown in the runtime signature.
- **Returns:** Local compatibility result; no Spark executor/storage/shuffle guarantee.
- **Errors:** Unsupported arguments or native failures may raise errors.
- **Example fragment:** `df.coalesce(1)`
- **Notes:** Do not use this spelling as evidence of distributed behavior or a performance guarantee.

### `collect(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** List of Rows supporting field-name and positional access.
- **Errors:** Native execution/conversion/cancellation errors.
- **Example fragment:** `rows = df.collect()`
- **Notes:** Materializes the entire result; use row[name] explicitly rather than assuming iteration yields values.

### `columns`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** List of field names
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.columns`
- **Notes:** Names preserve projection order.

### `count(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Integer row count; executes the plan.
- **Errors:** Native execution or cancellation error.
- **Example fragment:** `n = df.count()`

### `createOrReplaceGlobalTempView(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: process-global temporary view name.
- **Returns:** None; access through global_temp.name within this process.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.createOrReplaceGlobalTempView("orders")`
- **Notes:** Does not survive process restart.

### `createOrReplaceTempView(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: session-local view identifier.
- **Returns:** None; registers/replaces the view in the associated session.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.createOrReplaceTempView("orders")`

### `create_or_replace_temp_view(self, /, session, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: session-local view identifier.
- **Returns:** None; registers/replaces the view in the associated session.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.createOrReplaceTempView("orders")`

### `crossJoin(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `crosstab(self, /, col1, col2)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `cube(self, /, *cols)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `describe(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `distinct(self, /, subset=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** DataFrame with duplicate rows removed.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.distinct()`
- **Notes:** Output order is unspecified.

### `do_order_by(self, /, cols, ascending=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `do_order_by_impl(self, /, cols, ascending=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `drop(self, /, *columns)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Field names to remove.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.drop("temporary")`

### `dropDuplicates(self, /, subset=None, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Optional subset of field names; omit for whole-row deduplication.
- **Returns:** Deduplicated DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.dropDuplicates(["id"])`
- **Notes:** Do not rely on which non-key values survive without a separate policy.

### `drop_duplicates(self, /, subset=None, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Optional subset of field names; omit for whole-row deduplication.
- **Returns:** Deduplicated DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.dropDuplicates(["id"])`
- **Notes:** Do not rely on which non-key values survive without a separate policy.

### `dropna(self, /, subset=None, how=None, thresh=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dtypes`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** Name/type pairs
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.dtypes`
- **Notes:** Use schema for nested and nullability detail.

### `exceptAll(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `explain(self, /, extended=None)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark: explain() prints to stdout and returns None (PySpark parity).

### `fillna(self, /, value, subset=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `filter(self, /, condition)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** condition: Boolean Column or supported SQL predicate string. Null predicate rows are removed.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.filter(F.col("id") > 1)`

### `first(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

Return the first row as a Row, or None if empty (Phase 7.8 / PySpark first() semantics).

### `flatMap(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `flat_map(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `freqItems(self, /, columns, support=0.01)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `groupBy(self, /, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: field names/expressions; omit for global aggregation.
- **Returns:** GroupedData; call agg/count/sum/etc. to obtain a DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").agg(F.sum("amount").alias("total"))`

### `group_by(self, /, column_names)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column_names: list of field-name strings. Use groupBy(*cols) for the PySpark-style variadic form.
- **Returns:** GroupedData; call agg/count/sum/etc. to obtain a DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.group_by(["region"]).sum("amount")`

### `groupby(self, /, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: field names/expressions; omit for global aggregation.
- **Returns:** GroupedData; call agg/count/sum/etc. to obtain a DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").agg(F.sum("amount").alias("total"))`

### `head(self, /, n=None)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark parity: head() returns the first Row (or None if empty); head(n) returns a list of Row.

### `intersect(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `isEmpty(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `join(self, /, other, on=None, how='inner', left_on=None, right_on=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: DataFrame; on: name/list or supported equality condition; how defaults to inner. left_on/right_on optionally use different key names.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `orders.join(customers, on="customer_id", how="left")`
- **Notes:** Alias ambiguous fields. Non-equality joins can materialize a cross product and hit the row cap.

### `limit(self, /, n)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** n: nonnegative row count.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.limit(10)`
- **Notes:** An unordered input has no stable row-selection contract.

### `melt(self, /, id_vars, value_vars)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `na`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** Null-value helper
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.na`
- **Notes:** Supports documented fill/drop operations; not all Spark NA behavior.

### `orderBy(self, /, *cols, ascending=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: names or sort expressions; ascending defaults to ascending and may be a bool/list.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy("id", ascending=False)`
- **Notes:** Add tie-breakers for deterministic ordering. No distributed sort semantics are promised.

### `order_by(self, /, *cols, ascending=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: names or sort expressions; ascending defaults to ascending and may be a bool/list.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy("id", ascending=False)`
- **Notes:** Add tie-breakers for deterministic ordering. No distributed sort semantics are promised.

### `order_by_exprs(self, /, exprs)`

**Documentation status: runtime description; no authored per-symbol contract.**

order_by_exprs([col("a").asc(), col("b").desc_nulls_last()]) - order by SortOrder expressions.

### `order_by_names(self, /, column_names, ascending)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `persist(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Compatibility arguments shown in the runtime signature.
- **Returns:** Local compatibility result; no Spark executor/storage/shuffle guarantee.
- **Errors:** Unsupported arguments or native failures may raise errors.
- **Example fragment:** `df.persist()`
- **Notes:** Do not use this spelling as evidence of distributed behavior or a performance guarantee.

### `printSchema(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `print_schema(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rdd`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** Bounded local RDD helper
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.rdd`
- **Notes:** No distributed execution. Materialization cap applies.

### `repartition(self, /, _n)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Compatibility arguments shown in the runtime signature.
- **Returns:** Local compatibility result; no Spark executor/storage/shuffle guarantee.
- **Errors:** Unsupported arguments or native failures may raise errors.
- **Example fragment:** `df.repartition(2)`
- **Notes:** Do not use this spelling as evidence of distributed behavior or a performance guarantee.

### `replace(self, /, to_replace, *values, subset=None, **kwargs)`

**Documentation status: runtime description; no authored per-symbol contract.**

Replace value in columns. PySpark df.replace(to_replace, value=None, subset=...).
Uses *values and **kwargs so we can distinguish "value not provided" from "value is Python None",
and accept value when passed as keyword (e.g. from Python na.replace(to_replace, value=value, subset=...)).

### `rollup(self, /, *cols)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `sampleBy(self, /, col, fractions, seed=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `schema`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** StructType
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.schema`
- **Notes:** Inspect names, order, types and nullability.

### `select(self, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: field-name strings or Column expressions; alias computed expressions. Strings are names, not SQL expressions.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.select("id", F.col("amount").alias("total"))`

### `selectExpr(self, /, *exprs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *exprs: supported SQL expression strings.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.selectExpr("id", "amount + 1 AS next_amount")`

### `show(self, /, n=20, truncate=True)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** n=20 maximum displayed rows; truncate=True controls display shortening.
- **Returns:** None; prints a table.
- **Errors:** Native execution/display/cancellation errors.
- **Example fragment:** `df.show(5, truncate=False)`
- **Notes:** For inspection; use a supported writer for export.

### `sort(self, *cols, ascending=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: names or sort expressions; ascending defaults to ascending and may be a bool/list.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy("id", ascending=False)`
- **Notes:** Add tie-breakers for deterministic ordering. No distributed sort semantics are promised.

### `storage`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `subtract(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `summary(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark alias for describe().

### `take(self, /, n)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `toArrow(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; PyArrow required.
- **Returns:** PyArrow Table.
- **Errors:** ImportError or conversion/execution error.
- **Example fragment:** `table = df.toArrow()`
- **Notes:** Collects the complete result.

### `toArrowBatches(self, /, max_rows=10000, max_bytes=67108864)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** max_rows=10000 and max_bytes=67108864: positive per-output-batch bounds; PyArrow required.
- **Returns:** Context-managed RecordBatch iterator; close on early exit.
- **Errors:** ValueError for invalid limits or an oversized row; execution/cancellation errors.
- **Example fragment:** `with df.toArrowBatches(max_rows=16) as batches: sizes = [b.num_rows for b in batches]`
- **Notes:** Batch bounds do not cap intermediate query memory or process RSS.

### `toDF(self, /, *col_names)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `toPandas(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; pandas required.
- **Returns:** pandas DataFrame.
- **Errors:** ImportError when dependency is missing; native conversion/execution errors.
- **Example fragment:** `pdf = df.toPandas()`
- **Notes:** Collects the complete result; validate decimal, timestamp and nested conversions.

### `to_arrow_batches(self, /, max_rows=10000, max_bytes=67108864)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** max_rows=10000 and max_bytes=67108864: positive per-output-batch bounds; PyArrow required.
- **Returns:** Context-managed RecordBatch iterator; close on early exit.
- **Errors:** ValueError for invalid limits or an oversized row; execution/cancellation errors.
- **Example fragment:** `with df.toArrowBatches(max_rows=16) as batches: sizes = [b.num_rows for b in batches]`
- **Notes:** Batch bounds do not cap intermediate query memory or process RSS.

### `union(self, /, other)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: compatible DataFrame; columns align by position.
- **Returns:** A concatenated DataFrame; duplicates are retained.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.union(other)`
- **Notes:** Validate column order and types; use unionByName for name alignment.

### `unionAll(self, /, other)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: compatible DataFrame; columns align by position.
- **Returns:** A concatenated DataFrame; duplicates are retained.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.union(other)`
- **Notes:** Validate column order and types; use unionByName for name alignment.

### `unionByName(self, /, other, **kwargs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: DataFrame; allowMissingColumns=True permits missing fields.
- **Returns:** A concatenated DataFrame aligned by name.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.unionByName(other, allowMissingColumns=True)`

### `union_all(self, /, other)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: compatible DataFrame; columns align by position.
- **Returns:** A concatenated DataFrame; duplicates are retained.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.union(other)`
- **Notes:** Validate column order and types; use unionByName for name alignment.

### `unpivot(self, /, ids, values, *, variableColumnName=None, valueColumnName=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `where(self, /, condition)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** condition: Boolean Column or supported SQL predicate string. Null predicate rows are removed.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.filter(F.col("id") > 1)`

### `withColumn(self, /, name, col)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: output column string; col: Column expression. Adds or replaces that name.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.withColumn("next_id", F.col("id") + 1)`

### `withColumnRenamed(self, /, old_name, new_name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** existing and new field names. Validate collisions in your workload.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.withColumnRenamed("id", "order_id")`

### `withColumns(self, /, cols_map)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `withColumnsRenamed(self, /, cols_map)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `with_column(self, /, name, col_any)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: output column string; col: Column expression. Adds or replaces that name.
- **Returns:** A new DataFrame; transformations generally remain lazy until an action.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.withColumn("next_id", F.col("id") + 1)`

### `with_columns_renamed(self, /, cols_map)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `write`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Property; no call arguments.
- **Returns:** DataFrameWriter
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `value = df.write`
- **Notes:** Property; defaults to overwrite. Set a mode explicitly.

### `writeTo(self, /, _table)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## Column

### `alias(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** alias: output field name string.
- **Returns:** Column with renamed output.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.sum("amount").alias("total")`

### `and_(self, /, other)`

**Documentation status: runtime description; no authored per-symbol contract.**

Logical AND of two boolean columns. PySpark and_.

### `array_compact(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

Remove null elements from list (PySpark array_compact).

### `array_distinct(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

col[key] -> map get or array getItem. Key: int (array index), str (map key), or Column (map key).

### `asc(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").asc())`

### `asc_nulls_first(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").asc_nulls_first())`

### `asc_nulls_last(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").asc_nulls_last())`

### `asinh(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `astype(self, /, dtype)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** type_name: supported type string or DataType object exposing simpleString/typeName.
- **Returns:** Column with a type-conversion expression.
- **Errors:** Invalid type or failed conversion according to compatibility profile.
- **Example fragment:** `F.col("amount").cast("double")`

### `atanh(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `between(self, /, lower, upper)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** lower, upper: scalar or Column boundaries, inclusive.
- **Returns:** Boolean Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("age").between(18, 65)`

### `cast(self, /, type_name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** type_name: supported type string or DataType object exposing simpleString/typeName.
- **Returns:** Column with a type-conversion expression.
- **Errors:** Invalid type or failed conversion according to compatibility profile.
- **Example fragment:** `F.col("amount").cast("double")`

### `contains(self, /, literal)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Substring/pattern argument; like uses SQL wildcard patterns, rlike regular expressions.
- **Returns:** Boolean string-test Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("name").contains("A")`

### `cosh(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `crc32(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dense_rank(self, /, descending)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `desc(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").desc())`

### `descNullsLast(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").descNullsLast())`

### `desc_nulls_first(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").desc_nulls_first())`

### `desc_nulls_last(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Sort expression specifying direction and, where named, null placement.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.orderBy(F.col("id").desc_nulls_last())`

### `endswith(self, /, suffix)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Substring/pattern argument; like uses SQL wildcard patterns, rlike regular expressions.
- **Returns:** Boolean string-test Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("name").endswith("a")`

### `eq(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `eqNullSafe(self, /, other)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: comparable value/Column.
- **Returns:** Null-safe equality Column; unlike ordinary equality, compares nulls explicitly.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("id").eqNullSafe(F.col("other_id"))`

### `eq_null_safe(self, /, other)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** other: comparable value/Column.
- **Returns:** Null-safe equality Column; unlike ordinary equality, compares nulls explicitly.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("id").eqNullSafe(F.col("other_id"))`

### `exp(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `first_value(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `floor(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `ge(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `getField(self, /, name_or_index)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `getItem(self, /, index_or_key)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `get_field(self, /, name_or_index)`

**Documentation status: runtime description; no authored per-symbol contract.**

Get struct field by name or array element by index (PySpark Column.getField).
Accepts int (array index, 0-based) or str (struct field name). Issue #1066.

### `get_udf_call_info(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

Returns (udf_name, arg_column_names) if this column is a Python UDF call, else None. Used by Python UDF executor.

### `gt(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `hour(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `initcap(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `isNotNull(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing non-null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNotNull()`

### `isNull(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNull()`

### `is_not_null(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing non-null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNotNull()`

### `is_null(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNull()`

### `isin(self, /, *values)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Values or supported sequence, as shown in runtime signature.
- **Returns:** Boolean membership Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("id").isin([1, 2])`

### `isnotnull(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing non-null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNotNull()`

### `isnull(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** Boolean Column testing null.
- **Errors:** Native planning/execution error.
- **Example fragment:** `F.col("id").isNull()`

### `lag(self, /, offset=1)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `last_day(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `last_value(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `le(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lead(self, /, offset=1)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `length`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `levenshtein(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `like(self, /, pattern)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Substring/pattern argument; like uses SQL wildcard patterns, rlike regular expressions.
- **Returns:** Boolean string-test Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("name").like("A%")`

### `lower(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lt(self, /, other)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `ltrim(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `minute(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `months_between(self, /, start, round_off=True)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `name`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Read-only property; no call arguments.
- **Returns:** String name carried by this Column. Use alias(name) to rename output.
- **Errors:** Native property access errors if the column cannot provide its name.
- **Example fragment:** `F.col("id").name`
- **Notes:** This is not the PySpark name(alias) method.

### `or_(self, /, other)`

**Documentation status: runtime description; no authored per-symbol contract.**

Logical OR of two boolean columns. PySpark or_.

### `over(self, /, window)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** window: Window specification with partition/order/frame definitions.
- **Returns:** Window expression Column.
- **Errors:** Unsupported window/frame/input types or planning errors.
- **Example fragment:** `F.row_number().over(Window.partitionBy("region").orderBy("amount"))`

### `posexplode(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rank(self, /, descending)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `regexp_extract(self, /, pattern, idx=0)`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract first match of regex pattern. PySpark regexp_extract. idx=0 is full match.

### `regexp_replace(self, /, pattern, replacement)`

**Documentation status: runtime description; no authored per-symbol contract.**

Replace all matches of regex pattern. PySpark regexp_replace (global replace).

### `repeat(self, /, n)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `replace(self, /, to_replace, replacement=None)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark Column.replace(search, replacement) - literal string replace.
PySpark Column.replace: (search, replacement) or replace({old: new, ...}) or replace([(old, new), ...]).

### `reverse(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rlike(self, /, pattern)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Substring/pattern argument; like uses SQL wildcard patterns, rlike regular expressions.
- **Returns:** Boolean string-test Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("name").rlike("^A")`

### `round(self, /, scale=0)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `row_number(self, /, descending)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rtrim(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `second(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `sinh(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `soundex(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `split(self, /, delimiter, limit=Ellipsis)`

**Documentation status: runtime description; no authored per-symbol contract.**

Split string by delimiter. PySpark split. limit=-1 means no limit.

### `startswith(self, /, prefix)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Substring/pattern argument; like uses SQL wildcard patterns, rlike regular expressions.
- **Returns:** Boolean string-test Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("name").startswith("A")`

### `substr(self, /, start, length=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `timestamp_seconds(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `to_utc_timestamp(self, /, tz)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `trim(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `try_cast(self, /, _type_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark parity (#1233): Column.try_cast is not supported; raises TypeError like PySpark.

### `upper(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `withField(self, /, name, value)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark camelCase alias for with_field.

### `with_field(self, /, name, value)`

**Documentation status: runtime description; no authored per-symbol contract.**

Add or replace a struct field. PySpark withField.

### `xxhash64(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## GroupedData

### `agg(self, /, *exprs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *exprs: supported aggregate expressions; use aliases for stable field names.
- **Returns:** Aggregate DataFrame.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").agg(F.sum("amount").alias("total"))`

### `avg(self, /, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: one or more numeric field-name strings; omit only when the numeric-column selection behavior is appropriate for your schema.
- **Returns:** Aggregate DataFrame; field names/types depend on input and expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").avg("amount")`

### `count(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments.
- **Returns:** DataFrame with a count for each group.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").count()`

### `max(self, /, col_name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** col_name: required comparable field-name string.
- **Returns:** Aggregate DataFrame; field names/types depend on input and expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").max("amount")`

### `mean(self, /, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: one or more numeric field-name strings; omit only when the numeric-column selection behavior is appropriate for your schema.
- **Returns:** Aggregate DataFrame; field names/types depend on input and expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").mean("amount")`

### `min(self, /, col_name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** col_name: required comparable field-name string.
- **Returns:** Aggregate DataFrame; field names/types depend on input and expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").min("amount")`

### `pivot(self, /, pivot_col, values=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** pivot_col: field name; optional explicit values as in the runtime signature.
- **Returns:** Pivoted grouped object for subsequent aggregation.
- **Errors:** Unsupported keys/types/aggregates or native errors.
- **Example fragment:** `df.groupBy("region").pivot("kind", ["sale", "refund"]).sum("amount")`

### `sum(self, /, col_name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** col_name: required numeric field-name string.
- **Returns:** Aggregate DataFrame; field names/types depend on input and expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `df.groupBy("region").sum("amount")`

## DataFrameReader

### `csv(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: local filesystem path; configure supported options before the call.
- **Returns:** Lazy DataFrame.
- **Errors:** Missing/invalid path, unsupported compiled feature, parsing and conversion errors.
- **Example fragment:** `spark.read.csv(path)`
- **Notes:** See FILE_IO for exact options and CSV shortcut inference. Delta requires its native feature.

### `delta(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: local filesystem path; configure supported options before the call.
- **Returns:** Lazy DataFrame.
- **Errors:** Missing/invalid path, unsupported compiled feature, parsing and conversion errors.
- **Example fragment:** `spark.read.delta(path)`
- **Notes:** See FILE_IO for exact options and CSV shortcut inference. Delta requires its native feature.

### `format(self, /, fmt)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** fmt: supported format name.
- **Returns:** Reader for chaining.
- **Errors:** Unknown formats fail at load.
- **Example fragment:** `spark.read.format("json")`

### `jdbc(self, /, url, table, properties=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** url: native driver URL; table: database table; properties=None or dictionary; explicit properties override reader options.
- **Returns:** Local DataFrame from the selected compiled database adapter.
- **Errors:** RuntimeError if driver feature is absent; connection/SQL/conversion errors.
- **Example fragment:** `spark.read.jdbc(url, "users", properties=props)`
- **Notes:** Partition options are parsed without partitioned execution. Fetch/timeout effects vary by driver; see JDBC matrix.

### `json(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: local filesystem path; configure supported options before the call.
- **Returns:** Lazy DataFrame.
- **Errors:** Missing/invalid path, unsupported compiled feature, parsing and conversion errors.
- **Example fragment:** `spark.read.json(path)`
- **Notes:** See FILE_IO for exact options and CSV shortcut inference. Delta requires its native feature.

### `load(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: required local path; format defaults to Parquet.
- **Returns:** Lazy DataFrame.
- **Errors:** TypeError when path is omitted; format/IO/feature errors.
- **Example fragment:** `spark.read.format("parquet").load(path)`
- **Notes:** Generic CSV load follows native inference defaults; explicitly set inferSchema.

### `option(self, /, key, value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** key and scalar value; bool/int/float converted to text, None to empty string.
- **Returns:** Same reader for chaining.
- **Errors:** Unsupported option value type.
- **Example fragment:** `spark.read.option("header", True)`
- **Notes:** Retaining an unknown option does not implement it.

### `options(self, /, opts)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** opts: dictionary of option key/value pairs.
- **Returns:** Same reader for chaining.
- **Errors:** TypeError for non-dictionary input or invalid values.
- **Example fragment:** `spark.read.options({"header": True, "inferSchema": False})`

### `parquet(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: local filesystem path; configure supported options before the call.
- **Returns:** Lazy DataFrame.
- **Errors:** Missing/invalid path, unsupported compiled feature, parsing and conversion errors.
- **Example fragment:** `spark.read.parquet(path)`
- **Notes:** See FILE_IO for exact options and CSV shortcut inference. Delta requires its native feature.

### `table(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: local, warehouse or provider table identifier.
- **Returns:** Resolved DataFrame.
- **Errors:** Missing table/provider/schema errors.
- **Example fragment:** `spark.read.table("orders")`

## DataFrameWriter

### `csv(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: required local output path.
- **Returns:** None; executes a file write.
- **Errors:** IO/schema/conversion errors.
- **Example fragment:** `df.write.mode("overwrite").csv(path)`
- **Notes:** Plain file paths distinguish only append from overwrite; error/ignore mode strings do not protect existing data.

### `format(self, /, fmt)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** fmt: parquet/csv/json/delta for path saves, or provider format for table saves.
- **Returns:** Writer for chaining.
- **Errors:** Unsupported format/feature at save.
- **Example fragment:** `df.write.format("delta")`

### `jdbc(self, /, url, table, properties, mode=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** url, table, properties dictionary and optional mode; use an explicit save mode.
- **Returns:** None; writes through a compiled native driver.
- **Errors:** Feature/connection/SQL errors; partial multi-batch effects may require reconciliation.
- **Example fragment:** `df.write.jdbc(url, "backup", properties=props, mode="append")`
- **Notes:** Driver-specific behavior; no general transaction/Java JDBC plugin contract.

### `json(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: required local output path.
- **Returns:** None; executes a file write.
- **Errors:** IO/schema/conversion errors.
- **Example fragment:** `df.write.mode("overwrite").json(path)`
- **Notes:** Plain file paths distinguish only append from overwrite; error/ignore mode strings do not protect existing data.

### `mode(self, /, mode)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** mode: string; choose explicitly.
- **Returns:** Writer for chaining.
- **Errors:** Validation depends on destination; plain-file unknown/error/ignore values map to overwrite.
- **Example fragment:** `df.write.mode("append")`
- **Notes:** Initial mode is overwrite. Four-mode semantics apply separately to Delta and table saves.

### `option(self, /, key, value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** key and scalar value.
- **Returns:** Writer for chaining, preserving ordered duplicate option pairs for providers.
- **Errors:** Invalid option value type.
- **Example fragment:** `df.write.option("mergeSchema", True)`
- **Notes:** Supported effects depend on format; see FILE_IO.

### `options(self, /, opts=None, **kwargs)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** Optional mapping or ordered key/value pairs, plus keyword options.
- **Returns:** Writer for chaining.
- **Errors:** Invalid mapping/pairs/value types.
- **Example fragment:** `df.write.options({"header": True})`

### `parquet(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: required local output path.
- **Returns:** None; executes a file write.
- **Errors:** IO/schema/conversion errors.
- **Example fragment:** `df.write.mode("overwrite").parquet(path)`
- **Notes:** Plain file paths distinguish only append from overwrite; error/ignore mode strings do not protect existing data.

### `partitionBy(self, /, *cols)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** *cols: partition field names.
- **Returns:** Writer for chaining.
- **Errors:** Missing/invalid fields or unsupported partition format behavior.
- **Example fragment:** `df.write.partitionBy("region")`
- **Notes:** Hive-style file directories omit partition columns from part files. No Spark dynamic overwrite guarantee.

### `save(self, /, path)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** path: required; selected format defaults to Parquet.
- **Returns:** None; executes the write.
- **Errors:** Missing path TypeError, unsupported format ValueError, native feature/IO/schema errors.
- **Example fragment:** `df.write.format("parquet").mode("overwrite").save(path)`
- **Notes:** Default mode is overwrite. Delta has separate four-mode handling; see FILE_IO.

### `saveAsTable(self, /, name, mode=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: table identifier; mode=None uses chained mode; explicit mode overrides it.
- **Returns:** None; registers/writes the table or delegates to its external owner.
- **Errors:** Existence/schema/IO/capability/provider errors; provider commit failures may have unknown outcomes.
- **Example fragment:** `df.write.mode("error").saveAsTable("orders")`
- **Notes:** Initial Python mode is overwrite. A configured warehouse persists local data; otherwise state is local. Provider writes require complete-consumption/receipt rules.

### `save_as_table(self, /, name, mode=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: table identifier; mode=None uses chained mode; explicit mode overrides it.
- **Returns:** None; registers/writes the table or delegates to its external owner.
- **Errors:** Existence/schema/IO/capability/provider errors; provider commit failures may have unknown outcomes.
- **Example fragment:** `df.write.mode("error").saveAsTable("orders")`
- **Notes:** Initial Python mode is overwrite. A configured warehouse persists local data; otherwise state is local. Provider writes require complete-consumption/receipt rules.

## SessionBuilder

### `appName(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: application label string.
- **Returns:** Builder for chaining.
- **Errors:** Invalid argument type.
- **Example fragment:** `SparkSession.builder.appName("example")`

### `app_name(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: application label string.
- **Returns:** Builder for chaining.
- **Errors:** Invalid argument type.
- **Example fragment:** `SparkSession.builder.appName("example")`

### `config(self, /, key, value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** key and value: supported Sparkless setting, or retained compatibility key.
- **Returns:** Builder for chaining.
- **Errors:** Invalid argument types/configuration.
- **Example fragment:** `SparkSession.builder.config("spark.sql.warehouse.dir", warehouse)`
- **Notes:** Set warehouse before creating the session; see configuration precedence.

### `getOrCreate(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; configured builder settings apply to the local session selection.
- **Returns:** A session; compatible active sessions may be reused.
- **Errors:** Invalid settings or native initialization failure.
- **Example fragment:** `spark = SparkSession.builder.appName("example").getOrCreate()`
- **Notes:** Use newSession for isolated session state; stop only a session you own.

### `get_or_create(self, /)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** No arguments; configured builder settings apply to the local session selection.
- **Returns:** A session; compatible active sessions may be reused.
- **Errors:** Invalid settings or native initialization failure.
- **Example fragment:** `spark = SparkSession.builder.appName("example").getOrCreate()`
- **Notes:** Use newSession for isolated session state; stop only a session you own.

### `master(self, /, master)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `option(self, /, key, value)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark builder.option(key, value); value can be str, bool, int, etc. (stringified).

## Catalog

### `cacheTable(self, /, tableName)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `currentDatabase(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dropGlobalTempView(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** view name string.
- **Returns:** Removal result for the relevant temporary registration.
- **Errors:** Invalid/native errors.
- **Example fragment:** `spark.catalog.dropTempView("orders")`

### `dropTable(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** table name string.
- **Returns:** Removal result for the local saved-table registration.
- **Errors:** Provider/native errors may propagate.
- **Example fragment:** `spark.catalog.dropTable("orders")`
- **Notes:** Does not promise filesystem deletion of warehouse/external data.

### `dropTempView(self, /, name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** view name string.
- **Returns:** Removal result for the relevant temporary registration.
- **Errors:** Invalid/native errors.
- **Example fragment:** `spark.catalog.dropTempView("orders")`

### `getTable(self, /, tableName)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `isCached(self, /, tableName)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `listDatabases(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `listTables(self, /, dbName=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** dbName=None: optional database name.
- **Returns:** List of names, not PySpark Table objects.
- **Errors:** Provider/namespace lookup errors.
- **Example fragment:** `spark.catalog.listTables()`

### `setCurrentDatabase(self, /, name)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `tableExists(self, /, arg1, arg2=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** table name string.
- **Returns:** bool; provider failures propagate rather than becoming false.
- **Errors:** Provider/lookup error.
- **Example fragment:** `spark.catalog.tableExists("orders")`

### `uncacheTable(self, /, tableName)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## RuntimeConfig

### `get(key, default=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** key and optional default as shown in runtime signature.
- **Returns:** Stored configuration value or fallback.
- **Errors:** Missing required key or invalid argument.
- **Example fragment:** `spark.conf.get("spark.sql.caseSensitive", "false")`

### `is_case_sensitive()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `set(key, value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** key and value; only documented keys have execution effects.
- **Returns:** None; updates retained configuration.
- **Errors:** Invalid setting/native error.
- **Example fragment:** `spark.conf.set("sparkless.pyspark.compat", "4.0")`
- **Notes:** Configure schemas/warehouse before plans; Connect RuntimeConfig has a separate fixed read-only profile.

## LocalRDD

### `collect(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `collect_elements(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

Collect elements (rows when from df.rdd, or materialized list).

### `collect_elements_limited(self, /, limit)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `count(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `distinct(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark parity: return an RDD with duplicate elements removed (first occurrence kept).

### `filter(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `first(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `flatMap(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `isEmpty(self, /)`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark parity: True when the RDD has no elements.

### `map(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `reduce(self, /, func)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `take(self, /, n)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## CancellationHandle

### `cancelAllActiveJobs(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `cancel_all_active_actions(self, /)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## Window

### `currentRow`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `orderBy(*cols: 'object') -> 'WindowSpec'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `partitionBy(*cols: 'object') -> 'WindowSpec'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rangeBetween(start: 'int', end: 'int') -> 'WindowSpec'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `rowsBetween(start: 'int', end: 'int') -> 'WindowSpec'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `unboundedFollowing`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

### `unboundedPreceding`

**Documentation status: unreviewed property/descriptor.** See the support matrix; the inventory does not establish its behavior.

## functions

### `DataFrame()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `approx_count_distinct(col_or_name, rsd=0.05)`

**Documentation status: runtime description; no authored per-symbol contract.**

Approximate distinct count (PySpark approx_count_distinct). rsd is optional; when omitted, behaves like exact count_distinct with PySpark-style column naming.

### `array(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Create array column from columns (PySpark array).

### `array_compact(col_or_name: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Remove null elements from array (PySpark array_compact). Output column name matches PySpark: array_compact(col).

### `array_contains(column, value)`

**Documentation status: runtime description; no authored per-symbol contract.**

True if array contains value (PySpark array_contains).

- Literal values (int/float/bool/str) are compared directly to array elements.
- Column arguments (Column) use arrays_overlap(array_col, array(value_col)) so join
  conditions like df.filter(F.array_contains("tags", other_col)) work.

### `array_distinct(col_or_name: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Distinct elements in array (PySpark array_distinct). Output column name matches PySpark: array_distinct(col).

### `array_position(col_or_name: 'ColumnOrName', value: 'Union[ColumnOrName, int, float, bool, str]') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

1-based index of first occurrence of value in list, or 0 if not found. PySpark: F.array_position(col, value).

### `array_remove(col_or_name: 'ColumnOrName', value: 'Union[ColumnOrName, int, float, bool, str]') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Remove all elements equal to value from the array. PySpark: F.array_remove(col, element).

### `arrays_overlap(col1, col2)`

**Documentation status: runtime description; no authored per-symbol contract.**

True if two array columns have any element in common (PySpark arrays_overlap).

### `asc(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Sort key for ascending order in Window.orderBy.

### `asinh(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Inverse hyperbolic sine (PySpark asinh).

### `atanh(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Inverse hyperbolic tangent (PySpark atanh).

### `avg(c)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column: name or Column; count(column) counts non-null values.
- **Returns:** Aggregate Column for select/agg.
- **Errors:** Unsupported input type, missing column or aggregate context.
- **Example fragment:** `F.avg("amount").alias("result")`

### `cast(col_or_name: 'ColumnOrName', data_type: 'object') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Module-level cast for PySpark parity.

In real PySpark, `pyspark.sql.functions.cast` does not accept a `DataType` object
(only strings / SQL type names). Some tests assert that passing a `DataType`
fails, so we explicitly raise here to match that behavior.

### `coalesce(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Return first non-null value from columns (PySpark coalesce).

### `col(name)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** name: field-name string.
- **Returns:** Column reference expression.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.col("id")`

### `concat(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Concatenate columns as strings (PySpark concat).

### `concat_ws(separator, *cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Concatenate columns with separator (PySpark concat_ws).

### `cosh(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Hyperbolic cosine (PySpark cosh).

### `count(c='*')`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column: name or Column; count(column) counts non-null values.
- **Returns:** Aggregate Column for select/agg.
- **Errors:** Unsupported input type, missing column or aggregate context.
- **Example fragment:** `F.count("amount").alias("result")`

### `countDistinct(*cols)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `crc32(c: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `create_map(*cols: 'Any') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Build a map column from alternating key/value expressions (PySpark create_map).

With no args or create_map([]), returns a column of empty maps per row.

### `cume_dist() -> '_CumeDistExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window cume_dist() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `current_catalog()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `current_database()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `current_date()`

**Documentation status: runtime description; no authored per-symbol contract.**

Return current date as column expression (PySpark parity: no session required at build time).

### `current_schema()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `current_timestamp()`

**Documentation status: runtime description; no authored per-symbol contract.**

Return current timestamp as column expression (PySpark parity: no session required at build time).

### `current_user()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `date_add(column, n)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `date_format(column, fmt)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `date_sub(column, n)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `date_trunc(format, column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Truncate date/timestamp to unit (PySpark date_trunc).

### `datediff(end, start)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dayofmonth(column)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dayofweek(column)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `dense_rank() -> '_DenseRankExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window dense_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `desc(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Sort key for descending order in Window.orderBy.

### `explode(col_or_name: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

Explode array into one row per element (PySpark explode).

### `expr(sql_expr: 'str')`

**Documentation status: runtime description; no authored per-symbol contract.**

F.expr() support: SQL expression string resolved in select() context (PySpark parity).
- REGEXP/RLIKE: "col REGEXP 'pat'" -> col(col).rlike(pat)
- Other expressions (e.g. "upper(x) as up") -> expr_str; resolved when used in df.select().
Requires active SparkSession.

### `first(col_or_name: 'ColumnOrName', ignorenulls: 'bool' = False) -> '_ColumnType'`

**Documentation status: runtime description; no authored per-symbol contract.**

First value in group (PySpark first); use in groupBy().agg(). Default ignorenulls=False.

### `first_value(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Window first_value(col) expression; use with .over(Window.partitionBy(...)).

### `floor(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Floor to nearest integer (PySpark floor).

### `format_string(fmt, *cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Printf-style format (PySpark format_string).

### `from_unixtime(column, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `get_json_object(column, path)`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract JSON path from string column (PySpark get_json_object).

### `greatest(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Greatest of columns per row (PySpark greatest).

### `hour(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract hour from datetime (PySpark hour).

### `input_file_name()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `isnotnull(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Return a boolean column that is true when the column is not null. PySpark: F.isnotnull(col).

### `isnull(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Return a boolean column that is true when the column is null. PySpark: F.isnull(col).

### `json_tuple(column: 'ColumnOrName', *keys: 'str') -> 'Union[_ColumnType, Tuple[_ColumnType, ...]]'`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract keys from JSON as columns (PySpark json_tuple).

Returns one string column per key, named c0, c1, ... in the order of keys.
With a single key, returns one Column (for withColumn); multiple keys return a tuple
that select() flattens into separate columns.

### `lag(col_or_name, offset=1)`

**Documentation status: runtime description; no authored per-symbol contract.**

Window lag(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...)).

### `last_day(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Last day of month for date column (PySpark last_day).

### `last_value(col_or_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Window last_value(col) expression; use with .over(Window.partitionBy(...)).

### `lead(col_or_name, offset=1)`

**Documentation status: runtime description; no authored per-symbol contract.**

Window lead(col, offset) expression; use with .over(Window.partitionBy(...).orderBy(...)).

### `least(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Least of columns per row (PySpark least).

### `length(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

String length in characters (PySpark length).

### `levenshtein(column, other)`

**Documentation status: runtime description; no authored per-symbol contract.**

Levenshtein distance between two strings (PySpark levenshtein).

### `lit(value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** value: supported scalar literal.
- **Returns:** Literal Column.
- **Errors:** TypeError/ValueError for invalid arguments; SparklessError for native planning or execution failures. Invalid names/types may fail at an action.
- **Example fragment:** `F.lit("paid")`

### `lit_bool(value)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lit_f64(value)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lit_i64(value)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lit_null(dtype)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lit_str(value)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `lower(col)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `ltrim(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Trim leading whitespace (PySpark ltrim).

### `max(c)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column: name or Column; count(column) counts non-null values.
- **Returns:** Aggregate Column for select/agg.
- **Errors:** Unsupported input type, missing column or aggregate context.
- **Example fragment:** `F.max("amount").alias("result")`

### `mean(col_or_name: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `min(c)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column: name or Column; count(column) counts non-null values.
- **Returns:** Aggregate Column for select/agg.
- **Errors:** Unsupported input type, missing column or aggregate context.
- **Example fragment:** `F.min("amount").alias("result")`

### `minute(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract minute from datetime (PySpark minute).

### `month(column)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `months_between(end, start, round_off=True)`

**Documentation status: runtime description; no authored per-symbol contract.**

Months between end and start dates (PySpark months_between).

### `ntile(n: 'int') -> '_NtileExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window ntile(n) expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `nullif(col1, col2)`

**Documentation status: runtime description; no authored per-symbol contract.**

Return null if col1 equals col2, else col1. PySpark: F.nullif(col1, col2).

### `nvl(col_or_name, replacement)`

**Documentation status: runtime description; no authored per-symbol contract.**

Replace null with replacement. PySpark: F.nvl(col, replacement).

### `percent_rank() -> '_PercentRankExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window percent_rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `posexplode(col)`

**Documentation status: runtime description; no authored per-symbol contract.**

Explode array with position (PySpark posexplode). Returns (pos_col, val_col) or wrapper with .alias().

### `printf(fmt, *cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Printf-style format (PySpark format_string).

### `rank() -> '_RankExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window rank() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `regexp_extract(column, pattern, idx=0)`

**Documentation status: runtime description; no authored per-symbol contract.**

Extract first regex match (PySpark regexp_extract). idx=0 is full match, 1+ is capture group.

### `regexp_extract_all(column, pattern, idx=0)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `regexp_like(column, pattern)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `regexp_replace(column, pattern, replacement)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `repeat(column, n)`

**Documentation status: runtime description; no authored per-symbol contract.**

Repeat string n times (PySpark repeat).

### `round(column, scale=0)`

**Documentation status: runtime description; no authored per-symbol contract.**

Round to given decimal places (PySpark round). scale=0 by default.

### `row_number() -> '_RowNumberExpr'`

**Documentation status: runtime description; no authored per-symbol contract.**

Window row_number() expression; use with .over(Window.partitionBy(...).orderBy(...)). Requires active SparkSession.

### `sinh(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Hyperbolic sine (PySpark sinh).

### `size(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Number of elements in array column (PySpark size).

### `soundex(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

American Soundex code (PySpark soundex).

### `split(column, pattern, limit=-1)`

**Documentation status: runtime description; no authored per-symbol contract.**

Split string by delimiter (PySpark split). limit=-1 means no limit.

### `stddev(col_or_name)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `struct(*cols)`

**Documentation status: runtime description; no authored per-symbol contract.**

Create struct from columns (PySpark struct).

### `substring(column, start, length)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `substring_index(column, delimiter, count)`

**Documentation status: runtime description; no authored per-symbol contract.**

Substring before/after nth delimiter (PySpark substring_index). count > 0: before nth from left; count < 0: after nth from right.

### `sum(c)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** column: name or Column; count(column) counts non-null values.
- **Returns:** Aggregate Column for select/agg.
- **Errors:** Unsupported input type, missing column or aggregate context.
- **Example fragment:** `F.sum("amount").alias("result")`

### `timestamp_seconds(column)`

**Documentation status: runtime description; no authored per-symbol contract.**

Convert seconds since epoch to timestamp (PySpark timestamp_seconds).

### `to_date(column, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `to_timestamp(column, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `to_timestamp_ltz(column, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `to_timestamp_ntz(column, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `to_utc_timestamp(column, tz)`

**Documentation status: runtime description; no authored per-symbol contract.**

Interpret timestamp as in tz, convert to UTC (PySpark to_utc_timestamp).

### `translate(column, from_str, to_str)`

**Documentation status: runtime description; no authored per-symbol contract.**

Character-by-character translation (PySpark translate).

### `trim(col)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `try_add(left, right)`

**Documentation status: runtime description; no authored per-symbol contract.**

Add that returns null on overflow (PySpark try_add).

### `try_cast(column, type_name)`

**Documentation status: runtime description; no authored per-symbol contract.**

Cast to type, null on invalid (PySpark try_cast).

### `udf(f=None, returnType=None)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** f=None callable; returnType=None defaults to string; use an explicit DataType object.
- **Returns:** Callable that builds a local scalar UDF Column.
- **Errors:** Python callback/conversion errors; unsupported contexts.
- **Example fragment:** `double = F.udf(lambda x: None if x is None else x * 2, T.LongType())`
- **Notes:** Materializes input; no public pandas/grouped/remote UDF contract.

### `unix_timestamp(column=None, fmt=None)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `upper(col)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `when(condition, value)`

**Documentation status: authored local contract.** Supported contexts still follow the support matrix.

- **Parameters:** condition: Boolean Column; value: supported value or Column.
- **Returns:** Conditional builder; chain when/otherwise, not then.
- **Errors:** Invalid condition/value or incompatible branch types.
- **Example fragment:** `F.when(F.col("id") > 1, F.lit("large")).otherwise(F.lit("small"))`

### `xxhash64(c: 'ColumnOrName') -> '_ColumnType'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `year(column)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

## types

### `ArrayType(elementType: 'Optional[DataType]' = None, containsNull: 'bool' = True, *, _element_type: 'Optional[DataType]' = None, nullable: 'Optional[bool]' = None) -> 'None'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `BooleanType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `CharType(length: 'int' = 1)`

**Documentation status: runtime description; no authored per-symbol contract.**

Char(n) type; we treat as string for schema parsing.

### `DataType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `DateType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `DayTimeIntervalType(start_field: 'str' = 'day', end_field: 'str' = 'second')`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark 4 day-time interval type.

### `DecimalType(precision: 'int' = 10, scale: 'int' = 0)`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `DoubleType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `FloatType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `IntegerType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `IntervalType(start_field: 'str' = 'month', end_field: 'str' = 'month')`

**Documentation status: runtime description; no authored per-symbol contract.**

Interval type; we treat as string for schema parsing.

### `LongType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `MapType(keyType: 'DataType', valueType: 'DataType', valueContainsNull: 'bool' = True) -> 'None'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `Row(*args: 'RowValue', **kwargs: 'RowValue') -> "'Row'"`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark-like Row type returned by DataFrame.collect().

Contract: collect() returns a list of Row. Each Row preserves types (int, float, bool,
str, None, date, datetime, list, dict). Use row[name], row[idx], row.asDict(), or
attribute access (row.column_name). Row is tuple-like (iterable, indexable by int).

### `ShortType()`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark ShortType (16-bit integer).

### `StringType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `StructField(name: 'str', dataType: 'DataType', nullable: 'bool' = True, metadata: 'Optional[StructMetadata]' = None) -> 'None'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `StructType(fields: 'Optional[List[StructField]]' = None) -> 'None'`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `TimestampNTZType()`

**Documentation status: runtime description; no authored per-symbol contract.**

Timestamp without timezone conversion, matching Spark's TIMESTAMP_NTZ.

### `TimestampType()`

**Documentation status: unreviewed.** No runtime docstring or authored contract. Behavior outside the support matrix is unknown; inspect source and add an oracle regression before depending on this symbol.

### `VarcharType(length: 'int' = 1)`

**Documentation status: runtime description; no authored per-symbol contract.**

Varchar(n) type; we treat as string for schema parsing.

### `VariantType()`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark 4 semi-structured VARIANT type (stored as JSON string in Sparkless).

### `YearMonthInterval(months: 'int' = 0)`

**Documentation status: runtime description; no authored per-symbol contract.**

Collected year-month interval value (PySpark 4.0 shape).

### `YearMonthIntervalType(start_field: 'str' = 'year', end_field: 'str' = 'month')`

**Documentation status: runtime description; no authored per-symbol contract.**

PySpark 4 year-month interval type (collect shape differs from 3.x).

## sparkless_connect.server

### `AsyncSparklessServer(host: 'str' = '127.0.0.1', port: 'int' = 0, executable_path: 'Optional[str]' = None, startup_timeout: 'float' = 30.0, shutdown_timeout: 'float' = 10.0, kill_timeout: 'float' = 5.0, env: 'Optional[Dict[str, str]]' = None, connect_host: 'str' = '127.0.0.1', connect_port: 'Optional[int]' = 0, product: 'str' = 'base') -> 'None'`

**Documentation status: runtime description; no authored per-symbol contract.**

An asyncio owner for one ``sparkless-server`` child process.

### `SparklessConnect(*, server_options: 'Optional[Mapping[str, Any]]' = None) -> 'None'`

**Documentation status: runtime description; no authored per-symbol contract.**

Own a Spark Connect server and one real pyspark-client session.

### `SparklessServer(host: 'str' = '127.0.0.1', port: 'int' = 0, executable_path: 'Optional[str]' = None, startup_timeout: 'float' = 30.0, shutdown_timeout: 'float' = 10.0, kill_timeout: 'float' = 5.0, env: 'Optional[Dict[str, str]]' = None, connect_host: 'str' = '127.0.0.1', connect_port: 'Optional[int]' = 0, product: 'str' = 'base') -> 'None'`

**Documentation status: runtime description; no authored per-symbol contract.**

A synchronous owner for one ``sparkless-server`` child process.

### `SparklessServerError(native signature unavailable)`

**Documentation status: runtime description; no authored per-symbol contract.**

Raised when the owned native service cannot satisfy its contract.

### `UnsupportedServerPlatformError(native signature unavailable)`

**Documentation status: runtime description; no authored per-symbol contract.**

Raised when no packaged server executable exists for this platform.

### `UnsupportedSparkConnectError(native signature unavailable)`

**Documentation status: runtime description; no authored per-symbol contract.**

Raised when Spark Connect is disabled or unavailable in the server binary.

## external_catalog

### `ArrowIpcStream(native signature unavailable)`

**Documentation status: runtime description; no authored per-symbol contract.**

Arrow IPC stream bytes, compatible with Python's buffer protocol.

This is a bytes subclass so existing ``pyarrow.ipc.open_stream(data)`` calls
keep working. ``data.bytes`` exposes the Rust payload's named field as well.

### `BatchWritableExternalCatalogProvider(*args, **kwargs)`

**Documentation status: runtime description; no authored per-symbol contract.**

Provider interface that consumes bounded PyArrow RecordBatches lazily.

### `ExternalCatalogOwnership(owner_id: 'str', read: 'bool' = True, write: 'bool' = False, batch_write: 'bool' = False) -> None`

**Documentation status: runtime description; no authored per-symbol contract.**

Owner/capabilities for an identifier; return None when the local catalog owns it.

### `ExternalCatalogProvider(*args, **kwargs)`

**Documentation status: runtime description; no authored per-symbol contract.**

Python catalog callback surface used to build separate Rust capabilities.

``resolve_ownership`` is called before table lookup. Return None only when Sparkless'
local catalog should handle the identifier. Writable providers implement
``write_table_batches``; read-only objects may omit it.

### `ExternalTableDescriptor(table_id: 'str', canonical_location: 'str', schema: 'T.StructType', provider: 'str', version: 'Optional[int]' = None, permissions_context: 'Optional[str]' = None, format: 'Union[ExternalTableFormat, str]' = <ExternalTableFormat.DELTA: 'delta'>) -> None`

**Documentation status: runtime description; no authored per-symbol contract.**

Catalog identity, logical schema, and optional pinned Delta version.

### `ExternalTableFormat(value, names=None, *, module=None, qualname=None, type=None, start=1, boundary=None)`

**Documentation status: runtime description; no authored per-symbol contract.**

Storage formats supported by the native external catalog contract.

### `ExternalTableSaveMode(value, names=None, *, module=None, qualname=None, type=None, start=1, boundary=None)`

**Documentation status: runtime description; no authored per-symbol contract.**

Save modes accepted by DataFrameWriter.mode().

### `ExternalTableWriteRequest(operation_id: 'str', identifier: 'str', table: 'ExternalTableDescriptor', mode: 'Union[ExternalTableSaveMode, str]', format: 'Optional[str]', options: 'List[Tuple[str, str]]', partition_by: 'List[str]', batches: 'Iterator[Any]') -> None`

**Documentation status: runtime description; no authored per-symbol contract.**

A resolved write using a lazy sequence of bounded Arrow batches.

``batches`` is a lazy iterator of PyArrow RecordBatches. Closing or exhausting
it releases the producer; a callback may stop early by closing it. Options are ordered pairs
so duplicate keys survive. A successful callback must commit the data it
accepts before returning its receipt. A callback failure after it starts may have an
unknown commit outcome and is retained as such by the native layer.

### `ExternalTableWriteResult(table: 'ExternalTableDescriptor', committed_version: 'Optional[int]' = None, provider_commit_id: 'Optional[str]' = None) -> None`

**Documentation status: runtime description; no authored per-symbol contract.**

Descriptor and provider-specific identity of a completed commit.

### `SparklessExternalCatalogError(native signature unavailable)`

**Documentation status: runtime description; no authored per-symbol contract.**

External catalog provider callback failed

## Documentation coverage

140 of 421 inventoried entries have authored contracts.
Other entries explicitly retain their runtime-only or unreviewed status.
These counts measure documentation coverage, not engine parity or passing tests.
