# PySpark vs Robin-Sparkless: Known Differences

This document lists **intentional or known divergences** from PySpark semantics in robin-sparkless. Robin-sparkless aims for behavioral parity where practical; when perfect parity is impossible or deferred, we document it here.

**Unimplemented API surface:** For a full list of functions and methods present in Sparkless 3.28.0 but not yet implemented in robin-sparkless, see [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md). That list is **scoped to PySpark parity**: all listed items are standard PySpark APIs (or direct Sparkless equivalents); see [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md) for the canonical “missing vs PySpark” list with PySpark references.

## Window functions

- **percent_rank, cume_dist, ntile, nth_value**: The **API** is implemented (Rust and Python). Parity fixtures for these (`percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window`) are **covered** via a multi-step workaround in the harness (computing in separate columns then combining). See [PARITY_STATUS.md](PARITY_STATUS.md).

## GroupBy

- **Null keys and empty groups**: groupBy + aggregates are tested with fixtures `groupby_null_keys`, `groupby_single_group`, and `groupby_single_row_groups`. Behavior is aligned with PySpark for these cases (nulls in grouping keys produce one group per null; single-group and single-row groups behave as in PySpark). Any future divergence discovered will be listed here.

## SQL (optional `sql` feature)

- **Supported**: single `SELECT`, `FROM` (single table or JOIN), `WHERE`, `GROUP BY` + aggregates, `HAVING`, `ORDER BY`, `LIMIT`, and temporary views (`createOrReplaceTempView`, `table()`). Unsupported constructs produce clear errors.
- **Unsupported (tracked in #141)**: DDL (`CREATE/DROP DATABASE`, `CREATE/DROP TABLE`, `CREATE SCHEMA`, `SET CURRENT DATABASE`, etc.), DML (`INSERT INTO`, `UPDATE`, `DELETE FROM`), subqueries in `FROM`, CTEs.

## Delta Lake (optional `delta` feature)

- **Supported**: Read by path/version, overwrite, and append. See [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) §7.2.
- **Unsupported (tracked in #152)**: Schema evolution (e.g. add columns, change types under Delta rules) and MERGE (upsert with whenMatchedUpdate/whenNotMatchedInsert). Implement when Delta usage requires them.

## Array

- **array_distinct order**: Implemented with first-occurrence order to match PySpark (via UDF; parity fixture enabled).

## Control functions (assert_true, raise_error)

- **assert_true(expr)**: In PySpark, `assert_true` fails the job if `expr` is false for any row. In robin-sparkless, `assert_true` is implemented as an expression that **returns the original boolean column** but **fails evaluation** (returns an error) if **any value is explicitly `false`**. Nulls are allowed and do not trigger failure. This behavior matches the intent of PySpark's `assert_true` for typical uses, but details of error messages and null handling may differ.
- **raise_error(msg)**: In PySpark, `raise_error` produces an expression that always fails when evaluated. In robin-sparkless, `raise_error(msg)` is implemented as an expression that **always returns an error** with a message prefixed by `\"raise_error:\"`. The result type is an `Int64` expression that never materializes successfully.

## DataFrame: cube, rollup, write, and stubs

- **cube / rollup**: Implemented. `df.cube("a", "b").agg(...)` and `df.rollup("a", "b").agg(...)` run multiple grouping sets and union results (missing keys become null), matching PySpark semantics.
- **write**: Implemented. `df.write().mode("overwrite"|"append").format("parquet"|"csv"|"json").save(path)` uses Polars IO. Append for JSON is supported (NDJSON/JsonLines).
- **data**: Returns the same as `collect()` (list of row dicts). Best-effort local collection; no RDD.
- **toLocalIterator**: Returns the same as `collect()` (an iterable of rows). Best-effort local iterator.
- **rdd**: Stub; raises `NotImplementedError` ("RDD is not supported in Sparkless").
- **foreach**, **foreachPartition**: Stub; raise `NotImplementedError`.
- **mapInPandas**, **mapPartitions**: Stub; raise `NotImplementedError`.
- **storageLevel**: Stub; returns `None` (eager execution; no storage level).
- **isStreaming**: Always returns `False`; streaming is not supported.
- **withWatermark**: No-op; returns self. Streaming/watermark not supported.
- **persist** / **unpersist**: No-op; return self (execution is eager).

## SparkSession.createDataFrame / create_dataframe

- **PySpark** `createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)` accepts many input types: list of tuples (any length, types inferred or from schema), list of dicts, list of Row, RDD, pandas DataFrame; `schema` can be a list of column names (types inferred) or a StructType.
- **Robin-sparkless** has two entry points:
  - **`create_dataframe(data, column_names)`**: Accepts **only** a list of 3-tuples `(int, int, str)` and exactly three column names (e.g. `["id", "age", "name"]`). This is a convenience for the common simple case; it does **not** accept arbitrary tuple lengths or mixed types.
  - **`create_dataframe_from_rows(data, schema)`**: Accepts list of dicts (keys = column names) or list of lists (values in schema order), and `schema` as list of `(name, dtype_str)` e.g. `[("id", "bigint"), ("name", "string")]`. This matches PySpark when you have explicit schema with types. Supported dtypes: bigint, int, long, double, float, string, boolean, date, timestamp, etc.
- **For PySpark/Sparkless parity**: Use **`create_dataframe_from_rows(data, schema)`** for arbitrary column counts and types (e.g. from Sparkless or plan interpreter). Use **`create_dataframe(data, column_names)`** only when your data is exactly 3 columns `(int, int, str)`.
- **Alias**: `createDataFrame` is exposed as an alias of `create_dataframe` so that `spark.createDataFrame(data, ["id", "age", "name"])` works for the 3-tuple case; for other schemas use `create_dataframe_from_rows`.

## JVM / runtime stubs

The following JVM- or runtime-related functions are implemented as **stubs for API compatibility**, not full equivalents of PySpark behavior:

- **broadcast(df)**: Returns the same `DataFrame` unchanged. It is a **no-op hint**; there is no optimizer that takes broadcast hints into account.
- **spark_partition_id()**: Returns a **constant 0** for all rows, rather than the actual Spark partition id. This is sufficient for tests that only require the function to exist but does not model Spark's partitioning behavior.
- **input_file_name()**: Returns an **empty string** for all rows. File path information is not tracked on a per-row basis.
- **monotonically_increasing_id()**: Returns a **constant 0** for all rows, rather than a strictly increasing 64-bit id. This is a compatibility stub; code that relies on uniqueness should not use this stub.
- **current_catalog() / current_database() / current_schema()**: Return constant strings (`\"spark_catalog\"`, `\"default\"`, `\"default\"` respectively). There is no catalog or database concept in robin-sparkless.
- **current_user() / user()**: Return the constant string `\"unknown\"`. The actual OS or session user is not surfaced.

## Random functions (rand, randn)

- **rand(seed)** / **randn(seed)**: In PySpark, these return a column with **one distinct value per row** and optional seeding. In robin-sparkless, when you add the column via **`with_column`** or **`with_columns`** (Rust or Python), the implementation generates a full-length random series so you get **one value per row** (PySpark-like). Optional `seed` (e.g. `rand(42)`) gives reproducible results. Use `df.with_column("r", rand(42))` or `df.with_columns({"r": rand(42)})`; if you use the expression in other contexts (e.g. select only), per-row semantics are not guaranteed.

## Crypto (AES)

- **aes_encrypt / aes_decrypt / try_aes_decrypt**: Implemented using **AES-128-GCM**. Output format is **hex(nonce || ciphertext)** where nonce is 12 bytes (random per encryption) and ciphertext includes the GCM tag. PySpark 3.5+ defaults to GCM; key is taken as UTF-8 string (first 16 bytes used). Decryption returns null on failure (invalid hex, wrong key, or tampered data).

## Phase 10 & Phase 8 – Implemented

**All previously stubbed Phase 8 items are now implemented (February 2026):**

- **String 6.4**: `mask`, `translate`, `substring_index`; **`soundex`, `levenshtein`, `crc32`, `xxhash64`** (via Expr::map / map_many UDFs with strsim, crc32fast, twox-hash, soundex crates).
- **Array extensions**: `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`; **`array_flatten`, `array_repeat`** (via Expr::map UDFs).
- **Map (6b)**: **`create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays`** (Map as List(Struct{key, value}); create_map via as_struct/concat_list; map_keys/map_values via list.eval + struct.field; map_from_arrays via UDF).
- **JSON**: `get_json_object`, `from_json`, `to_json` (Polars extract_jsonpath / dtype-struct).
- **Window fixtures**: percent_rank, cume_dist, ntile, nth_value covered via multi-step workaround.
- **Phase 16 string/regex**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf` — all implemented.
- **Phase 17 datetime/unix**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial` — all implemented. Note: `unix_timestamp` and `from_unixtime` use chrono; results may differ from PySpark when session timezone differs from system timezone.
- **Phase 22 datetime**: `curdate`, `now`, `localtimestamp`, `date_diff`, `dateadd`, `datepart`, `extract`, `unix_micros`, `unix_millis`, `unix_seconds`, `dayname`, `weekday`, `make_timestamp`, `make_interval`, `timestampadd`, `timestampdiff`, `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone`, `current_timezone`, `to_timestamp` — all implemented. Note: `from_utc_timestamp` and `to_utc_timestamp` for UTC-stored timestamps return identity (instant unchanged); full timezone conversion semantics may differ from PySpark.
- **Phase 18 array/map/struct**: `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union`, `map_concat`, `map_from_entries`, `map_contains_key`, `get`, `struct`, `named_struct`, **`map_filter`**, **`map_zip_with`**, **`zip_with`** — all implemented. Uses Expr-based predicates/merge. Python: `map_filter_value_gt`, `zip_with_coalesce`, `map_zip_with_coalesce`.
- **Phase 19 aggregates/try/misc**: **`any_value`**, **`bool_and`**, **`bool_or`**, **`every`/`some`**, **`count_if`**, **`max_by`**, **`min_by`**, **`percentile`**, **`product`**, **`collect_list`**, **`collect_set`**; **`try_divide`**, **`try_add`**, **`try_subtract`**, **`try_multiply`**, **`try_element_at`**; **`width_bucket`**, **`elt`**, **`bit_length`**, **`typeof`** — all implemented. `percentile_approx` deferred (complex).
- **Phase 20 ordering/aggregates/numeric**: **`asc`**, **`asc_nulls_first`**, **`asc_nulls_last`**, **`desc`**, **`desc_nulls_first`**, **`desc_nulls_last`**; **`median`**, **`mode`**; **`stddev_pop`**, **`stddev_samp`**, **`var_pop`**, **`var_samp`**; **`try_sum`**, **`try_avg`**; **`bround`**, **`negate`**, **`negative`**, **`positive`**; **`cot`**, **`csc`**, **`sec`**; **`e`**, **`pi`**; **`covar_pop`**, **`covar_samp`**, **`corr`** (groupBy agg), **`kurtosis`**, **`skewness`**; **`approx_percentile`**, **`percentile_approx`** — all implemented.
- **Phase 21 string/binary/type/array/map/struct**: **`btrim`**, **`locate`**, **`conv`**; **`hex`**, **`unhex`**, **`bin`**, **`getbit`**; **`decode`**, **`encode`**, **`to_binary`**, **`try_to_binary`**; **`to_char`**, **`to_varchar`**, **`to_number`**, **`try_to_number`**, **`try_to_timestamp`**; **`str_to_map`**; **`arrays_overlap`**, **`arrays_zip`**, **`explode_outer`**, **`posexplode_outer`**, **`array_agg`**; **`transform_keys`**, **`transform_values`** — all implemented. Deferred: `aggregate` (array fold). PyO3: `transform_keys` and `transform_values` require Expr and are Rust-only for now.
- **Phase 23 JSON/URL/misc**: **`isin`**, **`isin_i64`**, **`isin_str`**; **`url_decode`**, **`url_encode`**; **`json_array_length`**, **`parse_url`**; **`hash`** (Murmur3 32-bit for PySpark parity); **`shift_left`**, **`shift_right`**, **`shift_right_unsigned`**; **`version`**; **`equal_null`**; **`stack`**; **`from_csv`**, **`to_csv`**, **`schema_of_csv`**, **`schema_of_json`**; **`get_json_object`**, **`json_tuple`** — all implemented. Deferred: `json_object_keys`.

## Optional / deferred (XML, XPath, sentences, RDD, UDF, Catalog, Streaming, sketch, JVM stubs)

The following are **not implemented** or are **stubs**; tracked in GitHub issues for parity:

- **RDD / distributed (#142)**: RDD and distributed execution APIs — not supported; `rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions` raise `NotImplementedError`.
- **UDF / UDTF (#143)**: User-defined functions and table functions — not implemented; use built-in expressions and plan interpreter where possible.
- **Catalog / DataFrameWriterV2 (#144)**: `writeTo`, catalog tables, `CREATE TABLE`-style DDL — not implemented; use `df.write().format(...).save(path)`.
- **Structured Streaming (#145)**: Not supported; `isStreaming` returns false, `withWatermark` is a no-op.
- **XML / XPath (#146)**: `from_xml`, `to_xml`, `schema_of_xml`, `xpath*` — would require an XML parser and feature flag; deferred.
- **Sketch aggregates (#147)**: Approximate aggregates (e.g. HyperLogLog, count-min sketch) — not implemented.
- **sentences / NLP (#148)**: `sentences` and JVM/UDTF helpers — deferred; could be implemented as string split + list of lists.
- **JVM / runtime stubs (#154)**: See section **JVM / runtime stubs** above — `broadcast`, `spark_partition_id`, `input_file_name`, `monotonically_increasing_id`, `current_catalog`, `current_user`, etc. are stubs for API compatibility.

See [ROADMAP.md](ROADMAP.md) and [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) for the full list.
