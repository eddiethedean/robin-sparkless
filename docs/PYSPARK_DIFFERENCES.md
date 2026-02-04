# PySpark vs Robin-Sparkless: Known Differences

This document lists **intentional or known divergences** from PySpark semantics in robin-sparkless. Robin-sparkless aims for behavioral parity where practical; when perfect parity is impossible or deferred, we document it here.

**Unimplemented API surface:** For a full list of functions and methods present in Sparkless 3.28.0 but not yet implemented in robin-sparkless, see [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md).

## Window functions

- **percent_rank, cume_dist, ntile, nth_value**: The **API** is implemented (Rust and Python). Parity fixtures for these (`percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window`) are **covered** via a multi-step workaround in the harness (computing in separate columns then combining). See [PARITY_STATUS.md](PARITY_STATUS.md).

## GroupBy

- **Null keys and empty groups**: groupBy + aggregates are tested with fixtures `groupby_null_keys`, `groupby_single_group`, and `groupby_single_row_groups`. Behavior is aligned with PySpark for these cases (nulls in grouping keys produce one group per null; single-group and single-row groups behave as in PySpark). Any future divergence discovered will be listed here.

## SQL (optional `sql` feature)

- **Unsupported constructs**: No subqueries in `FROM`, no CTEs, no DDL, no `HAVING`. Unsupported constructs should produce clear errors. Supported: single `SELECT`, `FROM` (single table or JOIN), `WHERE`, `GROUP BY` + aggregates, `ORDER BY`, `LIMIT`, and temporary views (`createOrReplaceTempView`, `table()`).

## Delta Lake (optional `delta` feature)

- **Deferred**: Schema evolution and MERGE are not implemented. Read by path/version, overwrite, and append are supported. See [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) §7.2.

## Array

- **array_distinct order**: Polars `list().unique()` may return distinct elements in a different order than PySpark `array_distinct`, which preserves first-occurrence order. The `array_distinct` parity fixture is skipped due to this ordering difference.

## Control functions (assert_true, raise_error)

- **assert_true(expr)**: In PySpark, `assert_true` fails the job if `expr` is false for any row. In robin-sparkless, `assert_true` is implemented as an expression that **returns the original boolean column** but **fails evaluation** (returns an error) if **any value is explicitly `false`**. Nulls are allowed and do not trigger failure. This behavior matches the intent of PySpark's `assert_true` for typical uses, but details of error messages and null handling may differ.
- **raise_error(msg)**: In PySpark, `raise_error` produces an expression that always fails when evaluated. In robin-sparkless, `raise_error(msg)` is implemented as an expression that **always returns an error** with a message prefixed by `\"raise_error:\"`. The result type is an `Int64` expression that never materializes successfully.

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
- **Phase 23 JSON/URL/misc**: **`isin`**, **`isin_i64`**, **`isin_str`**; **`url_decode`**, **`url_encode`**; **`json_array_length`**, **`parse_url`**; **`hash`**; **`shift_left`**, **`shift_right`**, **`shift_right_unsigned`**; **`version`**; **`equal_null`**; **`stack`** — all implemented. Note: `hash` uses xxHash64 (PySpark uses Murmur3); results will differ. Deferred: `json_object_keys`, `json_tuple`, `from_csv`, `to_csv`, `schema_of_csv`, `schema_of_json`.

See [ROADMAP.md](ROADMAP.md) and [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) for the full list.
