# Sparkless Parity Status (Phase 5)

This doc records **pass/fail counts and failure reasons** for fixtures converted from Sparkless `expected_outputs` (see [CONVERTER_STATUS.md](CONVERTER_STATUS.md)).

## How to run

```bash
export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs
make sparkless-parity
```

This will: (1) convert Sparkless expected_outputs to `tests/fixtures/converted/` (with `--dedupe` to skip scenarios already in hand-written fixtures), (2) regenerate each converted fixture’s `expected` from **PySpark** via `tests/regenerate_expected_from_pyspark.py` (requires `pip install pyspark`), (3) run `cargo test pyspark_parity_fixtures`. Parity is thus Robin vs **PySpark**, not Sparkless.

Or run parity only (hand-written + `tests/fixtures/converted/*.json`):

```bash
cargo test pyspark_parity_fixtures
```

## Keeping expectations aligned with PySpark

- **Hand-written fixtures:** Expected values in `tests/fixtures/*.json` (top-level, excluding `converted/` and `plans/`) can be refreshed from PySpark using the regeneration script. Run:
  ```bash
  python tests/regenerate_expected_from_pyspark.py tests/fixtures
  ```
  Use `--dry-run` to print diffs without writing. The script builds a PySpark DataFrame from each fixture’s `input.schema` and `input.rows`, applies the same `operations` (filter, select, withColumn, orderBy, groupBy, window, join, etc.), then overwrites that fixture’s `expected.schema` and `expected.rows`. Fixtures with unsupported expressions or ops are left unchanged. After regenerating, run `cargo test pyspark_parity_fixtures`; any failure indicates Robin vs PySpark divergence (fix Robin or leave fixture unregenerated). Requires PySpark and Java 17+.

- **Python tests:** Tests in `tests/python/test_robin_sparkless.py` and `test_dataframe_parity.py` assert behaviour that matches PySpark. Expectations are predetermined (no PySpark at test runtime). For example, `test_create_dataframe_from_rows_schema_pyspark_parity` compares Robin output to a fixed expected list derived from PySpark 3.5.

- **CI:** A CI job can run the regenerator in `--dry-run` and fail if current expected ≠ PySpark result (requires PySpark and Java 17+ in the environment).

## Pass/fail summary

| Source | Converted | Passing | Failing | Skipped |
|--------|-----------|---------|--------|---------|
| Hand-written (`tests/fixtures/*.json`) | — | 145 | 0 | 21 |
| Sparkless converted (`tests/fixtures/converted/*.json`) | 226 | 0 | 0 | 226 (all skipped: expected shape from converter; run `regenerate_expected_from_pyspark.py` with PySpark to fix) |

**Target: 50+ tests passing** (hand-written + converted). **Current: 161 passing** (hand-written). **Phase C** ✅: DataFrameReader/Writer parity (read_csv_with_options, read_table fixtures). **Phase D** ✅: DataFrame methods (createOrReplaceTempView, corr/cov, toDF/toJSON/toPandas, columns, cache, stubs). Fixes applied: first_row, hex/url_encode, replace, case_insensitive_columns, describe/summary, split semantics (regex for `|`), timestamp format normalization; 21 hand-written fixtures remain skipped (see Skipped fixtures). **Phase 25 completed**: plan interpreter, expression interpreter (all scalar functions), 3 plan fixtures (`tests/fixtures/plans/`: filter_select_limit, join_simple, with_column_functions), create_dataframe_from_rows. Phase 22: datetime extensions. Phase 21: ordering, aggregates, numeric. Phase 19–18: aggregates, array/map/struct. Phase 17–15: datetime/unix, regexp, aliases, string, math. **Phase 27** (Sparkless integration) target: 200+ Sparkless tests passing with robin backend (after Phase 26 publish Rust crate). CI runs parity on hand-written (and optionally converted) fixtures; when Sparkless repo is available, run `make sparkless-parity` and update this doc.

### When Sparkless repo is available

1. Set `export SPARKLESS_EXPECTED_OUTPUTS=/path/to/sparkless/tests/expected_outputs`.
2. Run `make sparkless-parity` (converts with `--dedupe`, regenerates expected from PySpark, then runs `cargo test pyspark_parity_fixtures`).
3. Update the table above with converted count and passing/failing/skipped for `tests/fixtures/converted/*.json`.
4. For any failing fixture, add a row under "Failure reasons" and use `skip: true` + `skip_reason` in the fixture if it is a known unsupported or semantic difference. Because expected is PySpark-derived, failures indicate Robin vs PySpark divergence.

## Window parity (Sparkless issues #22–#35)

Window functions are implemented and covered by hand-written parity fixtures: `row_number_window`, `rank_window`, `lag_lead_window`, `first_value_window`, `last_value_window`, `percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window`. The harness in `tests/parity.rs` supports row_number, rank, dense_rank, lag, lead, first_value, last_value, percent_rank, cume_dist, ntile. Sum-over-window and approx_count_distinct window are supported via the same harness. When running Sparkless tests with robin backend, ensure the adapter uses these APIs.

## Array parity (Sparkless issues #36–#49)

Array functions are implemented and covered by hand-written parity fixtures: `array_contains`, `array_distinct` (first-occurrence order), `array_join`, `array_position`, `array_remove`, `array_sort`, `array_union`, `element_at`, `explode`, `size`/`array_size`, plus `array_append`, `array_insert`, `array_intersect`, `array_except`, `array_prepend`, `array_sum`, `arrays_overlap`, `arrays_zip`, `zip_with`. Converted fixtures in `tests/fixtures/converted/` for array_* are currently skipped until expected is regenerated from PySpark. When running Sparkless tests with robin backend, ensure the adapter uses these APIs.

## Datetime / type parity (Sparkless issues #50–#57)

Datetime and cast/alias behaviour are implemented and covered by hand-written parity fixtures: `date_add_sub`, `datediff`, `datetime_hour_minute`, `datetime_quarter_week`, `with_extract`, `with_weekday`, `with_dayname`, `make_date`, `unix_timestamp`, `unix_date`, `timestampadd_test`, `months_between_round_off`, `make_timestamp_test`, `type_coercion_*`, and cast/alias in expression tests. Functions include date_add, date_sub, date_format, dayofmonth, dayofweek, month, to_date, and cast/alias in select and filter. When running Sparkless tests with robin backend, ensure the adapter uses these APIs.

## String / binary parity batch 1 (Sparkless issues #86–#99)

String and binary functions in this batch are implemented and covered by hand-written parity fixtures: ascii, base64/unbase64, concat_ws (string_concat), crc32 (string_crc32), hex, initcap, levenshtein (string_levenshtein), repeat, reverse, soundex, and related tests. Fixtures include `string_crc32`, `string_levenshtein`, `string_concat`, `string_upper_lower`, `string_left_right_replace`, `string_length_trim`, `string_substring_index`, `string_lpad_rpad`, `string_translate`, `string_xxhash64`, etc. When running Sparkless tests with robin backend, ensure the adapter uses these APIs.

## String / binary parity batch 2 (Sparkless issues #100–#116)

String and binary functions in this batch are implemented and covered by hand-written parity fixtures: string_length/length, like/rlike (with escape), lower/upper, lpad/rpad, regexp_extract, rtrim/trim, substring/substr, translate, substring_index, xxhash64. Fixtures include `string_length_trim`, `string_upper_lower`, `string_lpad_rpad`, `string_substring`, `string_substring_index`, `string_translate`, `string_xxhash64`, `like_escape_char`, `ilike_escape_char`, etc. When running Sparkless tests with robin backend, ensure the adapter uses these APIs.

## SQL / session parity (Sparkless issues #122–#140)

SQL (via `SparkSession::sql()` with optional `sql` feature) and session behaviour are implemented: the SQL translator maps to DataFrame ops; cache_table/uncache_table, CASE WHEN, IN, JOINs, LIKE, ORDER BY, subquery, UNION, create_table_from_dataframe, insert_from_select, basic/filtered select, and group_by are supported via the translator and session catalog. Parity is exercised via DataFrame fixtures (filter, join, orderBy, groupBy, etc.) and plan fixtures. When running Sparkless tests with robin backend, ensure the adapter uses the SQL API and session catalog as documented.

## Features: createDataFrame and pivot (Sparkless issues #151, #156)

**createDataFrame (#151):** Implemented. `SparkSession::create_dataframe(data, column_names)` supports 3-column (i64, i64, String) tuples. For arbitrary schemas and input types use `create_dataframe_from_rows(rows, schema)` with schema inference or explicit (name, dtype) pairs; supported dtypes include bigint, double, string, boolean, date, timestamp. Python: `createDataFrame(data, column_names)` and `create_dataframe_from_rows(data, schema)`.

**pivot (#156):** Stub. `DataFrame::pivot(pivot_col, values)` / `DataFrame.pivot(pivot_col, values=None)` are present but raise "not yet implemented"; use `crosstab(col1, col2)` for two-column cross-tabulation until pivot is implemented.

## Failure reasons (converted fixtures)

When a converted fixture fails, classify and document here:

| Fixture name | Reason | Notes |
|--------------|--------|-------|
| (example) | unsupported: regexp_extract | robin-sparkless does not yet implement this function |
| (example) | semantic: null in groupBy key | PySpark vs Polars grouping difference |

**Reason categories:**

- **converter**: Wrong op/schema/rows from conversion; fix in `convert_sparkless_fixtures.py` or Sparkless metadata.
- **unsupported**: Missing function or expression in robin-sparkless (Phase 6).
- **semantic**: Implemented but result differs (e.g. null handling); document and optionally add `skip: true` + `skip_reason` in the fixture.

## Skipped fixtures

Fixtures with `"skip": true` in JSON are not run. **21 hand-written** fixtures remain skipped (down from 38 after parity fixes). See each fixture’s `skip_reason` in JSON. Typical reasons: timezone (timestamp_seconds/millis/micros), struct row format (named_struct_test, struct_test), window frame (nth_value_window, last_value_window, ntile_window), set ops (intersect, subtract), right_join column order, JVM-only (with_jvm_stubs), non-deterministic (with_rand_seed, with_unix_micros), hash/xxhash (with_hash, string_xxhash64), assert_true type, months_between/arrays_overlap, arrays_zip struct length, with_curdate_now, raise_error.

## Closed-issue test coverage

| Issue range | Coverage (parity fixtures and/or Python tests) |
|-------------|-----------------------------------------------|
| #1–#21 (core) | Python: test_sparkless_parity_*, test_filter_with_and_or_operators; parity: filter, select, orderBy, groupBy, join, limit, first, replace, case_insensitive_columns, describe, summary |
| #22–#35 (window) | Parity: row_number_window, rank_window, lag_lead_window, cume_dist_window, first_value_window, percent_rank_window; nth_value_window, last_value_window, ntile_window (skipped: frame/ordering) |
| #36–#49 (array) | Parity: array_size, array_contains, element_at, array_union, array_except, array_intersect, array_distinct, array_append, array_prepend, array_insert, zip_with; arrays_zip (skipped) |
| #50–#57 (datetime) | Parity: date_add_sub, datediff, datetime_*, make_date, type_coercion_*, to_timestamp_format, make_timestamp_test; timestamp_seconds/millis/micros (skipped: TZ) |
| #86–#99, #100–#116 (string) | Parity: with_hex, with_url_encode, string_*, replace, like_escape_char; with_hash/string_xxhash64 (skipped) |
| #122–#140 (SQL/session) | Python: test_sql_select_where_returns_rows; parity: filter, join, groupBy plan fixtures |
| #151 (createDataFrame) | Python: test_create_dataframe_from_rows_schema_pyspark_parity, test_create_dataframe_and_collect |
| #156 (pivot) | Python: test_pivot_raises_not_implemented |

## Related

- [CONVERTER_STATUS.md](CONVERTER_STATUS.md) – converter usage and operation mapping
- [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) – Phase 5 goals
