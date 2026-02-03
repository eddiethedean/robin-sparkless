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

## Phase 10 & Phase 8 – Implemented

**All previously stubbed Phase 8 items are now implemented (February 2026):**

- **String 6.4**: `mask`, `translate`, `substring_index`; **`soundex`, `levenshtein`, `crc32`, `xxhash64`** (via Expr::map / map_many UDFs with strsim, crc32fast, twox-hash, soundex crates).
- **Array extensions**: `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`; **`array_flatten`, `array_repeat`** (via Expr::map UDFs).
- **Map (6b)**: **`create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays`** (Map as List(Struct{key, value}); create_map via as_struct/concat_list; map_keys/map_values via list.eval + struct.field; map_from_arrays via UDF).
- **JSON**: `get_json_object`, `from_json`, `to_json` (Polars extract_jsonpath / dtype-struct).
- **Window fixtures**: percent_rank, cume_dist, ntile, nth_value covered via multi-step workaround.
- **Phase 16 string/regex**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf` — all implemented.
- **Phase 17 datetime/unix**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial` — all implemented. Note: `unix_timestamp` and `from_unixtime` use chrono; results may differ from PySpark when session timezone differs from system timezone.
- **Phase 18 array/map/struct**: `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union`, `map_concat`, `map_from_entries`, `map_contains_key`, `get`, `struct`, `named_struct`, **`map_filter`**, **`map_zip_with`**, **`zip_with`** — all implemented. Uses Expr-based predicates/merge (e.g. `col("").struct_().field_by_name("value") > lit(30)`, `coalesce(left, right)`). Python: `map_filter_value_gt`, `zip_with_coalesce`, `map_zip_with_coalesce` convenience helpers.

See [ROADMAP.md](ROADMAP.md) and [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) for the full list.
