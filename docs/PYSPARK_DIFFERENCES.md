# PySpark vs Robin-Sparkless: Known Differences

This document lists **intentional or known divergences** from PySpark semantics in robin-sparkless. Robin-sparkless aims for behavioral parity where practical; when perfect parity is impossible or deferred, we document it here.

## Window functions

- **percent_rank, cume_dist, ntile, nth_value**: The **API** is implemented (Rust and Python). Parity fixtures for these (`percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window`) are **skipped** because Polars does not allow combining certain window expressions (e.g. `rank().over()` and `count().over()`) in a single expression in the way PySpark does. A multi-step workaround (e.g. computing in separate columns then combining) is possible but not yet used in the parity harness. See [PARITY_STATUS.md](PARITY_STATUS.md) and fixture `skip_reason` fields.

## GroupBy

- **Null keys and empty groups**: groupBy + aggregates are tested with fixtures `groupby_null_keys`, `groupby_single_group`, and `groupby_single_row_groups`. Behavior is aligned with PySpark for these cases (nulls in grouping keys produce one group per null; single-group and single-row groups behave as in PySpark). Any future divergence discovered will be listed here.

## SQL (optional `sql` feature)

- **Unsupported constructs**: No subqueries in `FROM`, no CTEs, no DDL, no `HAVING`. Unsupported constructs should produce clear errors. Supported: single `SELECT`, `FROM` (single table or JOIN), `WHERE`, `GROUP BY` + aggregates, `ORDER BY`, `LIMIT`, and temporary views (`createOrReplaceTempView`, `table()`).

## Delta Lake (optional `delta` feature)

- **Deferred**: Schema evolution and MERGE are not implemented. Read by path/version, overwrite, and append are supported. See [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) §7.2.

## Phase 8 (later) items

The following are **deferred** to Phase 8 and are not yet implemented:

- **array_repeat**: Blocked on Polars support for list.eval with dynamic repeat/flatten (or equivalent).
- **Map (6b)**: `map_keys`, `map_values`, `map_entries`, `map_from_arrays`, etc. Polars represents maps as `List(Struct)`; mapping PySpark MapType semantics is non-trivial.
- **JSON (6c)**: `get_json_object`, `from_json`, `to_json`; Polars JSON support is behind optional features.
- **String 6.4**: `soundex`, `levenshtein`, `translate`, `crc32`, `xxhash64`, `mask`, `substring_index`.
- **Window fixture simplification**: Enabling percent_rank/cume_dist/ntile/nth_value parity fixtures when Polars supports the required combined window/aggregation expressions, or documenting the multi-step workaround.

See [ROADMAP.md](ROADMAP.md) and [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) Phase 8 for the full list.
