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

## Pass/fail summary

| Source | Converted | Passing | Failing | Skipped |
|--------|-----------|---------|--------|---------|
| Hand-written (`tests/fixtures/*.json`) | — | 160 | 0 | 1 (with_curdate_now) |
| Sparkless converted (`tests/fixtures/converted/*.json`) | 226 | 0 | 0 | 226 (all skipped: expected shape from converter; run `regenerate_expected_from_pyspark.py` with PySpark to fix) |

**Target: 50+ tests passing** (hand-written + converted). **Current: 160 passing** (hand-written; with_curdate_now skipped). **Phase 25 completed**: plan interpreter, expression interpreter (all scalar functions), 3 plan fixtures (`tests/fixtures/plans/`: filter_select_limit, join_simple, with_column_functions), create_dataframe_from_rows. Phase 22: datetime extensions. Phase 21: ordering, aggregates, numeric. Phase 19–18: aggregates, array/map/struct. Phase 17–15: datetime/unix, regexp, aliases, string, math. **Phase 27** (Sparkless integration) target: 200+ Sparkless tests passing with robin backend (after Phase 26 publish Rust crate). CI runs parity on hand-written (and optionally converted) fixtures; when Sparkless repo is available, run `make sparkless-parity` and update this doc.

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

Fixtures with `"skip": true` in JSON are not run. List them here for visibility:

| Fixture name | skip_reason |
|--------------|-------------|
| array_distinct | Polars list().unique() returns different order than PySpark array_distinct (first-occurrence) |

## Related

- [CONVERTER_STATUS.md](CONVERTER_STATUS.md) – converter usage and operation mapping
- [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) – Phase 5 goals
