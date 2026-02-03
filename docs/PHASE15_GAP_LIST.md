# Phase 15: Function Gap List (PYSPARK_FUNCTION_MATRIX vs Robin-Sparkless)

Source: [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md). Sparkless implements 403 functions; robin-sparkless targets parity.

**Direct comparison with Sparkless 3.28.0:** A full gap analysis against the **installed** Sparkless 3.28.0 API is in [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md). To reproduce: `python3 -m venv .venv-sparkless && .venv-sparkless/bin/pip install sparkless==3.28.0`.

## Already implemented (verified in src/functions.rs, src/column.rs, src/udfs.rs)

Core: col, lit_*, count, sum, avg, min, max, stddev, variance, count_distinct, when, coalesce, nvl, ifnull, nullif, nanvl.
String: upper, lower, substring, length, trim, ltrim, rtrim, regexp_extract, regexp_replace, split, initcap, regexp_extract_all, regexp_like, repeat, reverse, instr, position, ascii, format_number, overlay, char, chr, base64, unbase64, sha1, sha2, md5, lpad, rpad, translate, mask, substring_index, soundex, levenshtein, crc32, xxhash64, regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf.
Math: abs, ceil, floor, round, sqrt, pow, exp, log, sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum.
Type: cast, try_cast, isnan, greatest, least.
Datetime: year, month, day, to_date, date_format, current_date, current_timestamp, hour, minute, second, date_add, date_sub, datediff, last_day, trunc, quarter, weekofyear, dayofweek, dayofyear, add_months, months_between, next_day.
Array: array, array_size, size, array_contains, array_join, array_max, array_min, element_at, array_sort, array_slice, explode, array_position, array_compact, array_remove, array_repeat, array_flatten, array_exists, array_forall, array_filter, array_transform, array_sum, array_mean, posexplode.
Map: create_map, map_keys, map_values, map_entries, map_from_arrays.
JSON: get_json_object, from_json, to_json.
Window: row_number, rank, dense_rank, lag, lead, first_value, last_value, percent_rank, cume_dist, ntile, nth_value.
Column: is_null, is_not_null (methods).

## Batch 1 – Aliases and simple (Phase 15)

- substr → alias substring
- power → alias pow
- ln → alias log
- ceiling → alias ceil
- lcase → alias lower
- ucase → alias upper
- dayofmonth → alias day
- toDegrees / to_degrees → alias degrees
- toRadians / to_radians → alias radians
- isnull(column) → column.is_null()
- isnotnull(column) → column.is_not_null()
- nvl2(col1, col2, col3) → when(col1.is_not_null()).then(col2).otherwise(col3)

## Batch 2 – String and regex

- left, right, replace, startswith, endswith, contains, like (implement properly), ilike, rlike, regexp (alias rlike) — **DONE**
- regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf — **DONE (Phase 16)**

## Batch 3 – Math and datetime

- acosh, asinh, atanh, cosh, sinh, tanh, cbrt, expm1, log1p, log2, log10, rint, hypot — **DONE (Phase 15)**
- pmod, factorial — **DONE (Phase 17)**
- unix_timestamp, to_unix_timestamp, from_unixtime, make_date, timestamp_seconds/millis/micros, unix_date, date_from_unix_date — **DONE (Phase 17)**

## Batch 4 – Array/map/struct

- array_append, array_prepend, array_insert, array_distinct, array_except, array_intersect, array_union
- zip_with, map_concat, map_filter, map_zip_with, transform_keys, transform_values
- named_struct

## Batch 5 – Aggregates and misc

- any_value, bool_and, bool_or, every, some, count_if, max_by, min_by, percentile, product
- try_add, try_divide, try_subtract, try_multiply, try_sum, try_avg, try_element_at
- width_bucket, elt, bit_length, typeof

## Defer / stub

ML (histogram_numeric, HLL, regr_*), JVM (broadcast, spark_partition_id, input_file_name, monotonically_increasing_id, session_window, current_user), crypto (aes_*), call_udf, reduce, stack, xpath_*, java_method, getbit/bit_*, to_binary, udtf.

**Full gap list vs Sparkless 3.28.0:** See [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md) for the complete list of functions and DataFrame methods present in Sparkless 3.28.0 but not yet in robin-sparkless.
