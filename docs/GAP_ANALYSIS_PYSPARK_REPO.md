# Gap Analysis: Robin-Sparkless vs PySpark (from source)

This document compares robin-sparkless with **Apache PySpark** using API surface extracted directly from the [PySpark source repository](https://github.com/apache/spark/tree/master/python/pyspark).

## Method

- **PySpark API**: Extracted from Apache Spark repo via `scripts/extract_pyspark_api_from_repo.py` (AST parsing of `pyspark.sql` sources).
- **PySpark version/branch**: 18 (branch/tag: v3.5.0)
- **Robin-sparkless API**: From robin_api_from_source.json (source extraction)
- **Scope**: `pyspark.sql` (functions, DataFrame, Column, GroupedData, SparkSession, Reader, Writer, Window).

## Summary

### Functions (pyspark.sql.functions)

| Classification | Count | Description |
|----------------|-------|-------------|
| exact | 15 | Same parameter names, order, and defaults |
| compatible | 0 | Same params/defaults; types may differ |
| partial | 205 | Different param names or counts |
| missing | 195 | In PySpark but not in robin-sparkless |
| extra | 20 | In robin-sparkless only (extensions) |

- **PySpark functions:** 415
- **Robin-sparkless functions:** 240

### Class methods

| Class | Exact | Partial | Missing | Extra |
|-------|-------|---------|---------|-------|
| SparkSession | 1 | 2 | 25 | 11 |
| DataFrame | 10 | 37 | 52 | 13 |
| Column | 0 | 6 | 9 | 134 |
| GroupedData | 1 | 5 | 2 | 24 |
| DataFrameReader | 0 | 0 | 12 | 0 |
| DataFrameWriter | 0 | 0 | 16 | 0 |
| Window | 0 | 0 | 4 | 0 |
| Catalog | 0 | 0 | 27 | 0 |

---

## Function details (sample)

### Exact match

- `curdate()`
- `current_catalog()`
- `current_database()`
- `current_schema()`
- `current_timezone()`
- `current_user()`
- `e()`
- `input_file_name()`
- `localtimestamp()`
- `monotonically_increasing_id()`
- `now()`
- `pi()`
- `spark_partition_id()`
- `user()`
- `version()`

### Partial (param mismatch)

| PySpark | Robin |
|---------|-------|
| `acos(col)` | `acos()` |
| `acosh(col)` | `acosh()` |
| `add_months(start, months)` | `add_months()` |
| `array_agg(col)` | `array_agg()` |
| `array_append(col, value)` | `array_append()` |
| `array_compact(col)` | `array_compact()` |
| `array_distinct(col)` | `array_distinct()` |
| `array_except(col1, col2)` | `array_except()` |
| `array_insert(arr, pos, value)` | `array_insert()` |
| `array_intersect(col1, col2)` | `array_intersect()` |
| `array_prepend(col, value)` | `array_prepend()` |
| `array_union(col1, col2)` | `array_union()` |
| `arrays_overlap(a1, a2)` | `arrays_overlap()` |
| `arrays_zip(cols)` | `arrays_zip()` |
| `asc(col)` | `asc()` |
| `asc_nulls_first(col)` | `asc_nulls_first()` |
| `asc_nulls_last(col)` | `asc_nulls_last()` |
| `ascii(col)` | `ascii()` |
| `asin(col)` | `asin()` |
| `asinh(col)` | `asinh()` |
| `assert_true(col, errMsg='None')` | `assert_true()` |
| `atan(col)` | `atan()` |
| `atan2(col1, col2)` | `atan2()` |
| `atanh(col)` | `atanh()` |
| `avg(col)` | `avg()` |
| *... and 180 more* | |

### Missing (PySpark only)

- `abs(col)`
- `aes_decrypt(input, key, mode='None', padding='None', aad='None')`
- `aes_encrypt(input, key, mode='None', padding='None', iv='None', aad='None')`
- `aggregate(col, initialValue, merge, finish='None')`
- `any_value(col, ignoreNulls='None')`
- `approx_count_distinct(col, rsd='None')`
- `approx_percentile(col, percentage, accuracy='10000')`
- `array(cols)`
- `array_contains(col, value)`
- `array_join(col, delimiter, null_replacement='None')`
- `array_max(col)`
- `array_min(col)`
- `array_position(col, value)`
- `array_remove(col, element)`
- `array_repeat(col, count)`
- `array_size(col)`
- `array_sort(col, comparator='None')`
- `bitmap_bit_position(col)`
- `bitmap_bucket_number(col)`
- `bitmap_construct_agg(col)`
- `bitmap_count(col)`
- `bitmap_or_agg(col)`
- `bool_and(col)`
- `bool_or(col)`
- `bucket(numBuckets, col)`
- `call_function(funcName, cols)`
- `call_udf(udfName, cols)`
- `cardinality(col)`
- `ceil(col)`
- `char_length(str)`
- `character_length(str)`
- `collect_list(col)`
- `collect_set(col)`
- `concat(cols)`
- `concat_ws(sep, cols)`
- `corr(col1, col2)`
- `count_distinct(col, cols)`
- `count_if(col)`
- `count_min_sketch(col, eps, confidence, seed)`
- `covar_pop(col1, col2)`
- `covar_samp(col1, col2)`
- `crc32(col)`
- `cume_dist()`
- `current_date()`
- `current_timestamp()`
- `date_add(start, days)`
- `date_format(date, format)`
- `date_sub(start, days)`
- `date_trunc(format, timestamp)`
- `datediff(end, start)`
- ... and 145 more

### Extra (robin-sparkless only)

- `bitwiseNOT()`
- `cast()`
- `chr()`
- `configure_for_multiprocessing()`
- `dayname()`
- `execute_plan()`
- `isin()`
- `isin_i64()`
- `isin_str()`
- `map_filter_value_gt()`
- `map_zip_with_coalesce()`
- `minutes()`
- `negate()`
- `power()`
- `shiftLeft()`
- `shiftRight()`
- `timestampadd()`
- `timestampdiff()`
- `try_cast()`
- `zip_with_coalesce()`

---

## Semantic annotations

Items tagged from [docs/gap_annotations.json](gap_annotations.json) and [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md):

**stub** (no-op or placeholder):

- `broadcast`
- `current_catalog`
- `current_database`
- `current_schema`
- `current_user`
- `grouping`
- `grouping_id`
- `input_file_name`
- `monotonically_increasing_id`
- `spark_partition_id`
- `user`
- `isStreaming`
- `is_streaming`
- `persist`
- `storageLevel`
- `storage_level`
- `unpersist`
- `withWatermark`
- `with_watermark`
- `current_catalog`
- `current_database`

**diverges** (behavior differs from PySpark):

- `aes_decrypt`
- `aes_encrypt`
- `assert_true`
- `from_unixtime`
- `from_utc_timestamp`
- `raise_error`
- `rand`
- `randn`
- `to_utc_timestamp`
- `try_aes_decrypt`
- `unix_timestamp`
- `from_unixtime`
- `unix_timestamp`

**deferred** (out of scope):

- `call_udf`
- `count_min_sketch`
- `histogram_numeric`
- `hll_sketch_agg`
- `hll_sketch_estimate`
- `hll_union`
- `hll_union_agg`
- `sentences`
- `session_window`
- `udf`
- `udtf`
- `xpath`
- `xpath_boolean`
- `xpath_double`
- `xpath_float`
- `xpath_int`
- `xpath_long`
- `xpath_number`
- `xpath_short`
- `xpath_string`
- `udf`
- `udtf`
- `foreach`
- `foreachPartition`
- `foreach_partition`

Parity fixture coverage: see [PARITY_STATUS.md](PARITY_STATUS.md).

---

## Regeneration

```bash
python scripts/extract_pyspark_api_from_repo.py --clone --branch v3.5.0
python scripts/extract_robin_api_from_source.py  # or use existing signatures_robin_sparkless.json
python scripts/gap_analysis_pyspark_repo.py --write-md docs/GAP_ANALYSIS_PYSPARK_REPO.md
```
