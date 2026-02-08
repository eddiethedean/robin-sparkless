# Gap Analysis: Robin-Sparkless vs PySpark (from source)

This document compares robin-sparkless with **Apache PySpark** using API surface extracted directly from the [PySpark source repository](https://github.com/apache/spark/tree/master/python/pyspark).

## Method

- **PySpark API**: Extracted from Apache Spark repo via `scripts/extract_pyspark_api_from_repo.py` (AST parsing of `pyspark.sql` sources).
- **PySpark version/branch**: 18 (branch/tag: v3.5.0)
- **Robin-sparkless API**: From signatures_robin_sparkless.json (introspection)
- **Scope**: `pyspark.sql` (functions, DataFrame, Column, GroupedData, SparkSession, Reader, Writer, Window).

## Summary

### Functions (pyspark.sql.functions)

| Classification | Count | Description |
|----------------|-------|-------------|
| exact | 214 | Same parameter names, order, and defaults |
| compatible | 0 | Same params/defaults; types may differ |
| partial | 30 | Different param names or counts |
| missing | 171 | In PySpark but not in robin-sparkless |
| extra | 13 | In robin-sparkless only (extensions) |

- **PySpark functions:** 415
- **Robin-sparkless functions:** 257

### Class methods

| Class | Exact | Partial | Missing | Extra |
|-------|-------|---------|---------|-------|
| SparkSession | 1 | 2 | 25 | 9 |
| DataFrame | 16 | 31 | 52 | 13 |
| Column | 0 | 6 | 9 | 131 |
| GroupedData | 2 | 4 | 2 | 24 |
| DataFrameReader | 0 | 0 | 12 | 0 |
| DataFrameWriter | 0 | 0 | 16 | 0 |
| Window | 0 | 0 | 4 | 0 |
| Catalog | 0 | 0 | 27 | 0 |

---

## Function details (sample)

### Exact match

- `abs(col)`
- `acos(col)`
- `acosh(col)`
- `add_months(start, months)`
- `array(cols)`
- `array_agg(col)`
- `array_append(col, value)`
- `array_compact(col)`
- `array_contains(col, value)`
- `array_distinct(col)`
- `array_except(col1, col2)`
- `array_insert(arr, pos, value)`
- `array_intersect(col1, col2)`
- `array_max(col)`
- `array_min(col)`
- `array_position(col, value)`
- `array_prepend(col, value)`
- `array_size(col)`
- `array_union(col1, col2)`
- `arrays_overlap(a1, a2)`
- `asc(col)`
- `asc_nulls_first(col)`
- `asc_nulls_last(col)`
- `ascii(col)`
- `asin(col)`
- `asinh(col)`
- `assert_true(col, errMsg='None')`
- `atan(col)`
- `atan2(col1, col2)`
- `atanh(col)`
- ... and 184 more

### Partial (param mismatch)

| PySpark | Robin |
|---------|-------|
| `aggregate(col, initialValue, merge, finish='None')` | `aggregate(col, zero)` |
| `array_join(col, delimiter, null_replacement='None')` | `array_join(col, delimiter)` |
| `array_sort(col, comparator='None')` | `array_sort(col)` |
| `arrays_zip(cols)` | `arrays_zip(col1, col2)` |
| `bit_and(col)` | `bit_and(col1, col2)` |
| `bit_or(col)` | `bit_or(col1, col2)` |
| `bit_xor(col)` | `bit_xor(col1, col2)` |
| `char_length(str)` | `char_length(col)` |
| `character_length(str)` | `character_length(col)` |
| `date_add(start, days)` | `date_add(col, days)` |
| `date_format(date, format)` | `date_format(col, format)` |
| `date_sub(start, days)` | `date_sub(col, days)` |
| `date_trunc(format, timestamp)` | `date_trunc(format, col)` |
| `elt(inputs)` | `elt(index, cols)` |
| `from_csv(col, schema, options='None')` | `from_csv(col)` |
| `from_unixtime(timestamp, format="'yyyy-MM-dd HH:mm:ss'")` | `from_unixtime(timestamp, format)` |
| `json_array_length(col)` | `json_array_length(col, path)` |
| `log(col)` | `log(col, base)` |
| `map_concat(cols)` | `map_concat(col1, col2)` |
| `named_struct(cols)` | `named_struct(names, columns)` |
| `overlay(src, replace, pos, len='-1')` | `overlay(src, replace, pos, len='Ellipsis')` |
| `regexp_extract_all(str, regexp, idx='None')` | `regexp_extract_all(str, regexp, idx=0)` |
| `schema_of_csv(csv, options='None')` | `schema_of_csv(col)` |
| `schema_of_json(json, options='None')` | `schema_of_json(col)` |
| `split(str, pattern, limit='-1')` | `split(src, delimiter)` |
| *... and 5 more* | |

### Missing (PySpark only)

- `aes_decrypt(input, key, mode='None', padding='None', aad='None')`
- `aes_encrypt(input, key, mode='None', padding='None', iv='None', aad='None')`
- `any_value(col, ignoreNulls='None')`
- `approx_count_distinct(col, rsd='None')`
- `approx_percentile(col, percentage, accuracy='10000')`
- `array_remove(col, element)`
- `array_repeat(col, count)`
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
- `datediff(end, start)`
- `decode(col, charset)`
- `dense_rank()`
- `element_at(col, extraction)`
- `encode(col, charset)`
- `every(col)`
- `exists(col, f)`
- `exp(col)`
- `explode(col)`
- `expr(str)`
- `filter(col, f)`
- `first(col, ignorenulls='False')`
- `first_value(col, ignoreNulls='None')`
- `flatten(col)`
- `floor(col)`
- `forall(col, f)`
- `from_json(col, schema, options='None')`
- `grouping(col)`
- `grouping_id(cols)`
- `histogram_numeric(col, nBins)`
- `hll_sketch_agg(col, lgConfigK='None')`
- ... and 121 more

### Extra (robin-sparkless only)

- `bitwiseNOT(col)`
- `cast(col, type_name)`
- `chr(col)`
- `dayname(col)`
- `isin(col, other)`
- `minutes(n)`
- `negate(col)`
- `power(col1, col2)`
- `shiftLeft(col, numBits)`
- `shiftRight(col, numBits)`
- `timestampadd(unit, amount, ts)`
- `timestampdiff(unit, start, end)`
- `try_cast(col, type_name)`

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
