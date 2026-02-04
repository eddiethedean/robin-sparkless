# PySpark vs Robin-Sparkless: Signature Gap Analysis

This document compares **signatures** (parameters, types, defaults) of the public Python API of robin-sparkless with **PySpark** to guide alignment.

## Method

- **PySpark signatures**: Obtained by introspecting an installed PySpark (`inspect.signature`, optional type hints) for `pyspark.sql.functions`, `SparkSession`, `SparkSession.builder`, `DataFrame`, `GroupedData`, `Column`, `DataFrameStat`, and `DataFrameNa`.
- **Robin-sparkless signatures**: Same approach on the built `robin_sparkless` module (requires `maturin develop --features pyo3`).
- **PySpark version used**: 3.5.0
- **Cross-check**: PySpark [official API docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) can be used to fill or correct parameter names and defaults where introspection is incomplete.

## Summary

### Functions (pyspark.sql.functions)

| Classification | Count | Description |
|----------------|-------|-------------|
| exact | 27 | Same parameter names, order, and defaults |
| compatible | 0 | Same params/defaults; types may differ |
| partial | 179 | Different param names or counts (e.g. `column` vs `col`) |
| missing | 216 | In PySpark but not in robin-sparkless |
| extra | 17 | In robin-sparkless but not in PySpark (extensions) |

- **PySpark function names (excluding typing re-exports):** 422
- **Robin-sparkless function names:** 223

### Class methods

| Class | Exact | Partial | Missing | Extra |
|-------|-------|---------|---------|-------|
| SparkSession | 0 | 0 | 19 | 7 |
| SparkSessionBuilder | 1 | 1 | 5 | 2 |
| DataFrame | 0 | 0 | 0 | 43 |
| GroupedData | 0 | 0 | 0 | 21 |
| Column | 0 | 9 | 23 | 119 |
| DataFrameStat | 0 | 0 | 0 | 3 |
| DataFrameNa | 0 | 0 | 0 | 2 |

---

## Function details (sample)

### Exact match (same signature)

- `broadcast(df)`
- `curdate()`
- `current_catalog()`
- `current_database()`
- `current_schema()`
- `current_timezone()`
- `current_user()`
- `date_diff(end, start)`
- `e()`
- `input_file_name()`
- `localtimestamp()`
- `make_date(year, month, day)`
- `make_interval(years, months, weeks, days, hours, mins, secs)`
- `monotonically_increasing_id()`
- `now()`
- `nvl2(col1, col2, col3)`
- `pi()`
- `pmod(dividend, divisor)`
- `rand(seed)`
- `randn(seed)`
- `spark_partition_id()`
- `try_add(left, right)`
- `try_divide(left, right)`
- `try_multiply(left, right)`
- `try_subtract(left, right)`
- `user()`
- `version()`

### Partial (param name or count difference)

Aligning parameter names to PySpark improves drop-in compatibility. Examples:

| PySpark | Robin |
|---------|-------|
| `acos(col)` | `acos(column)` |
| `acosh(col)` | `acosh(column)` |
| `add_months(start, months)` | `add_months(column, n)` |
| `array_agg(col)` | `array_agg(column)` |
| `array_append(col, value)` | `array_append(array, elem)` |
| `array_compact(col)` | `array_compact(column)` |
| `array_distinct(col)` | `array_distinct(column)` |
| `array_except(col1, col2)` | `array_except(a, b)` |
| `array_insert(arr, pos, value)` | `array_insert(array, pos, elem)` |
| `array_intersect(col1, col2)` | `array_intersect(a, b)` |
| `array_prepend(col, value)` | `array_prepend(array, elem)` |
| `array_union(col1, col2)` | `array_union(a, b)` |
| `arrays_overlap(a1, a2)` | `arrays_overlap(left, right)` |
| `arrays_zip(cols)` | `arrays_zip(left, right)` |
| `asc(col)` | `asc(column)` |
| `asc_nulls_first(col)` | `asc_nulls_first(column)` |
| `asc_nulls_last(col)` | `asc_nulls_last(column)` |
| `ascii(col)` | `ascii(column)` |
| `asin(col)` | `asin(column)` |
| `asinh(col)` | `asinh(column)` |
| `assert_true(col, errMsg)` | `assert_true(column)` |
| `atan(col)` | `atan(column)` |
| `atan2(col1, col2)` | `atan2(y, x)` |
| `atanh(col)` | `atanh(column)` |
| `avg(col)` | `avg(column)` |
| *... and 154 more (see signature_comparison.json)* | |

### Missing (in PySpark, not in robin-sparkless)

Functions present in PySpark but not implemented in robin-sparkless (first 40):

- `abs(col)`
- `aes_decrypt(input, key, mode, padding, aad)`
- `aes_encrypt(input, key, mode, padding, iv, aad)`
- `aggregate(col, initialValue, merge, finish)`
- `any_value(col, ignoreNulls)`
- `approx_count_distinct(col, rsd)`
- `approx_percentile(col, percentage, accuracy=10000)`
- `array(cols)`
- `array_contains(col, value)`
- `array_join(col, delimiter, null_replacement)`
- `array_max(col)`
- `array_min(col)`
- `array_position(col, value)`
- `array_remove(col, element)`
- `array_repeat(col, count)`
- `array_size(col)`
- `array_sort(col, comparator)`
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
- `column(col)`
- `concat(cols)`
- `concat_ws(sep, cols)`
- `corr(col1, col2)`
- `count_distinct(col, cols)`
- `count_if(col)`
- `count_min_sketch(col, eps, confidence, seed)`
- ... and 176 more (see `signature_comparison.json`)

### Extra (in robin-sparkless only)

Robin-sparkless extensions (e.g. for Sparkless backend):

- `bitwiseNOT(column)`
- `cast(column, type_name)`
- `chr(column)`
- `dayname(column)`
- `execute_plan(data, schema, plan_json)`
- `isin(column, other)`
- `isin_i64(column, values)`
- `isin_str(column, values)`
- `map_filter_value_gt(map_col, threshold)`
- `map_zip_with_coalesce(map1, map2)`
- `minutes(n)`
- `shiftLeft(column, n)`
- `shiftRight(column, n)`
- `timestampadd(unit, amount, ts)`
- `timestampdiff(unit, start, end)`
- `try_cast(column, type_name)`
- `zip_with_coalesce(left, right)`

---

## Recommendations

1. **Parameter names**: Where classification is **partial**, consider aliasing or renaming parameters to match PySpark (e.g. `column` → `col`, `n` → `months` for `add_months`) so that call sites using keyword arguments match. See **[SIGNATURE_ALIGNMENT_TASKS.md](SIGNATURE_ALIGNMENT_TASKS.md)** for a concrete checklist (simple renames, add-optional, and other renames).
2. **Defaults**: Add any missing optional parameters with PySpark defaults (e.g. `format=None`) so that existing PySpark code passes unchanged.
3. **Missing functions**: Prioritize implementing high-use PySpark functions that are **missing**; the full list is in `signature_comparison.json`.
4. **Extra APIs**: Keep robin-only APIs (e.g. `execute_plan`, `create_dataframe_from_rows`) for backend use; document them as extensions.
5. **Class methods**: Align DataFrame, SparkSession, GroupedData, and Column method signatures (parameter names and order) to PySpark where practical.

---

*Generated from `scripts/compare_signatures.py`. Regenerate with:*

```bash
python scripts/export_pyspark_signatures.py --output docs/signatures_pyspark.json
. .venv/bin/activate && python scripts/export_robin_signatures.py --output docs/signatures_robin_sparkless.json
python scripts/compare_signatures.py --output docs/signature_comparison.json --write-md docs/SIGNATURE_GAP_ANALYSIS.md
```
