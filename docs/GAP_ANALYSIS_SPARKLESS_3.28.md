# Gap Analysis: Robin-Sparkless vs Sparkless 3.28.0

**Date:** February 2026  
**Sparkless version:** 3.28.0 (installed via `pip install sparkless==3.28.0`)  
**Method:** Direct comparison of `sparkless.sql.functions` API vs robin-sparkless `src/functions.rs` + `src/column.rs` + `src/udfs.rs`.

## Summary

| Metric | Sparkless 3.28.0 | Robin-Sparkless |
|--------|-------------------|-----------------|
| **Functions (top-level / F.xxx)** | ~280+ distinct names | ~283 implemented |
| **Implemented in robin-sparkless** | — | ~283 PySpark-equivalent functions |
| **Gap (in Sparkless, not in robin-sparkless)** | — | ~10+ function names (see below) |

**Update (Phase 18–23):** Phase 18–22 — all implemented. **Phase 23:** isin, isin_i64, isin_str, url_decode, url_encode, json_array_length, parse_url, hash, shift_left, shift_right, shift_right_unsigned, version, equal_null, stack — all implemented.

Sparkless exposes both camelCase and snake_case for some (e.g. `countDistinct` / `count_distinct`). Robin-sparkless implements the snake_case PySpark-style names; where we have an equivalent, it’s counted as implemented.

## 1. Robin-Sparkless API Surface (Implemented)

The following are implemented in robin-sparkless (Rust `functions.rs`, `column.rs`, `udfs.rs`; Python via PyO3):

**Core / literals:** `col`, `lit` (as lit_i32, lit_i64, lit_f64, lit_bool, lit_str), `when` (+ WhenBuilder), `coalesce`, `nvl`, `ifnull`, `nullif`, `nanvl`, `nvl2`

**Aggregates:** `count`, `sum`, `avg`, `min`, `max`, `stddev`, `variance`, `count_distinct`

**String:** `upper`, `lower`, `substring`, `substr`, `length`, `trim`, `ltrim`, `rtrim`, `btrim`, `locate`, `conv`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`, `initcap`, `repeat`, `reverse`, `instr`, `position`, `ascii`, `format_number`, `overlay`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`, `lpad`, `rpad`, `translate`, `mask`, `substring_index`, `left`, `right`, `replace`, `startswith`, `endswith`, `contains`, `like`, `ilike`, `rlike`, `soundex`, `levenshtein`, `crc32`, `xxhash64`

**Math:** `abs`, `ceil`, `ceiling`, `floor`, `round`, `sqrt`, `pow`, `power`, `exp`, `log`, `ln`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `degrees`, `radians`, `to_degrees`, `to_radians`, `signum`, `cosh`, `sinh`, `tanh`, `acosh`, `asinh`, `atanh`, `cbrt`, `expm1`, `log1p`, `log10`, `log2`, `rint`, `hypot`

**Type / conditional:** `cast`, `try_cast`, `isnan`, `greatest`, `least`, `to_char`, `to_varchar`, `to_number`, `try_to_number`, `try_to_timestamp`

**Datetime:** `year`, `month`, `day`, `dayofmonth`, `quarter`, `weekofyear`, `dayofweek`, `dayofyear`, `to_date`, `date_format`, `current_date`, `current_timestamp`, `hour`, `minute`, `second`, `date_add`, `date_sub`, `datediff`, `last_day`, `trunc`, `add_months`, `months_between`, `next_day`, `curdate`, `now`, `localtimestamp`, `date_diff`, `dateadd`, `datepart`, `extract`, `date_part`, `dayname`, `weekday`, `unix_micros`, `unix_millis`, `unix_seconds`, `make_timestamp`, `make_timestamp_ntz`, `make_interval`, `timestampadd`, `timestampdiff`, `days`, `hours`, `minutes`, `months`, `years`, `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone`, `current_timezone`, `to_timestamp` (Phase 22)

**Concat:** `concat`, `concat_ws`

**Window:** `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`, `percent_rank`, `cume_dist`, `ntile`, `nth_value`

**Null checks:** `isnull`, `isnotnull` (and Column `is_null`, `is_not_null`)

**Array:** `array`, `array_size`, `size`, `array_contains`, `array_join`, `array_max`, `array_min`, `element_at`, `array_sort`, `array_distinct`, `array_slice`, `explode`, `explode_outer`, `array_position`, `array_compact`, `array_remove`, `array_repeat`, `array_flatten`, `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean`, `posexplode`, `posexplode_outer`, `arrays_overlap`, `arrays_zip`, `array_agg`

**Map:** `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays`, `map_concat`, `map_contains_key`, `map_filter`, `map_from_entries`, `map_zip_with`, `get` (Phase 18), `str_to_map` (Phase 21)

**Struct:** `struct`, `named_struct` (Phase 18), `transform_keys`, `transform_values` (Phase 21)

**Binary (Phase 21):** `hex`, `unhex`, `bin`, `getbit`

**Array (Phase 18):** `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union`, `zip_with`

**Aggregates (Phase 19):** `any_value`, `bool_and`, `bool_or`, `every`, `some`, `count_if`, `max_by`, `min_by`, `percentile`, `product`, `collect_list`, `collect_set`

**Try_* (Phase 19):** `try_divide`, `try_add`, `try_subtract`, `try_multiply`, `try_element_at`

**Misc (Phase 19):** `width_bucket`, `elt`, `bit_length`, `typeof`

**JSON:** `get_json_object`, `from_json`, `to_json`, `json_array_length`, `parse_url` (Phase 23)

**URL (Phase 23):** `url_decode`, `url_encode`

**Misc (Phase 23):** `isin`, `isin_i64`, `isin_str`, `equal_null`, `hash`, `shift_left`, `shift_right`, `shift_right_unsigned`, `version`, `stack`

---

## 2. Gaps (in Sparkless 3.28.0, not in Robin-Sparkless)

Functions and aliases that exist in Sparkless 3.28.0 but are **not** implemented in robin-sparkless, grouped by category.

### 2.1 Approx / distinct aggregates
- ~~`approx_count_distinct`~~ — **implemented** (GroupedData)
- `approx_percentile`

### 2.2 Crypto / binary
- `aes_decrypt`, `aes_encrypt`
- `try_aes_decrypt`
- `to_binary`, `try_to_binary`
- `decode`, `encode`
- ~~`hex`, `unhex`~~ — **implemented (Phase 21)**
- ~~`bin`~~ — **implemented (Phase 21)**
- ~~`getbit`~~ — **implemented (Phase 21)**
- ~~`bit_length`~~ — **implemented (Phase 19)**; `octet_length`, `char_length`, `character_length`

### 2.3 Array (additional)
- `aggregate` (array aggregate) — deferred
- ~~`array_agg`~~ — **implemented (Phase 21)**
- ~~`array_append`, `array_prepend`, `array_insert`~~ — **implemented (Phase 18)**
- ~~`array_except`, `array_intersect`, `array_union`~~ — **implemented (Phase 18)**
- ~~`arrays_overlap`, `arrays_zip`~~ — **implemented (Phase 21)**
- `cardinality` (array size alias in some contexts)
- `exists` (array), `filter` (array), `forall` (array) — we have array_exists, array_filter, array_forall
- `slice` — we have `array_slice`
- `sort_array` — we have `array_sort`
- `flatten` — we have `array_flatten`
- ~~`explode_outer`, `posexplode_outer`~~ — **implemented (Phase 21)**
- ~~`zip_with`~~ — **implemented (Phase 18)**

### 2.4 Map (additional)
- ~~`map_concat`, `map_contains_key`, `map_filter`, `map_from_entries`, `map_zip_with`, `get`~~ — **implemented (Phase 18)**
- ~~`str_to_map`~~ — **implemented (Phase 21)**

### 2.5 Struct / type
- ~~`named_struct`, `struct`~~ — **implemented (Phase 18)**
- ~~`transform_keys`, `transform_values`~~ — **implemented (Phase 21)**
- `transform` — we have `array_transform`

### 2.6 Ordering / sort helpers (Phase 20)
- ~~`asc`, `asc_nulls_first`, `asc_nulls_last`~~ — **implemented**
- ~~`desc`, `desc_nulls_first`, `desc_nulls_last`~~ — **implemented**

### 2.7 Assert / control
- ~~`assert_true`~~ — **implemented (Phase 24; control)**
- ~~`raise_error`~~ — **implemented (Phase 24; control)**

### 2.8 Bit / bitmap (PySpark 3.5+ style)
- ~~`bit_and`, `bit_or`, `bit_xor`, `bit_count`, `bit_get`~~ — **implemented (Phase 24; bit operations)**
- ~~`bitwiseNOT`, `bitwise_not`~~ — **implemented (Phase 24; aliases)**
- `bitmap_bit_position`, `bitmap_bucket_number`, `bitmap_construct_agg`, `bitmap_count`, `bitmap_or_agg`

### 2.9 Boolean aggregates
- ~~`bool_and`, `bool_or`, `every`, `some`~~ — **implemented (Phase 19)**

### 2.10 JVM / runtime (defer)
- ~~`broadcast`~~ — **implemented as stub (Phase 24; no-op)**  
- ~~`spark_partition_id`~~ — **implemented as stub (Phase 24; constant 0)**  
- ~~`input_file_name`~~ — **implemented as stub (Phase 24; empty string)**  
- ~~`monotonically_increasing_id`~~ — **implemented as stub (Phase 24; constant 0)**  
- ~~`current_catalog`, `current_database`, `current_schema`, `current_user`, `user`~~ — **implemented as stubs (Phase 24; placeholders)**

### 2.11 Rounding / numeric (Phase 20)
- ~~`bround`~~ — **implemented**
- ~~`pmod`~~ — **implemented (Phase 17)**
- ~~`factorial`~~ — **implemented (Phase 17)**
- ~~`width_bucket`~~ — **implemented (Phase 19)**

### 2.12 String (additional)
- ~~`btrim`~~ — **implemented (Phase 21)**
- ~~`conv`~~ — **implemented (Phase 21)**
- ~~`format_string`, `printf`~~ — **implemented (Phase 16)**
- ~~`find_in_set`~~ — **implemented (Phase 16)**
- ~~`split_part`~~ — **implemented (Phase 16)**
- ~~`locate`~~ — **implemented (Phase 21)**

### 2.13 Regex (additional)
- `regexp` — we have `rlike`
- ~~`regexp_count`, `regexp_instr`, `regexp_substr`~~ — **implemented (Phase 16)**

### 2.14 Math / trig (additional) (Phase 20)
- ~~`cot`, `csc`, `sec`~~ — **implemented**
- ~~`e`, `pi`~~ — **implemented**
- `sign` (we have `signum`)
- ~~`negate`, `negative`, `positive`~~ — **implemented**

### 2.15 Aggregates (additional) (Phase 20)
- ~~`any_value`, `count_if`, `max_by`, `min_by`, `percentile`, `product`~~ — **implemented (Phase 19)**
- `first`, `last` (aggregate) — we have GroupedData.first, last
- `covar_pop`, `covar_samp`, `corr` (deferred for groupBy agg)
- `mean` — we have `avg`
- ~~`median`, `mode`~~ — **implemented (Phase 20)**
- `percentile_approx` (deferred)
- `std` — we have `stddev`
- ~~`stddev_pop`, `stddev_samp`~~ — **implemented (Phase 20)**
- ~~`var_pop`, `var_samp`~~ — we have `variance`; **implemented (Phase 20)**
- `kurtosis`, `skewness` (deferred)

### 2.16 Collect aggregates
- ~~`collect_list`, `collect_set`~~ — **implemented (Phase 19)**

### 2.17 Datetime / timestamp (additional)
- ~~`convert_timezone`, `current_timezone`~~ — **implemented (Phase 22)**
- ~~`curdate`~~ — **implemented (Phase 22)**
- ~~`date_diff`, `date_part`~~ — **implemented (Phase 22)**
- `date_trunc` — we have `trunc`
- ~~`dateadd`, `datepart`~~ — **implemented (Phase 22)**
- ~~`date_from_unix_date`~~ — **implemented (Phase 17)**
- ~~`dayname`~~ — **implemented (Phase 22)**
- ~~`days`, `hours`, `months`, `years`~~ — **implemented (Phase 22)**
- ~~`extract`~~ — **implemented (Phase 22)**
- ~~`localtimestamp`~~ — **implemented (Phase 22)**
- ~~`make_date`~~ — **implemented (Phase 17)**; `make_dt_interval`, `make_ym_interval` deferred; ~~`make_interval`, `make_timestamp`, `make_timestamp_ntz`~~ — **implemented (Phase 22)**
- ~~`now`~~ — **implemented (Phase 22)**
- ~~`timestamp_micros`, `timestamp_millis`, `timestamp_seconds`~~ — **implemented (Phase 17)**
- ~~`timestampadd`, `timestampdiff`~~ — **implemented (Phase 22)**
- ~~`to_timestamp`~~ — **implemented (Phase 22)**; `to_timestamp_ltz`, `to_timestamp_ntz` deferred
- ~~`to_unix_timestamp`, `from_unixtime`~~ — **implemented (Phase 17)**
- ~~`unix_date`~~ — **implemented (Phase 17)**; ~~`unix_micros`, `unix_millis`, `unix_seconds`~~ — **implemented (Phase 22)**
- ~~`weekday`~~ — **implemented (Phase 22)**

### 2.18 JSON / XML
- ~~`json_array_length`~~, `json_object_keys`, `json_tuple` — **json_array_length implemented (Phase 23)**
- `from_xml`, `to_xml`, `schema_of_xml`
- ~~`parse_url`~~ — **implemented (Phase 23)**

### 2.19 Schema / I/O
- `from_csv`, `to_csv`
- `schema_of_csv`, `schema_of_json`

### 2.20 Try-* arithmetic (Phase 20)
- ~~`try_add`, `try_divide`, `try_subtract`, `try_multiply`, `try_element_at`~~ — **implemented (Phase 19)**
- ~~`try_sum`, `try_avg`~~ — **implemented (Phase 20)**
- ~~`try_to_number`, `try_to_timestamp`~~ — **implemented (Phase 21)**

### 2.21 Type / cast
- ~~`to_char`, `to_number`, `to_varchar`~~ — **implemented (Phase 21)**
- ~~`typeof`~~ — **implemented (Phase 19)**

### 2.22 URL / string
- ~~`url_decode`, `url_encode`~~ — **implemented (Phase 23)**

### 2.23 Regression (defer)
- `regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`, `regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy`

### 2.24 Misc
- `call_function`
- `case_when` — we have `when` + WhenBuilder
- ~~`equal_null`~~ — **implemented (Phase 23)**
- `grouping`, `grouping_id`
- ~~`hash`~~ — **implemented (Phase 23)**
- `inline`, `inline_outer`
- ~~`isin`~~ — **implemented (Phase 23)**
- `sentences`
- `sequence`
- `sha` (generic)
- ~~`shiftLeft`, `shiftRight`, `shiftRightUnsigned`~~ — **implemented (Phase 23)**
- `shuffle`
- ~~`stack`~~ — **implemented (Phase 23)** (alias for struct_)
- ~~`version`~~ — **implemented (Phase 23)**
- `window`, `window_time`
- `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_number`, `xpath_short`, `xpath_string`

### 2.25 Random / UDF
- ~~`rand`, `randn`~~ — **implemented (Phase 24): real RNG with optional seed; per-row values when used in with_column/with_columns (see PYSPARK_DIFFERENCES.md)**  
- `udf`, `pandas_udf`

### 2.26 Deferred (ML / JVM / UDTF)
- `count_min_sketch`, `histogram_numeric`, `hll_sketch_agg`, `hll_sketch_estimate`, `hll_union`, `hll_union_agg`
- `session_window`
- `mapInArrow`, `to`, `to_koalas`, `to_pandas_on_spark`, `withMetadata`, `get_active_spark_context`, `current_catalog`, `current_database`, `current_schema`, `current_user`
- `call_udf`, `udtf`, `reduce`, `reflect`, `java_method`
- `unpersist` (DataFrame method in Sparkless)

---

## 3. DataFrame Methods (Sparkless vs Robin-Sparkless)

Sparkless 3.28.0 exposes many DataFrame methods (see PYSPARK_FUNCTION_MATRIX.md). Robin-sparkless implements a subset, including: `select`, `filter`/`where`, `withColumn`, `withColumnRenamed`, `drop`, `limit`, `orderBy`/`sort`, `groupBy`/`groupby` + agg, `join`, `union`/`unionAll`, `unionByName`, `distinct`, `dropDuplicates`/`drop_duplicates`, `fillna`/`fillna`, `dropna`, `count`, `first`, `collect`, `summary`/`describe`, `coalesce` (repartition), `col_regex`/`colRegex`.  

Not implemented in robin-sparkless (examples): `corr`, `unpersist`, `mapInArrow`, `pandas_api`, `to`, `createGlobalTempView`, `createOrReplaceGlobalTempView`, `createOrReplaceTempView`, `createTempView`, `cache`, `checkpoint`, `localCheckpoint`, `persist`, `observe`, `randomSplit`, `repartition`, `repartitionByRange`, `sample`, `sampleBy`, `sameSemantics`, `semanticHash`, `toDF`, `toJSON`, `toLocalIterator`, `toPandas`, `withWatermark`, `writeTo`, `melt`/`unpivot`, `offset`, etc. A full method-by-method list can be derived from PYSPARK_FUNCTION_MATRIX.md “DataFrame Methods” table.

---

## 4. How to Reproduce

```bash
# Install Sparkless 3.28.0
python3 -m venv .venv-sparkless
.venv-sparkless/bin/pip install sparkless==3.28.0

# List Sparkless function names
.venv-sparkless/bin/python -c "
import sparkless.sql.functions as F
names = sorted([x for x in dir(F) if not x.startswith('_') and x not in ('Functions','Any','Dict','attr_name','attr_value','warnings')])
funcs = sorted([x for x in dir(F.Functions) if not x.startswith('_')])
for n in sorted(set(names)|set(funcs)):
    print(n)
"
```

Robin-sparkless function names are defined in `src/functions.rs` (public functions) and `src/column.rs` (Column methods); Python bindings in `src/python/mod.rs` mirror the same API.

---

## 5. References

- **Sparkless:** [PyPI sparkless 3.28.0](https://pypi.org/project/sparkless/3.28.0/)
- **PySpark matrix (Sparkless repo):** [PYSPARK_FUNCTION_MATRIX.md](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md)
- **Robin-sparkless:** `docs/PHASE15_GAP_LIST.md`, `docs/PYSPARK_DIFFERENCES.md`, `docs/PARITY_STATUS.md`
