# Parity Check: Robin-Sparkless vs Sparkless 3.28.0

**Date:** February 2026  
**Method:** Direct comparison of Sparkless 3.28.0 `sparkless.sql.functions` API vs robin-sparkless implementation.

## Robin-Sparkless Implemented Functions (by Sparkless name)

The following Sparkless function names have an equivalent implementation in robin-sparkless:

### Implemented (matches Sparkless 3.28.0)

| Sparkless name | Robin-Sparkless | Notes |
|----------------|-----------------|-------|
| abs | ✅ | |
| acos | ✅ | |
| acosh | ✅ | |
| add_months | ✅ | |
| any_value | ✅ | Phase 19 |
| approx_count_distinct | ✅ | GroupedData method |
| array | ✅ | |
| array_append | ✅ | Phase 18 |
| array_compact | ✅ | |
| array_contains | ✅ | |
| array_distinct | ✅ | |
| array_except | ✅ | Phase 18 |
| array_insert | ✅ | Phase 18 |
| array_intersect | ✅ | Phase 18 |
| array_join | ✅ | |
| array_max | ✅ | |
| array_min | ✅ | |
| array_position | ✅ | |
| array_prepend | ✅ | Phase 18 |
| array_remove | ✅ | |
| array_repeat | ✅ | |
| array_size | ✅ | |
| array_sort | ✅ | |
| array_union | ✅ | Phase 18 |
| ascii | ✅ | |
| asin | ✅ | |
| asinh | ✅ | |
| atan | ✅ | |
| atan2 | ✅ | |
| atanh | ✅ | |
| avg | ✅ | |
| base64 | ✅ | |
| bit_length | ✅ | Phase 19 |
| bool_and | ✅ | Phase 19 |
| bool_or | ✅ | Phase 19 |
| cast | ✅ | |
| cbrt | ✅ | |
| ceil | ✅ | |
| ceiling | ✅ | |
| char | ✅ | |
| chr | ✅ | |
| coalesce | ✅ | |
| col | ✅ | |
| collect_list | ✅ | Phase 19 |
| collect_set | ✅ | Phase 19 |
| concat | ✅ | |
| concat_ws | ✅ | |
| contains | ✅ | |
| cos | ✅ | |
| cosh | ✅ | |
| count | ✅ | |
| count_distinct | ✅ | |
| count_if | ✅ | Phase 19 |
| crc32 | ✅ | |
| create_map | ✅ | |
| cume_dist | ✅ | |
| current_date | ✅ | |
| current_timestamp | ✅ | |
| date_add | ✅ | |
| date_format | ✅ | |
| date_from_unix_date | ✅ | Phase 17 |
| date_sub | ✅ | |
| datediff | ✅ | |
| day | ✅ | |
| dayofmonth | ✅ | |
| dayofweek | ✅ | |
| dayofyear | ✅ | |
| decode | ❌ | Not implemented |
| degrees | ✅ | |
| dense_rank | ✅ | |
| element_at | ✅ | |
| elt | ✅ | Phase 19 |
| endswith | ✅ | |
| every | ✅ | Phase 19 (alias bool_and) |
| exp | ✅ | |
| explode | ✅ | |
| expm1 | ✅ | |
| factorial | ✅ | Phase 17 |
| find_in_set | ✅ | Phase 16 |
| first | ✅ | GroupedData |
| first_value | ✅ | |
| floor | ✅ | |
| format_number | ✅ | |
| format_string | ✅ | Phase 16 |
| from_json | ✅ | |
| from_unixtime | ✅ | Phase 17 |
| get | ✅ | Phase 18 (map element) |
| get_json_object | ✅ | |
| greatest | ✅ | |
| hour | ✅ | |
| hypot | ✅ | |
| ifnull | ✅ | |
| ilike | ✅ | |
| initcap | ✅ | |
| instr | ✅ | |
| isnan | ✅ | |
| isnotnull | ✅ | |
| isnull | ✅ | |
| lag | ✅ | |
| last | ✅ | GroupedData |
| last_day | ✅ | |
| last_value | ✅ | |
| lcase | ✅ | |
| lead | ✅ | |
| least | ✅ | |
| left | ✅ | |
| length | ✅ | |
| levenshtein | ✅ | |
| like | ✅ | |
| lit | ✅ | lit_i32, lit_i64, lit_f64, lit_bool, lit_str |
| ln | ✅ | |
| log | ✅ | |
| log10 | ✅ | |
| log1p | ✅ | |
| log2 | ✅ | |
| lower | ✅ | |
| lpad | ✅ | |
| ltrim | ✅ | |
| make_date | ✅ | Phase 17 |
| map_concat | ✅ | Phase 18 |
| map_contains_key | ✅ | Phase 18 |
| map_entries | ✅ | |
| map_filter | ✅ | Phase 18 |
| map_from_arrays | ✅ | |
| map_from_entries | ✅ | Phase 18 |
| map_keys | ✅ | |
| map_values | ✅ | |
| map_zip_with | ✅ | Phase 18 |
| mask | ✅ | |
| max | ✅ | |
| max_by | ✅ | Phase 19 |
| md5 | ✅ | |
| min | ✅ | |
| min_by | ✅ | Phase 19 |
| minute | ✅ | |
| month | ✅ | |
| months_between | ✅ | |
| named_struct | ✅ | Phase 18 |
| nanvl | ✅ | |
| next_day | ✅ | |
| nth_value | ✅ | |
| ntile | ✅ | |
| nullif | ✅ | |
| nvl | ✅ | |
| nvl2 | ✅ | |
| overlay | ✅ | |
| percent_rank | ✅ | |
| percentile | ✅ | Phase 19 |
| pmod | ✅ | Phase 17 |
| posexplode | ✅ | |
| position | ✅ | |
| pow | ✅ | |
| power | ✅ | |
| printf | ✅ | Phase 16 |
| product | ✅ | Phase 19 |
| quarter | ✅ | |
| radians | ✅ | |
| rank | ✅ | |
| regexp_count | ✅ | Phase 16 |
| regexp_extract | ✅ | |
| regexp_extract_all | ✅ | |
| regexp_instr | ✅ | Phase 16 |
| regexp_like | ✅ | |
| regexp_replace | ✅ | |
| regexp_substr | ✅ | Phase 16 |
| repeat | ✅ | |
| replace | ✅ | |
| reverse | ✅ | |
| right | ✅ | |
| rint | ✅ | |
| rlike | ✅ | (regexp alias) |
| round | ✅ | |
| row_number | ✅ | |
| rpad | ✅ | |
| rtrim | ✅ | |
| second | ✅ | |
| sha1 | ✅ | |
| sha2 | ✅ | |
| signum | ✅ | (sign alias) |
| sin | ✅ | |
| sinh | ✅ | |
| size | ✅ | |
| some | ✅ | Phase 19 (alias bool_or) |
| soundex | ✅ | |
| split | ✅ | |
| split_part | ✅ | Phase 16 |
| sqrt | ✅ | |
| startswith | ✅ | |
| stddev | ✅ | |
| struct | ✅ | Phase 18 |
| substr | ✅ | |
| substring | ✅ | |
| substring_index | ✅ | |
| sum | ✅ | |
| tan | ✅ | |
| tanh | ✅ | |
| timestamp_micros | ✅ | Phase 17 |
| timestamp_millis | ✅ | Phase 17 |
| timestamp_seconds | ✅ | Phase 17 |
| to_date | ✅ | |
| to_degrees | ✅ | |
| to_json | ✅ | |
| to_radians | ✅ | |
| to_unix_timestamp | ✅ | Phase 17 |
| translate | ✅ | |
| trim | ✅ | |
| trunc | ✅ | |
| try_add | ✅ | Phase 19 |
| try_divide | ✅ | Phase 19 |
| try_element_at | ✅ | Phase 19 |
| try_multiply | ✅ | Phase 19 |
| try_subtract | ✅ | Phase 19 |
| typeof | ✅ | Phase 19 |
| ucase | ✅ | |
| unbase64 | ✅ | |
| unix_date | ✅ | Phase 17 |
| unix_timestamp | ✅ | Phase 17 |
| upper | ✅ | |
| variance | ✅ | |
| weekofyear | ✅ | |
| when | ✅ | (case_when equivalent) |
| width_bucket | ✅ | Phase 19 |
| xxhash64 | ✅ | |
| year | ✅ | |
| zip_with | ✅ | Phase 18 |

## Not Implemented in Robin-Sparkless (Gaps)

### Approx / distinct
- approx_percentile

### Crypto / binary
- aes_decrypt, aes_encrypt, try_aes_decrypt
- to_binary, try_to_binary
- decode, encode
- hex, unhex
- bin
- getbit

### Array (additional)
- aggregate (array aggregate)
- array_agg
- arrays_overlap, arrays_zip
- explode_outer, posexplode_outer

### Map
- str_to_map

### Struct
- transform_keys, transform_values

### Ordering
- asc, asc_nulls_first, asc_nulls_last
- desc, desc_nulls_first, desc_nulls_last

### Control
- assert_true, raise_error

### Bit / bitmap
- bit_and, bit_or, bit_xor, bit_count, bit_get
- bitwiseNOT, bitwise_not
- bitmap_* functions

### JVM / runtime (defer)
- broadcast, spark_partition_id, input_file_name
- monotonically_increasing_id
- current_catalog, current_database, current_schema, current_user, user

### Numeric
- bround

### String
- btrim, conv, locate (we have instr/position)

### Math
- cot, csc, sec, e, pi
- negate, negative, positive

### Aggregates
- covar_pop, covar_samp, corr
- median, mode
- percentile_approx
- stddev_pop, stddev_samp, var_pop, var_samp
- kurtosis, skewness
- try_sum, try_avg

### Datetime
- convert_timezone, current_timezone
- curdate, date_diff, date_part, date_trunc
- dateadd, datepart
- dayname, days, hours, months, years
- extract, localtimestamp
- make_dt_interval, make_interval, make_timestamp, make_timestamp_ltz, make_timestamp_ntz, make_ym_interval
- now
- timestampadd, timestampdiff
- to_timestamp, to_timestamp_ltz, to_timestamp_ntz
- from_utc_timestamp, to_utc_timestamp
- unix_micros, unix_millis, unix_seconds
- weekday

### JSON / XML
- json_array_length, json_object_keys, json_tuple
- from_xml, to_xml, schema_of_xml
- parse_url

### Schema / I/O
- from_csv, to_csv
- schema_of_csv, schema_of_json

### Type / cast
- to_char, to_number, to_varchar
- try_to_number, try_to_timestamp

### URL
- url_decode, url_encode

### Misc
- call_function, equal_null
- grouping, grouping_id
- hash
- inline, inline_outer
- isin
- sentences, sequence
- sha (generic)
- shiftLeft, shiftRight, shiftRightUnsigned
- shuffle, stack
- version
- window, window_time
- xpath_* functions

### Regression (defer)
- regr_*

### Random / UDF
- rand, randn
- udf, pandas_udf

## Summary

| Metric | Sparkless 3.28.0 | Robin-Sparkless | Coverage |
|--------|-------------------|-----------------|----------|
| **Function names** | ~280+ distinct | ~165 implemented | ~59% |
| **Parity fixtures** | — | 128 passing | — |

**Conclusion:** Robin-sparkless has substantial parity with Sparkless 3.28.0 for the core PySpark operations used in typical data pipelines (filter, select, groupBy, join, window, array, map, string, math, datetime, type/conditional). Remaining gaps (~115 functions) are addressed in **ROADMAP Phases 20–24 – Full parity** (5 manageable phases; see [ROADMAP.md](ROADMAP.md)). The 128 parity fixtures validate behavior for implemented functions.
