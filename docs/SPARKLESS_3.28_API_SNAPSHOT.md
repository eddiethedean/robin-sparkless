# Sparkless 3.28.0 API Snapshot

**Generated:** By installing `sparkless==3.28.0` and introspecting `sparkless.sql.functions.F.Functions` and DataFrame methods.

Use this to double-check parity: `F` is accessed as `import sparkless.sql.functions as F` then `F.col`, `F.sum`, etc. The actual callables live on `F.Functions` (417 names). DataFrame methods come from `SparkSession().createDataFrame(...)` (95 methods).

## Functions (F.Functions) — 417 names

```
abs, acos, acosh, add_months, aes_decrypt, aes_encrypt, aggregate, any_value,
approxCountDistinct, approx_count_distinct, approx_percentile, array, array_agg,
array_append, array_compact, array_contains, array_distinct, array_except,
array_insert, array_intersect, array_join, array_max, array_min, array_position,
array_prepend, array_remove, array_repeat, array_size, array_sort, array_union,
arrays_overlap, arrays_zip, asc, asc_nulls_first, asc_nulls_last, ascii, asin,
asinh, assert_true, atan, atan2, atanh, avg, base64, bin, bit_and, bit_count,
bit_get, bit_length, bit_or, bit_xor, bitmap_bit_position, bitmap_bucket_number,
bitmap_construct_agg, bitmap_count, bitmap_or_agg, bitwiseNOT, bitwise_not,
bool_and, bool_or, broadcast, bround, btrim, call_function, cardinality,
case_when, cast, cbrt, ceil, ceiling, char, char_length, character_length,
coalesce, col, collect_list, collect_set, column, concat, concat_ws, contains,
conv, convert_timezone, corr, cos, cosh, cot, count, countDistinct, count_distinct,
count_if, covar_pop, covar_samp, crc32, create_map, csc, cume_dist, curdate,
current_catalog, current_database, current_date, current_schema, current_timestamp,
current_timezone, current_user, date_add, date_diff, date_format, date_from_unix_date,
date_part, date_sub, date_trunc, dateadd, datediff, datepart, day, dayname,
dayofmonth, dayofweek, dayofyear, days, decode, degrees, dense_rank, desc,
desc_nulls_first, desc_nulls_last, e, element_at, elt, encode, endswith, equal_null,
every, exists, exp, explode, explode_outer, expm1, expr, extract, factorial,
filter, find_in_set, first, first_value, flatten, floor, forall, format_number,
format_string, from_csv, from_json, from_unixtime, from_utc_timestamp, from_xml,
get, get_json_object, getbit, greatest, grouping, grouping_id, hash, hex, hour,
hours, hypot, ifnull, ilike, initcap, inline, inline_outer, input_file_name,
instr, isin, isnan, isnotnull, isnull, json_array_length, json_object_keys,
json_tuple, kurtosis, lag, last, last_day, last_value, lcase, lead, least, left,
length, levenshtein, like, lit, ln, localtimestamp, locate, log, log10, log1p,
log2, lower, lpad, ltrim, make_date, make_dt_interval, make_interval,
make_timestamp, make_timestamp_ltz, make_timestamp_ntz, make_ym_interval,
map_concat, map_contains_key, map_entries, map_filter, map_from_arrays,
map_from_entries, map_keys, map_values, map_zip_with, mask, max, max_by, md5,
mean, median, min, min_by, minute, mode, monotonically_increasing_id, month,
months, months_between, named_struct, nanvl, negate, negative, next_day, now,
nth_value, ntile, nullif, nvl, nvl2, octet_length, overlay, pandas_udf, parse_url,
percent_rank, percentile, percentile_approx, pi, pmod, posexplode, posexplode_outer,
position, positive, pow, power, printf, product, quarter, radians, raise_error,
rand, randn, rank, regexp, regexp_count, regexp_extract, regexp_extract_all,
regexp_instr, regexp_like, regexp_replace, regexp_substr, regr_avgx, regr_avgy,
regr_count, regr_intercept, regr_r2, regr_slope, regr_sxx, regr_sxy, regr_syy,
repeat, replace, reverse, right, rint, rlike, round, row_number, rpad, rtrim,
schema_of_csv, schema_of_json, schema_of_xml, sec, second, sentences, sequence,
sha, sha1, sha2, shiftLeft, shiftRight, shiftRightUnsigned, shiftleft, shiftright,
shiftrightunsigned, shuffle, sign, signum, sin, sinh, size, skewness, slice,
some, sort_array, soundex, spark_partition_id, split, split_part, sqrt, stack,
startswith, std, stddev, stddev_pop, stddev_samp, str_to_map, struct, substr,
substring, substring_index, sum, sumDistinct, sum_distinct, tan, tanh,
timestamp_micros, timestamp_millis, timestamp_seconds, timestampadd, timestampdiff,
toDegrees, toRadians, to_binary, to_char, to_csv, to_date, to_json, to_number,
to_str, to_timestamp, to_timestamp_ltz, to_timestamp_ntz, to_unix_timestamp,
to_utc_timestamp, to_varchar, to_xml, transform, transform_keys, transform_values,
translate, trim, trunc, try_add, try_aes_decrypt, try_avg, try_divide,
try_element_at, try_multiply, try_subtract, try_sum, try_to_binary, try_to_number,
try_to_timestamp, typeof, ucase, udf, unbase64, unhex, unix_date, unix_micros,
unix_millis, unix_seconds, unix_timestamp, upper, url_decode, url_encode, user,
var_pop, var_samp, variance, version, weekday, weekofyear, when, width_bucket,
window, window_time, xpath, xpath_boolean, xpath_double, xpath_float, xpath_int,
xpath_long, xpath_number, xpath_short, xpath_string, xxhash64, year, years, zip_with
```

## DataFrame methods — 95 methods

```
agg, alias, approxQuantile, cache, checkpoint, coalesce, colRegex, collect,
columns, count, cov, createGlobalTempView, createOrReplaceGlobalTempView,
createTempView, createOrReplaceTempView, crossJoin, crosstab, cube, data,
describe, distinct, drop, dropDuplicates, drop_duplicates, dropna, dtypes,
exceptAll, explain, fillna, filter, first, foreach, foreachPartition, freqItems,
groupBy, groupby, head, hint, inputFiles, intersect, intersectAll, isEmpty,
isLocal, isStreaming, join, limit, localCheckpoint, mapInPandas, mapPartitions,
melt, na, observe, offset, orderBy, persist, printSchema, randomSplit, rdd,
registerTempTable, repartition, repartitionByRange, replace, rollup, sameSemantics,
sample, sampleBy, schema, select, selectExpr, semanticHash, show, sort,
sortWithinPartitions, storage, subtract, summary, tail, take, toDF, toJSON,
toLocalIterator, toPandas, transform, union, unionAll, unionByName, unpivot,
where, withColumn, withColumnRenamed, withColumns, withColumnsRenamed, withWatermark,
write, writeTo
```

## How to reproduce

```bash
python3 -m venv .venv-sparkless
.venv-sparkless/bin/pip install sparkless==3.28.0
.venv-sparkless/bin/python -c "
import sparkless.sql.functions as F
print(sorted([x for x in dir(F.Functions) if not x.startswith('_')]))
"
# DataFrame: SparkSession().createDataFrame([], []). then dir(df)
```

## Comparison with robin-sparkless

- **Functions:** See [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md) and [PARITY_CHECK_SPARKLESS_3.28.md](PARITY_CHECK_SPARKLESS_3.28.md) for which of these 417 are implemented in robin-sparkless. Main gaps: aes_*, decode, encode, to_binary, try_to_binary, aggregate (array), approx_percentile, bitmap_*, covar_pop/covar_samp/corr (groupBy), percentile_approx, kurtosis, skewness, regr_*, from_csv, to_csv, schema_of_*, from_xml, to_xml, xpath_*, call_function, grouping, grouping_id, inline, inline_outer, sentences, sha, session_window, current_catalog/current_database/current_schema/current_user (Sparkless has them; we have stubs), etc.
- **DataFrame methods:** Robin-sparkless implements most of the above; not implemented: corr, unpersist, mapInPandas, mapPartitions, pandas_api, to, createGlobalTempView, createOrReplaceGlobalTempView, createTempView, createOrReplaceTempView (we have SQL feature with create_or_replace_temp_view), cache/persist/unpersist (we have no-op stubs), observe (we have no-op), randomSplit/sample/sampleBy (we have them), toDF, toJSON, toLocalIterator, toPandas (we have to_pandas), withWatermark (we have no-op), writeTo, write (we have write_delta with delta feature), rdd, data, dtypes, storage, cube, rollup, foreach, foreachPartition, isStreaming, registerTempTable, repartitionByRange.
