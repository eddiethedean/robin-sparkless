# What Robin-Sparkless Does NOT Have (vs Sparkless 3.28.0)

This list is **only** items that **Sparkless has** and **robin-sparkless does not** (or has only as a stub/no-op). Implemented equivalents (e.g. `signum` for `sign`, `avg` for `mean`, `trunc` for `date_trunc`) are not listed.

**PySpark parity scope:** All items below are **PySpark APIs** (or direct Sparkless equivalents of PySpark), unless marked as *Sparkless-specific*. PySpark reference: [pyspark.sql](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) (SparkSession, DataFrame, Catalog, functions), [pyspark.sql.functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html), and [RDD / SparkContext](https://spark.apache.org/docs/latest/api/python/reference/pyspark.html). Tracking these as unimplemented is for **PySpark parity**; implementing them would align robin-sparkless with standard PySpark behavior.

For a consolidated view of deferred/optional scope (XML, UDF, streaming, RDD, sketch, Catalog DDL), see [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md).

---

## Functions (sparkless.sql.functions) — Missing or stub only

### Crypto / binary
- ~~`aes_decrypt`, `aes_encrypt`, `try_aes_decrypt`~~ — **implemented** (AES-128-GCM; see PYSPARK_DIFFERENCES)
- ~~`to_binary`, `try_to_binary`~~ — **implemented**
- ~~`decode`, `encode`~~ — **implemented** (UTF-8, hex)
- ~~`octet_length`, `char_length`, `character_length`~~ — **implemented**

### Approx / aggregates
- ~~`approx_percentile`~~ — **implemented**
- ~~`percentile_approx`~~ — **implemented**
- ~~`covar_pop`, `covar_samp`, `corr` as groupBy aggregations~~ — **implemented**
- ~~`kurtosis`, `skewness`~~ — **implemented**

### Array
- ~~`aggregate` (array fold)~~ — **implemented** (simplified: zero + sum(list))
- ~~`cardinality`~~ — **implemented** (alias for size)

### Bitmap (PySpark 3.5+)
- ~~`bitmap_bit_position`, `bitmap_bucket_number`, `bitmap_construct_agg`, `bitmap_count`, `bitmap_or_agg`~~ — **implemented**

### Datetime / interval
- ~~`make_dt_interval`, `make_ym_interval`~~ — **implemented**
- ~~`to_timestamp_ltz`, `to_timestamp_ntz`~~ — **implemented** (we have `to_timestamp`)

### JSON / XML / CSV
- ~~`json_object_keys`, `json_tuple`~~ — **implemented**
- `from_xml`, `to_xml`, `schema_of_xml` (PySpark 3.4+/4.0+; optional/deferred)
- ~~`from_csv`, `to_csv`~~ — **implemented** (minimal)
- ~~`schema_of_csv`, `schema_of_json`~~ — **implemented** (stub: return literal schema string)

### Misc / UDF / JVM
- `call_function` (stub: not supported; *Sparkless-specific* — PySpark equivalent: register UDF with `spark.udf.register` then use the name in `expr()` or SQL)
- ~~`grouping`, `grouping_id`~~ — **implemented** (stub: return 0)
- ~~`inline`, `inline_outer`~~ — **implemented** (explode list of structs; use unnest for struct fields)
- **sentences** (optional/deferred): PySpark `pyspark.sql.functions.sentences` — NLP string→array of array of words; implement only if prioritized.
- ~~`sequence`~~ — **implemented** (generate array of numbers)
- ~~`sha`~~ — we have sha1, sha2
- ~~`shuffle`~~ — **implemented**
- `window`, `window_time` (PySpark; we have `.over()`; thin wrappers if needed; see PYSPARK_DIFFERENCES)
- `pandas_udf` (PySpark `pandas_udf` decorator for scalar, grouped map, and other function types; robin-sparkless currently supports only a minimal grouped aggregation variant via `pandas_udf(..., function_type="grouped_agg")` on the Python side)
- `count_min_sketch`, `histogram_numeric`, `hll_sketch_agg`, `hll_sketch_estimate`, `hll_union`, `hll_union_agg` (PySpark 3.5+; stub/defer)
- `session_window` (PySpark Structured Streaming; stub: no streaming)
- `call_udf`, `udtf`, `reduce`, `reflect`, `java_method` (PySpark JVM/UDTF APIs; stub: not supported)

### Regression
- ~~`regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`, `regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy`~~ — **implemented**

### XPath (deferred)
- **XPath (deferred)**: PySpark `xpath`, `xpath_boolean`, `xpath_double`, etc. (3.5+) — require XML support; stub or defer (see PYSPARK_DIFFERENCES).

### Aliases we don’t expose (Sparkless has, we have equivalent under different name)
- ~~`sign`~~ — alias of signum
- ~~`std`~~ — alias of stddev
- ~~`mean`~~ — alias of avg
- ~~`date_trunc`~~ — alias of trunc
- ~~`regexp`~~ — alias of rlike

---

## DataFrame methods — Missing or no-op only

- **`corr`** — We have `df.stat().corr(col1, col2)` (scalar) and `df.corr()` (correlation matrix DataFrame).
- ~~**`createGlobalTempView`**, **`createOrReplaceGlobalTempView`**~~ — **implemented** (stub: same catalog as temp view).
- **`createTempView`**, **`createOrReplaceTempView`** — we expose `create_or_replace_temp_view` (SQL feature).
- ~~**`cube`**, **`rollup`**~~ — **implemented** (multiple grouping sets then union).
- ~~**`data`**~~ — **implemented** (best-effort: same as `collect()`, list of row dicts).
- ~~**`dtypes`**~~ — **implemented** (returns list of (name, dtype_string)).
- **`foreach`**, **`foreachPartition`** — PySpark DataFrame; stub: raise `NotImplementedError` (see PYSPARK_DIFFERENCES).
- **`mapInPandas`**, **`mapPartitions`** — PySpark DataFrame; stub: raise `NotImplementedError`.
- **`rdd`** — PySpark `DataFrame.rdd`; stub: raise `NotImplementedError` (use `collect()` or `toLocalIterator()` for local data).
- **`registerTempTable`** — legacy; we have `create_or_replace_temp_view`.
- ~~**`repartitionByRange`**~~ — **implemented** (no-op).
- **`sameSemantics`**, **`semanticHash`** — we have no-op stubs that return fixed values.
- ~~**`sortWithinPartitions`**~~ — **implemented** (no-op).
- **`storageLevel`** — PySpark; stub: returns `None` (eager execution).
- **`to`** — PySpark generic writer; we have `write_delta` with delta feature.
- ~~**`toLocalIterator`**~~ — **implemented** (best-effort: same as `collect()`, iterable of rows).
- ~~**`unpersist`**~~ — we have it (no-op).
- **`withWatermark`** — PySpark Structured Streaming; no-op stub (streaming not supported).
- ~~**`write`**~~ — **implemented** (generic write: parquet/csv/json, mode overwrite/append). **`writeTo`** — PySpark `DataFrameWriterV2` / catalog table API; stub or use write to path.
- **`isStreaming`** — PySpark; stub: always returns `False`.

---

## Summary counts (approximate)

| Category              | Sparkless | Robin-Sparkless missing (approx) |
|-----------------------|-----------|-----------------------------------|
| **Functions**         | 417       | ~55–60 (crypto, XML, regr, bitmap, CSV/JSON schema, UDF/UDTF, etc.) |
| **DataFrame methods** | 95        | ~20–25 (cube/rollup, RDD/foreach, writeTo, streaming, etc.) |

For full “we have it” lists see [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md) and [PARITY_CHECK_SPARKLESS_3.28.md](PARITY_CHECK_SPARKLESS_3.28.md).
