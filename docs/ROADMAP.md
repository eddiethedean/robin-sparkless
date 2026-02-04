# Roadmap: PySpark Semantics in a Rust Crate (No JVM)

## Core Principle: PySpark Parity on a Polars Backend

**Primary Goal**: Implement a Rust crate whose behavior closely emulates PySpark's `SparkSession` / `DataFrame` / `Column` semantics, but runs entirely in Rust with Polars as the execution engine (no JVM, no Python runtime).

**Sparkless Integration Goal**: Robin-sparkless is designed to **replace the backend logic** of [Sparkless](https://github.com/eddiethedean/sparkless) (the Python PySpark drop-in). Sparkless would call robin-sparkless via PyO3/FFI for DataFrame execution. See [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) for architecture mapping, structural learnings, and test conversion strategy.

**Constraints**:
- Use Polars as the underlying dataframe/expressions engine.
- Match PySpark behavior where practical (null handling, grouping, joins, expression semantics).
- Stay honest about differences (and document them) when perfect parity is impossible.

### Short-Term Objectives (0–1 month) ✅ **COMPLETED**

1. **Clarify API Surface** ✅
   - ✅ Decided on core PySpark API surface: `SparkSession.builder`, `createDataFrame`, core `DataFrame` transforms/actions
   - ✅ Implemented Rust equivalents with Rust types and error handling

2. **Minimal Parity Slice** ✅
   - ✅ End-to-end support for PySpark-style pipelines in Rust:
     - ✅ Session creation (`SparkSession::builder().get_or_create()`)
     - ✅ `createDataFrame` from simple rows (`Vec<(i64, i64, String)>` tuples)
     - ✅ `select`, `filter`, `groupBy(...).count()`, `orderBy`
     - ✅ `show`, `collect`, `count`
   - ✅ Behavior-checked these operations against PySpark on fixtures (36 scenarios passing)

3. **Behavioral Tests** ✅
   - ✅ Test harness implemented (`tests/parity.rs`):
     - ✅ Runs pipelines in PySpark via `tests/gen_pyspark_cases.py`
     - ✅ Runs logical equivalent through Robin Sparkless
     - ✅ Compares schemas and results with proper null/type handling
   - ✅ JSON fixtures generated and versioned (`tests/fixtures/*.json`)
   - ✅ All parity tests passing for initial slice

### Medium-Term Objectives (1–3 months) ✅ **COMPLETED**

4. **Data Source Readers** ✅ **COMPLETED**
   - ✅ Implement CSV/Parquet/JSON readers using Polars IO
   - ✅ Basic PySpark-like schema inference behavior (header detection, infer_schema_length)
   - ✅ Parity tests for file reading operations (3 new fixtures: read_csv, read_parquet, read_json)

5. **Expression Semantics** ✅ **COMPLETE**
   - ✅ Basic `Column` and functions (`col`, `lit`, basic aggregates)
   - ✅ String literal support in filter expressions
   - ✅ Expand functions: `when`, `coalesce` implemented
   - ✅ `when().then().otherwise()` conditional expressions
   - ✅ `coalesce()` for null handling
   - ✅ `withColumn()` support for adding computed columns
   - ✅ PySpark-style null comparison semantics, including `eqNullSafe` and comparisons against NULL columns
   - ✅ Basic numeric type coercion for int/double comparisons and arithmetic (via Polars expressions)
   - ✅ Complex filter expressions with logical operators (AND, OR, NOT, &&, ||, !) and nested conditions
   - ✅ Arithmetic expressions in withColumn (+, -, *, /) with proper operator precedence
   - ✅ Mixed arithmetic and logical expressions (e.g., `(col('a') + col('b')) > col('c')`)

6. **Grouping and Joins** ✅ **COMPLETE**
   - ✅ Basic `groupBy` + `count()` working with parity tests
   - ✅ Additional aggregates: `sum`, `avg`, `min`, `max` on GroupedData
   - ✅ Generic `agg()` method for multiple aggregations
   - ✅ Column reordering after groupBy to match PySpark order (grouping columns first)
   - ✅ Ensure `groupBy` + aggregates behave like PySpark (verified via groupby_null_keys, groupby_single_group, groupby_single_row_groups; see [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md))
   - ✅ Implement common join types (inner, left, right, outer) with parity fixtures
   - ✅ Multi-agg support in `GroupedData::agg()` with parity fixture
   - ✅ Window functions: row_number, rank, dense_rank, lag, lead with `.over(partition_by)` parity fixtures
- ✅ String functions: upper, lower, substring, concat, concat_ws with parity fixtures

### Longer-Term Objectives (3+ months) – Full Sparkless Backend

The path to full backend replacement is planned in [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md). Summary:

| Phase | Goal | Est. Effort |
|-------|------|-------------|
| **1. Foundation** | Structural alignment, case sensitivity, fixture converter | 2–3 weeks |
| **2. High-Value Functions** | String (length, trim, regexp_*), datetime (to_date, date_add), math (stddev, variance) | 4–6 weeks |
| **3. DataFrame Methods** | union, distinct, drop, fillna, limit, withColumnRenamed | 3–4 weeks |
| **4. PyO3 Bridge** | Python bindings so Sparkless can call robin-sparkless | 4–6 weeks |
| **5. Test Conversion** | Convert 50+ Sparkless tests, CI integration | 2–3 weeks |
| **6. Broad Function Parity** | Array (array_position, array_remove, posexplode ✅; array_repeat, array_flatten ✅ Phase 8), Map/JSON/string 6.4/window ✅ | 8–12 weeks |
| **7. SQL & Advanced** | SQL executor, Delta Lake, performance | ✅ **COMPLETED** (optional features) |
| **8. Remaining Parity** | ✅ **COMPLETED** (Feb 2026): array_repeat, array_flatten, Map (create_map, map_keys, map_values, map_entries, map_from_arrays), String 6.4 (soundex, levenshtein, crc32, xxhash64); window fixtures covered; documentation of differences |

7. **Broader API Coverage** (Phases 2 & 6)
   - String: length, trim, regexp_extract, regexp_replace, split, initcap (string basics ✅ done).
   - Datetime: to_date, date_add, date_sub, date_format, year, month, day, etc.
   - Math: abs, ceil, floor, sqrt, stddev, variance, count_distinct.
   - Array/Map/JSON: array_*, map_*, get_json_object, from_json, to_json.
   - Function parity with Sparkless (403+); use [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) as checklist.
   - UDF story: pure-Rust UDFs; Python UDFs out of scope.

8. **DataFrame Methods** (Phase 3)
   - union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed.
   - crossJoin, replace, describe, cache/persist.

9. **PyO3 Bridge** (Phase 4) ✅ **COMPLETED**
   - Optional `pyo3` feature; `robin_sparkless` Python module with SparkSession, DataFrame, Column, GroupedData.
   - `create_dataframe`, `read_csv`/`read_parquet`/`read_json`, filter, select, join, group_by, collect (list of dicts), etc.
   - See [PYTHON_API.md](PYTHON_API.md). Sparkless BackendFactory "robin" option lives in Sparkless repo.

10. **Performance & Robustness** (Phase 7) ✅ **COMPLETED**
    - Benchmarks: `cargo bench` compares robin-sparkless vs plain Polars (filter → select → groupBy).
    - Target: within ~2x of Polars for supported ops.
    - Error handling: clearer messages (column names, hints); Troubleshooting in [QUICKSTART.md](QUICKSTART.md).

11. **Remaining Parity – Phase 10 & Phase 8** ✅ **COMPLETED**
    - **String 6.4**: mask, translate, substring_index, **soundex, levenshtein, crc32, xxhash64** (all implemented; Phase 8 UDFs via strsim, crc32fast, twox-hash, soundex).
    - **Array extensions**: array_exists, array_forall, array_filter, array_transform, array_sum, array_mean, **array_flatten, array_repeat** (all implemented; Phase 8 map UDFs).
    - **Map (6b)**: **create_map, map_keys, map_values, map_entries, map_from_arrays** (all implemented; Map as List(Struct{key, value}); Phase 8).
    - **JSON (6c)**: get_json_object, from_json, to_json implemented (extract_jsonpath, dtype-struct).
    - **Window fixtures**: percent_rank, cume_dist, ntile, nth_value covered (multi-step workaround in harness).
    - **Documentation of differences**: See [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

### Sparkless Integration Phases (see SPARKLESS_INTEGRATION_ANALYSIS.md, FULL_BACKEND_ROADMAP.md)

- **Phase 1 – Foundation**: Structural alignment (split dataframe.rs), case sensitivity, fixture converter. *Prereqs done: joins, windows, strings.*
- **Phase 2 – High-Value Functions**: String (length, trim, regexp_*), datetime (to_date, date_add), math (stddev, variance)
- **Phase 3 – DataFrame Methods**: union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed ✅ **COMPLETED**
- **Phase 4 – PyO3 Bridge**: Python bindings for Sparkless to call robin-sparkless ✅ **COMPLETED** (see [PYTHON_API.md](PYTHON_API.md))
- **Phase 5 – Test Conversion**: Converter extended (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed); parity discovers `tests/fixtures/` + `tests/fixtures/converted/`; `make sparkless-parity` (set SPARKLESS_EXPECTED_OUTPUTS); [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) for pass/fail; **159 passing** (50+ target met; +10 signature-alignment fixtures) ✅ **COMPLETED**
- **Phase 6 – Broad Parity**: Array (6a ✅; array_position, array_remove, posexplode via list.eval; array_repeat, array_flatten ✅ Phase 8), Map (6b ✅ Phase 8), JSON (6c ✅), additional string (6e ✅; 6.4 soundex/levenshtein/crc32/xxhash64 ✅ Phase 8), window extensions (6d ✅; percent_rank/cume_dist/ntile/nth_value covered).
- **Phase 7 – SQL & Advanced** ✅ **COMPLETED**: Optional **SQL** (`sql` feature: `spark.sql()`, temp views); optional **Delta** (`delta` feature: `read_delta`, `read_delta_with_version`, `write_delta`); benchmarks and error-message improvements. See [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) §7.
- **Phase 8 – Remaining Parity** ✅ **COMPLETED** (Feb 2026): array_repeat, array_flatten; Map (create_map, map_keys, map_values, map_entries, map_from_arrays); String 6.4 (soundex, levenshtein, crc32, xxhash64); window fixtures covered; documentation of differences.

## Success Metrics

We know we're on track if:

- ✅ **Behavioral parity**: For core operations (filter, select, orderBy, groupBy+count/sum/avg/min/max/agg, when/coalesce, basic type coercion, null semantics, joins, window functions, array and string functions, math, datetime, type/conditional), DataFrame methods (union, distinct, drop, dropna, fillna, limit, withColumnRenamed), and file readers (CSV/Parquet/JSON), PySpark and Robin Sparkless produce the same schema and data on test fixtures. **Status: PASSING (159 fixtures)**
- ✅ **Documentation of differences**: Any divergence from PySpark semantics is called out in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) (window, SQL, Delta, Phase 8).
- ✅ **Performance envelope**: For supported operations, we stay within ~2x of doing the same thing directly in Polars. **Status: BENCHMARKED** (`cargo bench`; see [QUICKSTART.md](QUICKSTART.md) § Benchmarks)

**Full backend targets** (see [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md)):

| Metric | Current | After Phase 24 (full parity) | Full Backend (Phase 27) |
|--------|---------|------------------------------|-------------------------|
| Parity fixtures | 159 | 180+ | 180+ |
| Functions | ~283 | ~280 (Sparkless 3.28) | ~280 |
| DataFrame methods | ~55+ | ~55+ | 85 |
| Sparkless tests passing (robin backend) | 0 | — | 200+ |
| PyO3 bridge | ✅ Yes (optional) | Yes | Yes |

## Current Status (February 2026)

**Completed Core Parity Slice:**
- ✅ `SparkSession::create_dataframe` for simple tuples
- ✅ `DataFrame::filter` with simple expressions (`col('x') > N`, string comparisons)
- ✅ `DataFrame::select` 
- ✅ `DataFrame::order_by` / `sort`
- ✅ `DataFrame::group_by` → `GroupedData::count()`
- ✅ `DataFrame::with_column` for adding computed columns
- ✅ `DataFrame::join()` for inner, left, right, outer joins
- ✅ File readers: `read_csv()`, `read_parquet()`, `read_json()` with schema inference
- ✅ Expression functions: `when().then().otherwise()`, `coalesce()`
- ✅ GroupedData aggregates: `sum()`, `avg()`, `min()`, `max()`, and generic `agg()`
- ✅ Null comparison semantics: equality/inequality vs NULL, ordering comparisons vs NULL, and `eqNullSafe`
- ✅ Numeric type coercion for int/double comparisons and simple arithmetic
- ✅ Window functions: `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()`, `first_value`, `last_value`, `percent_rank()` with `.over(partition_by)`
- ✅ Array functions: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`
- ✅ String functions: `upper()`, `lower()`, `substring()`, `concat()`, `concat_ws()`, `length`, `trim`, `regexp_extract`, `regexp_replace`, `regexp_extract_all`, `regexp_like`, `split`
- ✅ Datetime: `year()`, `month()`, `day()`, `to_date()`, `date_format(format)` (chrono strftime)
- ✅ DataFrame methods: `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`
- ✅ **PyO3 bridge** (optional `pyo3` feature): Python module `robin_sparkless` with SparkSession, DataFrame, Column, GroupedData; `create_dataframe`, filter, select, join, group_by, collect (list of dicts), etc. Build: `maturin develop --features pyo3`. Tests: `make test` runs Rust + Python smoke tests. See [PYTHON_API.md](PYTHON_API.md).
- ✅ **Phase 9** (high-value functions & DataFrame methods): Datetime (`current_date`, `current_timestamp`, `date_add`, `date_sub`, `hour`, `minute`, `second`, `datediff`, `last_day`, `trunc`); string (`repeat`, `reverse`, `instr`, `lpad`, `rpad`); math (`sqrt`, `pow`, `exp`, `log`); conditional (`nvl`/`ifnull`, `nullif`, `nanvl`); GroupedData (`first`, `last`, `approx_count_distinct`); DataFrame (`replace`, `cross_join`, `describe`, `cache`/`persist`/`unpersist`, `subtract`, `intersect`).
- ✅ Parity test harness with 159 passing fixtures:
  - `filter_age_gt_30`: filter + select + orderBy
  - `filter_and_or`: nested boolean logic with AND/OR and parentheses
  - `filter_nested`: nested boolean logic
  - `filter_not`: NOT / negation semantics
  - `groupby_count`: groupBy + count + orderBy
  - `groupby_with_nulls`: groupBy with null values + count
  - `groupby_sum`: groupBy + sum aggregation
  - `groupby_avg`: groupBy + avg aggregation
  - `read_csv`: CSV file reading + operations
  - `read_parquet`: Parquet file reading + operations
  - `read_json`: JSON file reading + operations
  - `with_logical_column`: boolean columns / logical expressions in withColumn
  - `with_arithmetic_logical_mix`: mixed arithmetic + logical comparison in withColumn
  - `when_otherwise`: when().then().otherwise() conditional expressions
  - `when_then_otherwise`: chained when expressions
  - `coalesce`: null handling with coalesce
  - `null_comparison_equality`: null equality/inequality semantics
  - `null_comparison_ordering`: ordering comparisons vs NULL
  - `null_safe_equality`: null-safe equality (`eqNullSafe`)
  - `null_in_filter`: null handling in filter predicates
  - `type_coercion_numeric`: int vs double comparison coercion
  - `type_coercion_mixed`: int + double arithmetic coercion
  - `inner_join`, `left_join`, `right_join`, `outer_join`: join operations
  - `groupby_multi_agg`: multiple aggregations in one agg() call
  - `row_number_window`, `rank_window`, `lag_lead_window`: window functions
  - `string_upper_lower`, `string_substring`, `string_concat`, `string_length_trim`: string functions
  - `union_all`, `union_by_name`, `distinct`, `drop_columns`, `dropna`, `fillna`, `limit`, `with_column_renamed`: DataFrame methods
  - `array_contains`, `element_at`, `array_size`: array functions (split + array_contains/element_at/size)
  - `regexp_like`, `regexp_extract_all`: string regex fixtures
  - `string_repeat_reverse`, `string_lpad_rpad`: repeat, reverse, lpad, rpad
  - `math_sqrt_pow`: sqrt, pow
  - `groupby_first_last`: first, last aggregates
  - `cross_join`, `describe`, `replace`, `subtract`, `intersect`: DataFrame methods
  - **Phase 12**: `first_row`, `head_n`, `offset_n`: first/head/offset DataFrame methods
  - **Phase 20**: `groupby_median`, `with_bround`: median, bround; OrderBy with nulls_first
  - **Phase 21**: `with_btrim`, `with_hex`, `with_conv`, `with_str_to_map`, `arrays_overlap`, `arrays_zip`
  - **Phase 22**: `with_dayname`, `with_weekday`, `with_extract`, `with_unix_micros`, `make_timestamp_test`, `timestampadd_test`, `from_utc_timestamp_test`
  - **Phase 23**: `with_isin`, `with_url_decode`, `with_url_encode`, `json_array_length_test`, `with_hash`, `with_shift_left`
- ✅ **Phase 25** (readiness for post-refactor merge): **Plan interpreter** (`execute_plan(session, data, schema, plan)` in Rust; Python `robin_sparkless.execute_plan(data, schema, plan_json)`); **expression interpreter** (serialized expr trees → Polars Expr in `src/plan/expr.rs`); **logical plan schema** ([LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md)); **plan fixtures** (`tests/fixtures/plans/filter_select_limit.json`, `join_simple.json`) with `plan_parity_fixtures` test; **create_dataframe_from_rows** (Rust + Python) for arbitrary schema and list of dicts/rows.

## Next Steps to Full Sparkless Parity

To reach **full Sparkless parity** (robin-sparkless as a complete backend replacement), the remaining work is organized into phases 12–27 below (phases 9–25 complete). **Phases 26–27** are crate publish and Sparkless integration. Reference: [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md), [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md), [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md).

### Phase overview

| Phase | Goal | Est. effort |
|-------|------|-------------|
| **9** | High-value functions + DataFrame methods | ✅ **COMPLETED** |
| **10** | Complex types (Map, JSON, array_repeat, string 6.4) + window fixture simplification | ✅ **COMPLETED** |
| **11** | Parity scale (93 fixtures), harness date/datetime, converter + CI | ✅ **COMPLETED** |
| **12** | DataFrame methods parity (~55+ methods; freq_items, approx_quantile, crosstab, melt, sample_by, no-ops; PyO3 stat/na/to_pandas) | ✅ **COMPLETED** |
| **13** | Functions batch 1: string, binary, collection (~80 new → ~200 total) | ✅ **COMPLETED** |
| **14** | Functions batch 2: math, datetime, type/conditional (~100 new → ~300 total) | ✅ **COMPLETED** |
| **15** | Functions batch 3: remaining functions + fixture growth (88 → 150+ fixtures, 403 functions) | ✅ **COMPLETED** |
| **16** | Remaining gaps 1: string/regex (regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf) | ✅ **COMPLETED** |
| **17** | Remaining gaps 2: datetime/unix (unix_timestamp, from_unixtime, make_date, timestamp_*, pmod, factorial) | ✅ **COMPLETED** |
| **18** | Remaining gaps 3: array/map/struct (array_append, array_prepend, array_insert, array_except/intersect/union, map_concat, map_from_entries, map_contains_key, get, named_struct, struct, map_filter, map_zip_with, zip_with) | ✅ **COMPLETED** |
| **19** | Remaining gaps 4: aggregates and try_* (any_value, bool_and, bool_or, count_if, max_by, min_by, percentile, product, try_add/divide/subtract/multiply/sum/avg, try_element_at, width_bucket, elt, bit_length, typeof) | ✅ **COMPLETED** |
| **20** | Full parity 1: ordering, aggregates, numeric | ✅ **COMPLETED** |
| **21** | Full parity 2: string, binary, type, array/map/struct | ✅ **COMPLETED** |
| **22** | Full parity 3: datetime extensions | ✅ **COMPLETED** |
| **23** | Full parity 4: JSON, CSV, URL, misc | ✅ **COMPLETED** |
| **24** | Full parity 5: bit, control, JVM stubs, random, crypto | ✅ **COMPLETED** |
| **25** | Readiness for post-refactor merge (plan interpreter, expression interpreter, plan schema, plan fixtures, create_dataframe_from_rows) | ✅ **COMPLETED** |
| **26** | Prepare and publish robin-sparkless as a Rust crate (crates.io, API stability, docs, release) | 2–3 weeks |
| **27** | Sparkless integration (BackendFactory "robin", 200+ tests), PyO3 surface | 4–6 weeks |

---

### Phase 9 – High-value functions & DataFrame methods (4–6 weeks) ✅ **COMPLETED**

**Goal**: Complete remaining high-use functions and DataFrame methods so most Sparkless pipelines can run without falling back.

- **Datetime**: `current_date`, `current_timestamp`, `date_add`, `date_sub`, `hour`, `minute`, `second`, `datediff`, `last_day`, `trunc` ✅. (to_date, date_format, year, month, day ✅)
- **String**: `repeat`, `reverse`, `instr`/`locate`, `lpad`/`rpad` ✅. (length, trim, regexp_*, split, initcap ✅)
- **Math**: `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `exp`, `log` ✅; aggregates `first`, `last`, `approx_count_distinct` ✅. (stddev, variance, count_distinct ✅)
- **Conditional/null**: `ifnull`/`nvl`, `nullif`, `nanvl` ✅.
- **DataFrame methods**: `replace`, `crossJoin`, `describe`, `cache`/`persist`/`unpersist`, `subtract`, `intersect` ✅.

**Outcome**: Functions ~37+ → ~85+; DataFrame methods ~25 → ~35+. Parity harness extended with `parse_with_column_expr` for new functions and new Operation variants (Replace, CrossJoin, Describe, Subtract, Intersect). New fixtures: string_repeat_reverse, string_lpad_rpad, math_sqrt_pow, groupby_first_last, cross_join, describe, replace, subtract, intersect.

---

### Phase 10 – Complex types & window parity ✅ **COMPLETED**

**Goal**: Implement Map, JSON, remaining array/string functions, and enable full window parity fixtures.

- **Array**: `array_repeat`, `array_flatten` (Phase 8, Expr::map UDFs); `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean` ✅.
- **Map**: `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays` (Phase 8; Map as List(Struct{key, value})) ✅.
- **JSON**: `get_json_object`, `from_json`, `to_json` ✅; optional `base64`, `unbase64` (deferred).
- **String 6.4**: `mask`, `translate`, `substring_index` (Phase 10); `soundex`, `levenshtein`, `crc32`, `xxhash64` (Phase 8 UDFs) ✅.
- **Window**: percent_rank, cume_dist, ntile, nth_value parity fixtures covered via multi-step workaround; documented in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

**Optional (document only)**: SQL extensions (subqueries, CTEs, HAVING, DDL); Delta schema evolution, MERGE. See [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

**Outcome**: Functions ~85+ → ~120+; Phase 8 and Phase 10 “remaining parity” items implemented.

---

### Phase 11 – Parity scale & test conversion ✅ **COMPLETED**

**Goal**: Grow parity coverage and integrate Sparkless test conversion into CI.

- **Parity harness**: Date/datetime and boolean column support in fixture input ([tests/parity.rs](tests/parity.rs)); `dtype_to_string` and `collect_to_simple_format` for Date/Datetime/Int8; `types_compatible` for date/timestamp.
- **Fixture growth**: 73 → 80 (Phase 11) → 82 (Phase 12–13) → 88 (Phase 14–15) → **93** (Phase 16) fixtures (Phase 16: regexp_count, regexp_substr, regexp_instr, split_part, find_in_set, format_string; array_distinct skipped).
- **Converter**: Date/timestamp type mapping added in [tests/convert_sparkless_fixtures.py](tests/convert_sparkless_fixtures.py).
- **CI**: [.github/workflows/ci.yml](.github/workflows/ci.yml) runs format, clippy, audit, deny, and all tests (including `pyspark_parity_fixtures`); separate job for Python (PyO3) tests.
- **Docs**: [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) documents date/timestamp fixture format; [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) updated with CI note.

**Outcome**: 93 parity fixtures passing; CI runs parity; SPARKLESS_PARITY_STATUS kept current.

---

### Phase 12 – DataFrame methods parity (4–6 weeks) ✅ **COMPLETED**

**Goal**: Implement the remaining ~50–60 DataFrame methods so robin-sparkless reaches the 85-method target before integration.

- **Implemented**: `sample`, `random_split`, `first`, `head`, `take`, `tail`, `is_empty`, `to_df`, `stat()` (cov, corr), `summary` (alias describe), `to_json`, `explain`, `print_schema`, `checkpoint`, `local_checkpoint`, `repartition`, `coalesce`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `na()` (fill, drop), `to_pandas`, `offset`, `transform`, `freq_items`, `approx_quantile`, `crosstab`, `melt`, `except_all`, `intersect_all`, `sample_by` (stratified sampling); Spark no-ops: `hint`, `is_local`, `input_files`, `same_semantics`, `semantic_hash`, `observe`, `with_watermark`.
- **Parity**: Fixtures `first_row.json`, `head_n.json`, `offset_n.json` for first/head/offset.
- **PyO3**: All of the above exposed on Python DataFrame where applicable; `random_split`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat()` (returns `DataFrameStat` with cov/corr), `na()` (returns `DataFrameNa` with fill/drop), `to_pandas` (same as collect; for use with `pandas.DataFrame.from_records`).
- **Outcome**: DataFrame methods ~35 → ~55+; ready for function batches (Phase 13).

---

### Phase 13 – Functions batch 1: string, binary, collection (4–6 weeks) ✅ **COMPLETED**

**Goal**: Add ~80 functions in string, binary, and collection categories toward ~200 total functions.

- **String** ✅ (partial): `ascii`, `format_number`, `overlay`, `position`, `char`, `chr` implemented; `base64`, `unbase64` (base64 crate). Remaining: `format_string`, `encode`/`decode`, etc.
- **Binary** ✅ (partial): `sha1`, `sha2(bit_length)`, `md5` (string in → hex string out; sha1, sha2, md5 crates). AES_* deferred.
- **Collection** ✅ (partial): `array_compact` (drop nulls from list). Remaining: array_distinct, map extensions, etc.
- **Parity**: Parser branches for all new functions; fixtures `string_ascii`, `string_format_number` (82 fixtures at Phase 13 completion; 88 after Phase 15).
- **PyO3**: Module and Column methods for ascii, format_number, overlay, position, char, chr, base64, unbase64, sha1, sha2, md5, array_compact.
- **Outcome**: Functions ~120 → ~130+; 82 parity fixtures at Phase 13. Phase 14 (84 fixtures), Phase 15 (88 fixtures, ~175+ functions).

---

### Phase 14 – Functions batch 2: math, datetime, type/conditional ✅ **COMPLETED**

**Goal**: Add ~100 functions in math, datetime, type casting, and conditional logic toward ~300 total.

- **Math** ✅: sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum (UDFs).
- **Datetime** ✅: quarter, weekofyear, dayofweek, dayofyear, add_months, months_between, next_day (Polars dt + chrono UDFs).
- **Type/conditional** ✅: cast, try_cast (PySpark-like type names), isnan, greatest, least.
- **Parity** ✅: Parser branches for all; fixtures `math_sin_cos`, `datetime_quarter_week` (84 fixtures).
- **PyO3** ✅: Module and Column methods for all Phase 14 functions. Docs updated.

---

### Phase 15 – Functions batch 3: remaining + fixture growth (6–8 weeks) ✅ **COMPLETED**

**Goal**: Reach 403-function parity and grow parity fixtures from 88 to 150+.

- **Functions**: Batch 1 aliases (nvl, nvl2, substr, power, ln, ceiling, lcase, ucase, dayofmonth, to_degrees, to_radians, isnull, isnotnull), Batch 2 string (left, right, replace, startswith, endswith, contains, like, ilike, rlike), Batch 3 math (cosh, sinh, tanh, acosh, asinh, atanh, cbrt, expm1, log1p, log10, log2, rint, hypot), Batch 4 array_distinct — all implemented. See [PHASE15_GAP_LIST.md](PHASE15_GAP_LIST.md), [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md).
- **Fixtures**: 82 → **88** hand-written; target 150+ with `convert_sparkless_fixtures.py` when Sparkless expected_outputs available.
- **Outcome**: Phase 15 scope done; remaining gaps covered in Phases 16–19.

---

### Phase 16 – Remaining gaps 1: string and regex (2–3 weeks) ✅ **COMPLETED**

**Goal**: Implement remaining string/regex functions from [PHASE15_GAP_LIST.md](PHASE15_GAP_LIST.md) and [GAP_ANALYSIS_SPARKLESS_3.28.md](GAP_ANALYSIS_SPARKLESS_3.28.md).

- **String/regex**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf` — all implemented. (`btrim`, `locate`, `conv` → Phase 21.)
- **Parity**: Parser branches and fixtures (`regexp_count`, `regexp_substr`, `regexp_instr`, `split_part`, `find_in_set`, `format_string`).
- **PyO3**: Exposed on Column and module.
- **Outcome**: String/regex gap closed; ready for Phase 17.

---

### Phase 17 – Remaining gaps 2: datetime and unix (2–3 weeks) ✅ **COMPLETED**

**Goal**: Implement datetime/unix and remaining math from the gap list.

- **Datetime/unix**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date` — all implemented; optional `convert_timezone`, `current_timezone`, `now`, `curdate`, `localtimestamp` deferred.
- **Math**: `pmod`, `factorial` — implemented.
- **Parity**: Parser branches and 10 fixtures.
- **PyO3**: Exposed on Column and module.
- **Outcome**: Datetime/unix gap closed; ready for Phase 18.

---

### Phase 18 – Remaining gaps 3: array, map, struct ✅ **COMPLETED**

**Goal**: Implement remaining array/map/struct functions.

- **Array**: `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union`, **`zip_with`** (UDF + list.eval). (`arrays_overlap`, `arrays_zip`, `explode_outer`, `posexplode_outer`, `array_agg` → Phase 21.)
- **Map**: `map_concat`, **`map_filter`**, **`map_zip_with`** (list.eval / UDF + list.eval), `map_from_entries`, `map_contains_key`, `get` (map element). (`str_to_map` → Phase 21.)
- **Struct**: `named_struct`, `struct`. (`transform_keys`, `transform_values` → Phase 21.)
- **Parity**: Parser for struct-field expressions; fixtures `map_filter`, `zip_with`, `map_zip_with` (121 → 124).
- **PyO3**: `map_filter_value_gt`, `zip_with_coalesce`, `map_zip_with_coalesce` convenience helpers.
- **Outcome**: Array/map/struct gap closed including deferred functions; ready for Phase 19.

---

### Phase 19 – Remaining gaps 4: aggregates and try_* ✅ **COMPLETED**

**Goal**: Implement remaining aggregates and try_* / misc functions.

- **Aggregates**: `any_value`, `bool_and`, `bool_or`, `every`/`some`, `count_if`, `max_by`, `min_by`, `percentile`, `product`, `collect_list`, `collect_set` — all implemented. `percentile_approx` deferred.
- **Try_***: `try_divide`, `try_add`, `try_subtract`, `try_multiply`, `try_element_at` — all implemented.
- **Misc**: `width_bucket`, `elt`, `bit_length`, `typeof`.
- **Parity**: `groupby_any_value`, `groupby_product`, `try_divide`, `width_bucket` (124 → 128).
- **PyO3**: GroupedData methods; try_*, width_bucket, elt, bit_length, typeof.
- **Outcome**: Aggregates and try_* gap closed; ready for Phase 20 (full parity part 1).

---

### Phase 20 – Full parity 1: ordering, aggregates, numeric (1.5–2 weeks) ✅ **COMPLETED**

**Goal**: High-value ordering and aggregate functions. Reference: [PARITY_CHECK_SPARKLESS_3.28.md](PARITY_CHECK_SPARKLESS_3.28.md).

- **Ordering** ✅: `asc`, `asc_nulls_first`, `asc_nulls_last`, `desc`, `desc_nulls_first`, `desc_nulls_last` (return SortOrder for use in orderBy); `DataFrame::order_by_exprs(sort_orders)`.
- **Aggregates** ✅: `median`, `mode`; `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`; `try_sum`, `try_avg`. Deferred: `covar_pop`, `covar_samp`, `corr` (as groupBy agg), `kurtosis`, `skewness`, `percentile_approx`.
- **Numeric** ✅: `bround`; `negate`, `negative`, `positive`; `cot`, `csc`, `sec`; `e`, `pi` (constants).

**Parity**: Fixtures `groupby_median`, `with_bround`; OrderBy supports optional `nulls_first`. **Outcome**: ~25 new functions; ready for Phase 21.

---

### Phase 21 – Full parity 2: string, binary, type, array/map/struct (2 weeks) ✅ **COMPLETED**

**Goal**: String/binary/type and collection extensions.

- **String**: `btrim`; `locate`; `conv` (base conversion). ✅
- **Binary**: `hex`, `unhex`; `bin`; `getbit`. Defer: `decode`, `encode`; `to_binary`, `try_to_binary`. ✅
- **Type/cast**: `to_char`, `to_number`, `to_varchar`; `try_to_number`, `try_to_timestamp`. ✅
- **Array**: `array_agg`; `arrays_overlap`, `arrays_zip`; `explode_outer`, `posexplode_outer`. Defer: `aggregate`. ✅
- **Map**: `str_to_map`. ✅
- **Struct**: `transform_keys`, `transform_values`. ✅

**Parity**: Fixtures `with_btrim`, `with_hex`, `with_conv`, `with_str_to_map`, `arrays_overlap`, `arrays_zip`. **Outcome**: ~20 new functions; ready for Phase 22.

---

### Phase 22 – Full parity 3: datetime extensions ✅ **COMPLETED**

**Goal**: Complete datetime function set.

- `convert_timezone`, `current_timezone`; `curdate`; `date_diff`, `date_part`; `dateadd`, `datepart`; `dayname`, `weekday`; `days`, `hours`, `months`, `years`; `extract`; `localtimestamp`; `now`.
- `make_timestamp`, `make_timestamp_ntz`, `make_interval`; `timestampadd`, `timestampdiff`; `to_timestamp`; `from_utc_timestamp`, `to_utc_timestamp`; `unix_micros`, `unix_millis`, `unix_seconds`.

**Parity**: Fixtures `with_dayname`, `with_weekday`, `with_extract`, `with_unix_micros`, `make_timestamp_test`, `timestampadd_test`, `from_utc_timestamp_test`; `with_curdate_now` skipped (non-deterministic). **Outcome**: ~25 new functions; ready for Phase 23.

---

### Phase 23 – Full parity 4: JSON, CSV, URL, misc ✅ **COMPLETED**

**Goal**: JSON/CSV/URL and misc helpers.

- **JSON**: `json_array_length`, `parse_url`. Defer: `json_object_keys`, `json_tuple`.
- **Schema/I/O**: Defer `from_csv`, `to_csv`, `schema_of_csv`, `schema_of_json`.
- **URL**: `url_decode`, `url_encode`.
- **Misc**: `isin`, `isin_i64`, `isin_str`; `equal_null`; `hash`; `shiftLeft`, `shiftRight`, `shiftRightUnsigned`; `stack`; `version`. Defer: `inline`, `inline_outer`, `sentences`, `sequence`, `shuffle`, `call_function`.

**Parity**: Fixtures `with_isin`, `with_url_decode`, `with_url_encode`, `json_array_length_test`, `with_hash`, `with_shift_left`. **Outcome**: ~18 new functions; ready for Phase 24.

---

### Phase 24 – Full parity 5: bit, control, JVM stubs, random, crypto (1.5–2 weeks) ✅ **COMPLETED**

**Goal**: Bit operations, control flow, JVM compatibility stubs, random, crypto.

- **Bit** ✅: `bit_and`, `bit_or`, `bit_xor`, `bit_count`, `bit_get`; `bitwiseNOT`, `bitwise_not`. (`bitmap_*` deferred if in Sparkless 3.28.)
- **Control** ✅: `assert_true`, `raise_error`.
- **JVM stubs** ✅: `broadcast`, `spark_partition_id`, `input_file_name`, `monotonically_increasing_id`, `current_catalog`, `current_database`, `current_schema`, `current_user`, `user` — no-ops or placeholders for API compatibility.
- **Random** ✅: `rand(seed)`, `randn(seed)` — real RNG with optional seed; one value per row when used in `with_column`/`with_columns` (PySpark-like). `udf`, `pandas_udf` — stub or minimal support.
- **Crypto** ✅: `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt` (AES-128-GCM). See [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

**Defer**: `regr_*` (regression), XML (`from_xml`, `to_xml`, `xpath_*`), ML/HLL — document as out of scope.

**Parity**: Fixture `with_bit_ops`; `with_rand_seed`, `with_jvm_stubs`. **PyO3**: All new functions exposed. **Outcome**: Full parity 5 done; ready for Phase 25 (readiness for post-refactor merge).

---

### Phase 25 – Readiness for post-refactor merge (3–4 weeks) ✅ **COMPLETED**

**Goal**: Prepare robin-sparkless so that when Sparkless completes its [refactor plan](SPARKLESS_REFACTOR_PLAN.md) (serializable logical plan and `materialize_from_plan`), integration is a thin adapter. See [READINESS_FOR_SPARKLESS_PLAN.md](READINESS_FOR_SPARKLESS_PLAN.md) for full detail.

- **Plan interpreter** ✅: `execute_plan(session, data, schema, plan)` in Rust (`src/plan/`); exposed in Python as `robin_sparkless.execute_plan(data, schema, plan_json)` returning a DataFrame (call `.collect()` for list of dicts).
- **Logical plan schema** ✅: [docs/LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md) defines op names, payload shapes, and expression tree format.
- **Expression interpreter** ✅: `src/plan/expr.rs` turns serialized expression trees into Polars `Expr` (col, lit, comparisons, and, or, not, upper, lower, coalesce, eq_null_safe).
- **Plan-based fixtures and tests** ✅: `tests/fixtures/plans/` (filter_select_limit, join_simple); `plan_parity_fixtures` test in `tests/parity.rs`.
- **Flexible DataFrame creation** ✅: Rust `create_dataframe_from_rows(rows, schema)` in session; Python `create_dataframe_from_rows(data, schema)` on SparkSession.

**Outcome**: When Sparkless emits a logical plan, we can execute it via `execute_plan`; Sparkless robin backend becomes a thin wrapper. Ready for Phase 26 (crate publish).

---

### Phase 26 – Prepare and publish robin-sparkless as a Rust crate (2–3 weeks)

**Goal**: Make the library ready for public use as a Rust dependency and (optionally) a Python wheel before Sparkless integration.

- **Crate metadata**: Finalize `Cargo.toml` (description, license, repository, keywords, categories); ensure semver and version are release-ready.
- **API surface**: Review public API for stability; document breaking-change policy (e.g. semver for 0.x); consider `#[deprecated]` or feature flags for experimental APIs.
- **Documentation**: `cargo doc` builds cleanly; add or expand crate-level and module docs; link from README to docs.rs (or hosted docs).
- **Release workflow**: Tag releases; publish to [crates.io](https://crates.io) (`cargo publish`); optionally publish Python wheels to PyPI via maturin (e.g. `maturin publish --features pyo3`).
- **CI**: Ensure CI runs full check (format, clippy, audit, deny, tests, benchmarks) on release branches/tags; document how to cut a release in CONTRIBUTING or README.

**Outcome**: `robin-sparkless` is published on crates.io; consumers can add it as a dependency; optional PyPI wheel for Python users; clear release and versioning process.

---

### Phase 27 – Sparkless integration & PyO3 surface (4–6 weeks)

**Goal**: Make robin-sparkless a runnable backend for Sparkless and keep the Python API in sync.

- **Sparkless repo**: Add "robin" backend option to BackendFactory; when selected, delegate DataFrame execution to robin-sparkless via PyO3 (using the published crate or wheel from Phase 26). After Sparkless refactor, wire `materialize_from_plan` to our `execute_plan`.
- **Fallback**: When an operation is not supported, raise a clear error or fall back to Python Polars; document behavior.
- **Target**: 200+ Sparkless tests passing with robin backend (current: 0).
- **PyO3**: Expose new Rust functions (Phases 20–25) on Python `Column` and module-level API; keep [PYTHON_API.md](PYTHON_API.md) updated.

**Outcome**: Sparkless can run against robin-sparkless; 200+ tests passing; Python API matches full function set.

---

### Ongoing / optional (lower priority)

- **Trait-based backend**: `QueryExecutor`, `DataMaterializer` for pluggability (FULL_BACKEND_ROADMAP Phase 1 Future).
- **Expression model doc**: Document Column/Expr mapping to Sparkless `ColumnOperation` trees.
- **UDFs**: Pure-Rust UDFs only; Python UDFs out of scope.
- **Memory / scale**: Optional memory profiling and large-dataset handling.

---

### Summary metrics (full parity targets)

| Metric | Current | After Phase 22 | After Phase 24 (full parity) | After Phase 25 (readiness) | After Phase 26 (crate) | Full Backend (Phase 27) |
|--------|---------|----------------|------------------------------|----------------------------|------------------------|-------------------------|
| Parity fixtures | 159 | 159 | 180+ | 180+ | 180+ | 180+ |
| Functions | ~283 | ~283 | ~280 | ~280 | ~280 | ~280 |
| DataFrame methods | ~55+ | ~55+ | ~55+ | ~55+ | ~55+ | 85 |
| Plan interpreter / execute_plan | Yes | — | — | Yes | Yes | Yes |
| Crate on crates.io | No | — | — | — | Yes | Yes |
| Sparkless tests passing (robin backend) | 0 | — | — | — | — | 200+ |

## Testing Strategy

To enforce the roadmap above, we will:

- **Use PySpark as the oracle**
  - ✅ Maintain a small Python tool (`tests/gen_pyspark_cases.py`) that:
    - ✅ Runs pipelines in PySpark.
    - ✅ Emits JSON fixtures describing inputs, operations, and expected outputs.
  - ✅ Fixtures live under `tests/fixtures/` and are versioned with the repo.
  - **Sparkless test conversion**: Build a fixture converter to reuse Sparkless's 270+ expected_outputs; see [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) §4.

- **Drive Rust tests from fixtures**
  - ✅ `tests/parity.rs`:
    - ✅ Reconstructs a `DataFrame` from each fixture's `input`.
    - ✅ Applies the listed operations (`filter`, `select`, `groupBy+agg`, `orderBy`, etc.) via the Rust API.
    - ✅ Collects results and compares schema + rows against `expected`, with well-defined tolerances (e.g. for floats, or order-insensitive comparisons where PySpark doesn't guarantee ordering).
  - ✅ **Plan fixtures** (Phase 25): `plan_parity_fixtures` loads `tests/fixtures/plans/*.json`, runs `plan::execute_plan`, and asserts schema and rows match expected. Fixtures use the serialized plan format in [LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md).

- **Track parity coverage**
  - ✅ Initial parity test infrastructure in place
  - ✅ Maintain a parity matrix (operations × data types × edge cases) in `PARITY_STATUS.md` (see `PARITY_STATUS.md`)
    - Each cell indicates whether it's covered (and by which fixture), not yet covered, or intentionally diverges.

This testing strategy makes PySpark behavior the reference and gives us a clear, automated way to detect regressions as the Rust API and Polars versions evolve.
