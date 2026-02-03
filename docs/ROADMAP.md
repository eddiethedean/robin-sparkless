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

### Medium-Term Objectives (1–3 months) 🚧 **IN PROGRESS**

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
- **Phase 5 – Test Conversion**: Converter extended (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed); parity discovers `tests/fixtures/` + `tests/fixtures/converted/`; `make sparkless-parity` (set SPARKLESS_EXPECTED_OUTPUTS); [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) for pass/fail; **73 passing** (50+ target met) ✅ **COMPLETED**
- **Phase 6 – Broad Parity**: Array (6a ✅; array_position, array_remove, posexplode via list.eval; array_repeat, array_flatten ✅ Phase 8), Map (6b ✅ Phase 8), JSON (6c ✅), additional string (6e ✅; 6.4 soundex/levenshtein/crc32/xxhash64 ✅ Phase 8), window extensions (6d ✅; percent_rank/cume_dist/ntile/nth_value covered).
- **Phase 7 – SQL & Advanced** ✅ **COMPLETED**: Optional **SQL** (`sql` feature: `spark.sql()`, temp views); optional **Delta** (`delta` feature: `read_delta`, `read_delta_with_version`, `write_delta`); benchmarks and error-message improvements. See [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md) §7.
- **Phase 8 – Remaining Parity** ✅ **COMPLETED** (Feb 2026): array_repeat, array_flatten; Map (create_map, map_keys, map_values, map_entries, map_from_arrays); String 6.4 (soundex, levenshtein, crc32, xxhash64); window fixtures covered; documentation of differences.

## Success Metrics

We know we're on track if:

- ✅ **Behavioral parity**: For core operations (filter, select, orderBy, groupBy+count/sum/avg/min/max/agg, when/coalesce, basic type coercion, null semantics, joins, window functions, array and string functions), DataFrame methods (union, distinct, drop, dropna, fillna, limit, withColumnRenamed), and file readers (CSV/Parquet/JSON), PySpark and Robin Sparkless produce the same schema and data on test fixtures. **Status: PASSING (73 fixtures)**
- ✅ **Documentation of differences**: Any divergence from PySpark semantics is called out in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) (window, SQL, Delta, Phase 8).
- ✅ **Performance envelope**: For supported operations, we stay within ~2x of doing the same thing directly in Polars. **Status: BENCHMARKED** (`cargo bench`; see [QUICKSTART.md](QUICKSTART.md) § Benchmarks)

**Full backend targets** (see [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md)):

| Metric | Current | Phase 5 | Full Backend |
|--------|---------|---------|--------------|
| Parity fixtures | 73 | 80+ | 150+ |
| Functions | ~120+ | ~120 | 250+ |
| DataFrame methods | ~25 | ~40 | 60+ |
| Sparkless tests passing (robin backend) | 0 | 50+ | 200+ |
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
- ✅ Parity test harness with 73 passing fixtures:
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

## Next Steps to Full Sparkless Parity

To reach **full Sparkless parity** (robin-sparkless as a complete backend replacement), the remaining work is organized into phases 9–12 below. Reference: [FULL_BACKEND_ROADMAP.md](FULL_BACKEND_ROADMAP.md), [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md).

### Phase overview

| Phase | Goal | Est. effort |
|-------|------|-------------|
| **9** | High-value functions + DataFrame methods | 4–6 weeks |
| **10** | Complex types (Map, JSON, array_repeat, string 6.4) + window fixture simplification | ✅ **COMPLETED** |
| **11** | Parity scale (80+ → 150+ fixtures), harness date/datetime, converter + CI | 2–4 weeks |
| **12** | Sparkless integration (BackendFactory "robin", 200+ tests), PyO3 surface | 4–6 weeks |

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

### Phase 11 – Parity scale & test conversion (2–4 weeks)

**Goal**: Grow parity coverage and integrate Sparkless test conversion into CI.

- **Parity harness**: Add date/datetime column support in fixture input so datetime fixtures can be added; add hand-written fixtures for Phase 9/10 functions.
- **Fixture growth**: 73 → 80+ (intermediate) → 150+ (full backend target).
- **Sparkless converter**: When Sparkless repo is available, run `make sparkless-parity` with `SPARKLESS_EXPECTED_OUTPUTS` set; update [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) with converted/passing/failing counts and failure reasons.
- **CI**: Run converted fixtures in CI; classify failures (unsupported vs semantic); add `skip: true` + `skip_reason` where appropriate.

**Outcome**: 80+ parity fixtures; converter and parity run in CI; SPARKLESS_PARITY_STATUS kept current.

---

### Phase 12 – Sparkless integration & PyO3 surface (4–6 weeks)

**Goal**: Make robin-sparkless a runnable backend for Sparkless and keep the Python API in sync.

- **Sparkless repo**: Add "robin" backend option to BackendFactory; when selected, delegate DataFrame execution to robin-sparkless via PyO3.
- **Fallback**: When an operation is not supported, raise a clear error or fall back to Python Polars; document behavior.
- **Target**: 200+ Sparkless tests passing with robin backend (current: 0).
- **PyO3**: Expose new Rust functions (Phase 9/10) on Python `Column` and module-level API; keep [PYTHON_API.md](PYTHON_API.md) updated.

**Outcome**: Sparkless can run against robin-sparkless; 200+ tests passing; Python API matches new functions.

---

### Ongoing / optional (lower priority)

- **Trait-based backend**: `QueryExecutor`, `DataMaterializer` for pluggability (FULL_BACKEND_ROADMAP Phase 1 Future).
- **Expression model doc**: Document Column/Expr mapping to Sparkless `ColumnOperation` trees.
- **UDFs**: Pure-Rust UDFs only; Python UDFs out of scope.
- **Memory / scale**: Optional memory profiling and large-dataset handling.

---

### Summary metrics (full parity targets)

| Metric | Current | After Phase 11 | Full Backend target |
|--------|---------|----------------|---------------------|
| Parity fixtures | 73 | 80+ | 150+ |
| Functions | ~120+ | ~120 | 250+ |
| DataFrame methods | ~25 | ~40 | 60+ |
| Sparkless tests passing (robin backend) | 0 | — | 200+ |

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

- **Track parity coverage**
  - ✅ Initial parity test infrastructure in place
  - ✅ Maintain a parity matrix (operations × data types × edge cases) in `PARITY_STATUS.md` (see `PARITY_STATUS.md`)
    - Each cell indicates whether it's covered (and by which fixture), not yet covered, or intentionally diverges.

This testing strategy makes PySpark behavior the reference and gives us a clear, automated way to detect regressions as the Rust API and Polars versions evolve.
