# Full Sparkless Backend Roadmap

This document plans the path for **robin-sparkless** to become a complete backend replacement for [Sparkless](https://github.com/eddiethedean/sparkless). Sparkless implements 403+ PySpark functions and 85+ DataFrame methods; robin-sparkless currently covers a core subset with 36 parity fixtures.

**Reference**: [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) catalogs all functions/methods; [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) describes architecture mapping.

---

## Current State (February 2026)

| Area | Robin-Sparkless | Sparkless | Gap |
|------|-----------------|-----------|-----|
| **Functions** | ~25 (col, lit, count, sum, avg, min, max, when, coalesce, upper, lower, substring, concat, concat_ws, row_number, rank, dense_rank, lag, lead) | 403 | ~378 |
| **DataFrame methods** | ~15 (filter, select, orderBy, groupBy, withColumn, join, collect, count, show, read_csv, read_parquet, read_json) | 85 | ~70 |
| **Parity fixtures** | 36 passing | 270+ expected_outputs | 234+ |
| **SQL** | Not implemented | Full DDL/DML support | Full |

---

## Phase Overview

| Phase | Goal | Est. Effort |
|-------|------|--------------|
| **1. Foundation** | Structural alignment, case sensitivity, fixture converter | 2–3 weeks |
| **2. High-Value Functions** | Top 60 functions used by Sparkless parity tests | 4–6 weeks |
| **3. DataFrame Methods** | Core methods: union, distinct, drop, fillna, etc. | 3–4 weeks |
| **4. PyO3 Bridge** | Python bindings so Sparkless can call robin-sparkless | 4–6 weeks |
| **5. Test Conversion** | Convert 50+ Sparkless tests, run in CI | 2–3 weeks |
| **6. Broad Function Parity** | Remaining ~300 functions, prioritized by usage | 8–12 weeks |
| **7. SQL & Advanced** | SQL executor, Delta Lake, UDFs (deferred or limited) | Ongoing |

---

## Phase 1: Foundation (2–3 weeks)

**Goal**: Align structure with Sparkless so subsequent work maps cleanly.

### 1.1 Structural Alignment

- [ ] Split `dataframe.rs` into submodules:
  - `src/dataframe/transformations.rs` (filter, select, withColumn)
  - `src/dataframe/aggregations.rs` (groupBy, agg)
  - `src/dataframe/joins.rs` (join logic)
- [ ] Introduce trait-based backend abstraction:
  - `trait QueryExecutor` for future pluggability
  - `trait DataMaterializer` for lazy/materialization
- [ ] Document expression model (Column/Expr) and ensure it can represent Sparkless `ColumnOperation` trees

### 1.2 Case Sensitivity

- [ ] Add `spark.sql.caseSensitive` configuration (default: false)
- [ ] Centralized column resolution for filter, select, withColumn, join
- [ ] Fixture for case-insensitive column matching

### 1.3 Fixture Converter

- [ ] Script: `tests/convert_sparkless_fixtures.py` (or Rust tool)
- [ ] Map Sparkless JSON (`input_data`, `expected_output`) → robin-sparkless (`input`, `operations`, `expected`)
- [ ] Handle operation mapping: filter_operations, groupby, join, etc.
- [ ] Target: Convert 10–20 high-value Sparkless parity tests

---

## Phase 2: High-Value Functions (4–6 weeks)

**Goal**: Implement functions that appear most often in Sparkless parity tests and expected_outputs.

### 2.1 String (extend beyond current)

| Function | Polars API | Priority |
|----------|------------|----------|
| `length` | `col.str().len_chars()` | High |
| `trim` / `ltrim` / `rtrim` | `str().strip_chars()`, `strip_chars_start`, `strip_chars_end` | High |
| `regexp_extract` | `str().extract()` | High |
| `regexp_replace` | `str().replace()` | High |
| `split` | `str().split()` | High |
| `initcap` | `str().to_titlecase()` | Medium |
| `repeat` | `str().repeat_by()` | Medium |
| `reverse` | `str().reverse()` | Medium |
| `instr` / `locate` | `str().find()` | Medium |
| `lpad` / `rpad` | `str().pad_start` / `pad_end` | Medium |

### 2.2 Datetime

| Function | Polars API | Priority |
|----------|------------|----------|
| `current_date` | `lit(Date::today())` | High |
| `current_timestamp` | `lit(Utc::now())` | High |
| `to_date` | `col.cast(Date)` | High |
| `date_add` | `col + Duration::days(n)` | High |
| `date_sub` | `col - Duration::days(n)` | High |
| `date_format` | `col.dt().strftime()` | High |
| `year`, `month`, `day`, `hour`, `minute`, `second` | `col.dt().year()`, etc. | High |
| `datediff` | `(col1 - col2).dt().total_days()` | Medium |
| `last_day` | `col.dt().last_day_of_month()` | Medium |
| `trunc` | `col.dt().truncate()` | Medium |

### 2.3 Math & Aggregates

| Function | Polars API | Priority |
|----------|------------|----------|
| `abs`, `ceil`, `floor`, `round` | `col.abs()`, `ceil()`, `floor()`, `round()` | High |
| `sqrt`, `pow`, `exp`, `log` | `col.sqrt()`, `pow()`, `exp()`, `log()` | High |
| `stddev` / `stddev_samp` | `col.std()` | High |
| `variance` / `var_samp` | `col.var()` | High |
| `count_distinct` | `col.n_unique()` | High |
| `first`, `last` | `col.first()`, `last()` | High |
| `approx_count_distinct` | `col.n_unique()` (or HLL if available) | Medium |

### 2.4 Conditional & Null

| Function | Status | Notes |
|----------|--------|-------|
| `when`, `coalesce` | ✅ Done | — |
| `ifnull` / `nvl` | To add | Alias for coalesce |
| `nullif` | To add | Returns null if equal |
| `nanvl` | To add | Replace NaN with value |

---

## Phase 3: DataFrame Methods (3–4 weeks)

**Goal**: Implement methods needed for Sparkless DataFrame pipelines.

| Method | Polars API | Priority |
|--------|------------|----------|
| `union` / `unionAll` | `LazyFrame.vstack()` | High |
| `unionByName` | `concat` with schema alignment | High |
| `distinct` / `dropDuplicates` | `LazyFrame.unique()` | High |
| `drop` | `LazyFrame.drop()` | High |
| `dropna` | `LazyFrame.drop_nulls()` | High |
| `fillna` | `LazyFrame.fill_null()` | High |
| `limit` | `LazyFrame.fetch()` / `slice()` | High |
| `withColumnRenamed` | `LazyFrame.rename()` | High |
| `replace` | `LazyFrame.replace()` | Medium |
| `crossJoin` | `LazyFrame.join(..., how=Cross)` | Medium |
| `describe` | `LazyFrame.describe()` | Medium |
| `cache` / `persist` | Materialize and store; `unpersist` | Medium |
| `subtract` / `except` | Anti-join or set diff | Medium |
| `intersect` / `intersectAll` | Set operations | Low |

---

## Phase 4: PyO3 Bridge (4–6 weeks)

**Goal**: Enable Sparkless (Python) to call robin-sparkless (Rust) for execution.

### 4.1 Crate Layout

- [ ] Create `robin-sparkless-pyo3` (or `robin_sparkless` with `pyo3` feature)
- [ ] Expose `SparkSession`, `DataFrame`, `Column` as Python classes
- [ ] Map Polars → PyArrow for data transfer (Sparkless uses Polars Python; may need Arrow intermediate)

### 4.2 API Surface

- [ ] `SparkSession::create_dataframe(py, data, schema)` → Python
- [ ] `DataFrame::filter`, `select`, `with_column`, `join`, `group_by`, etc.
- [ ] `Column` and functions: `col`, `lit`, aggregates, string, datetime
- [ ] Actions: `collect` → list of dicts or Row objects; `count`, `show`

### 4.3 Sparkless Integration

- [ ] Sparkless `BackendFactory` adds "robin" backend option
- [ ] When "robin" selected, Sparkless delegates to robin-sparkless via PyO3
- [ ] Fallback: if robin-sparkless doesn't support an op, raise or fall back to Python Polars

### 4.4 Risks

- **Schema round-trip**: PySpark/Sparkless types ↔ Polars ↔ Arrow; ensure nullability and types align
- **Performance**: PyO3 overhead vs. native Python Polars; benchmark
- **Error handling**: Rust errors → Python exceptions with useful messages

---

## Phase 5: Test Conversion (2–3 weeks)

**Goal**: Run Sparkless parity tests against robin-sparkless backend.

- [ ] Fixture converter produces robin-sparkless fixtures from Sparkless expected_outputs
- [ ] Identify tests that use only supported ops; run those first
- [ ] CI: `make sparkless-parity` runs converted tests
- [ ] Target: 50+ tests passing on robin-sparkless
- [ ] Document which tests fail and why (missing function, semantic difference)

---

## Phase 6: Broad Function Parity (8–12 weeks)

**Goal**: Implement remaining high-usage functions from PYSPARK_FUNCTION_MATRIX.

### 6.1 Array Functions

- `array`, `array_contains`, `array_join`, `array_max`, `array_min`
- `array_position`, `array_remove`, `array_repeat`, `array_size`
- `array_sort`, `element_at`, `explode`, `posexplode`
- `slice`, `size`, `flatten`, `exists`, `forall`, `filter`, `transform`, `aggregate`

### 6.2 Map Functions

- `create_map`, `map_keys`, `map_values`, `map_entries`
- `map_from_arrays`, `map_from_entries`, `map_concat`
- `map_filter`, `map_zip_with`, `transform_keys`, `transform_values`

### 6.3 JSON & Binary

- `get_json_object`, `json_tuple`, `from_json`, `to_json`
- `base64`, `unbase64`, `hex`, `unhex`, `decode`, `encode`

### 6.4 Additional String

- `regexp_extract_all`, `regexp_replace`, `regexp_like`
- `soundex`, `levenshtein`, `crc32`, `xxhash64`
- `mask`, `translate`, `substring_index`

### 6.5 Window Extensions

- `percent_rank`, `cume_dist`, `ntile`
- `first_value`, `last_value`, `nth_value`

### 6.6 Deferred / Out of Scope

- **UDFs**: Python UDFs require Python runtime; document as out of scope; consider pure-Rust UDFs
- **SQL**: Full SQL parser/executor is large; consider sqlparser + translation to DataFrame ops
- **Delta Lake**: Sparkless has Delta support; could be Phase 7
- **XML**: `xpath_*`; low priority
- **Specialized**: `histogram_numeric`, `hll_*`, `count_min_sketch`; defer

---

## Phase 7: SQL & Advanced (Ongoing)

### 7.1 SQL Support (Optional)

- [ ] `SparkSession::sql(query)` → parse SQL, translate to DataFrame ops
- [ ] Use `sqlparser` or similar for parsing
- [ ] Support: SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
- [ ] Temporary views: `createOrReplaceTempView`, `table()`

### 7.2 Delta Lake (Optional)

- [ ] Read/write Delta tables
- [ ] Time travel, schema evolution
- [ ] MERGE operations

### 7.3 Performance & Robustness

- [ ] Benchmarks: robin-sparkless vs. plain Polars vs. PySpark
- [ ] Ensure within 2x of Polars for supported ops
- [ ] Memory profiling, large-dataset handling
- [ ] Error messages that help users fix issues

---

## Success Metrics

| Metric | Current | Phase 2 | Phase 5 | Full Backend |
|--------|---------|---------|---------|--------------|
| Parity fixtures | 36 | 60+ | 80+ | 150+ |
| Functions implemented | ~25 | ~85 | ~120 | 250+ |
| DataFrame methods | ~15 | ~25 | ~40 | 60+ |
| Sparkless tests passing (robin backend) | 0 | — | 50+ | 200+ |
| PyO3 bridge | No | No | Yes | Yes |

---

## Implementation Order (Summary)

1. **Phase 1**: Fixture converter, case sensitivity, structural split
2. **Phase 2**: String (length, trim, regexp_*), datetime (to_date, date_add, etc.), math (stddev, variance)
3. **Phase 3**: union, distinct, drop, fillna, limit
4. **Phase 4**: PyO3 bridge (can start in parallel with Phase 2/3)
5. **Phase 5**: Convert Sparkless tests, CI integration
6. **Phase 6**: Array, Map, JSON, remaining string/window
7. **Phase 7**: SQL, Delta, performance (as needed)

---

## Related Docs

- [ROADMAP.md](ROADMAP.md) – High-level roadmap and current status
- [PARITY_STATUS.md](PARITY_STATUS.md) – Parity matrix and fixtures
- [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) – Architecture mapping
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – How to add fixtures
