# Full Sparkless Backend Roadmap

This document plans the path for **robin-sparkless** to become a complete backend replacement for [Sparkless](https://github.com/eddiethedean/sparkless). Sparkless implements 403+ PySpark functions and 85+ DataFrame methods; robin-sparkless currently covers a core subset with 82 parity fixtures (Phases 11–13 complete).

**Reference**: [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) catalogs all functions/methods; [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) describes architecture mapping.

---

## Current State (February 2026)

| Area | Robin-Sparkless | Sparkless | Gap |
|------|-----------------|-----------|-----|
| **Functions** | ~120+ (Phase 10: mask, translate, substring_index, get_json_object, from_json, to_json, array_exists, forall, filter, transform, array_sum, array_mean; Phase 8: Map/create_map/map_keys/map_values/map_entries/map_from_arrays, array_repeat, array_flatten, soundex, levenshtein, crc32, xxhash64 — all implemented) | 403 | ~283 |
| **DataFrame methods** | ~55+ (Phase 12: filter, select, orderBy, groupBy, withColumn, join, union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed, collect, count, show, read_*; sample, random_split, first, head, tail, take, is_empty, to_json, to_pandas, explain, print_schema, checkpoint, repartition, coalesce, offset, summary, to_df, select_expr, col_regex, with_columns, with_columns_renamed, stat, na, freq_items, approx_quantile, crosstab, melt, except_all, intersect_all, sample_by, no-ops) | 85 | ~30 |
| **Parity fixtures** | 82 passing | 270+ expected_outputs | 190+ |
| **PyO3 bridge** | ✅ Optional `pyo3` feature; `robin_sparkless` Python module | — | — |
| **SQL** | Optional `sql` feature: SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT; temp views | Full DDL/DML support | Subqueries, CTEs, DDL, HAVING |

---

## Phase Overview

| Phase | Goal | Est. Effort |
|-------|------|--------------|
| **1. Foundation** | Structural alignment, case sensitivity, fixture converter | 2–3 weeks |
| **2. High-Value Functions** | Top 60 functions used by Sparkless parity tests | 4–6 weeks |
| **3. DataFrame Methods** | Core methods: union, distinct, drop, fillna, etc. | 3–4 weeks |
| **4. PyO3 Bridge** | Python bindings so Sparkless can call robin-sparkless | 4–6 weeks |
| **5. Test Conversion** | Convert 50+ Sparkless tests, run in CI | 2–3 weeks |
| **6. Broad Function Parity** | Array (array_position, array_remove, posexplode ✅; array_repeat → Phase 8), Map/JSON/string 6.4/window → Phase 8 | 8–12 weeks |
| **7. SQL & Advanced** | SQL executor, Delta Lake, performance & robustness | ✅ **Completed** (optional `sql` / `delta` features) |
| **8. Remaining Parity** | ✅ String 6.4 (mask, translate, substring_index, soundex, levenshtein, crc32, xxhash64); array extensions (exists, forall, filter, transform, sum, mean, array_repeat, array_flatten); Map (create_map, map_keys, map_values, map_entries, map_from_arrays); JSON (get_json_object, from_json, to_json); window fixtures covered | **Done** |

---

## Phase 1: Foundation (2–3 weeks)

**Goal**: Align structure with Sparkless so subsequent work maps cleanly.

### 1.1 Structural Alignment

- [x] Split `dataframe.rs` into submodules:
  - `src/dataframe/transformations.rs` (filter, select, withColumn)
  - `src/dataframe/aggregations.rs` (groupBy, agg)
  - `src/dataframe/joins.rs` (join logic)
- [ ] **Future**: Trait-based backend abstraction (`trait QueryExecutor`, `trait DataMaterializer`) for pluggability
- [ ] **Future**: Document expression model (Column/Expr) and ensure it can represent Sparkless `ColumnOperation` trees

### 1.2 Case Sensitivity

- [x] Add `spark.sql.caseSensitive` configuration (default: false)
- [x] Centralized column resolution for filter, select, withColumn, join
- [x] Fixture for case-insensitive column matching (`case_insensitive_columns`)

### 1.3 Fixture Converter

- [x] Script: `tests/convert_sparkless_fixtures.py` (or Rust tool)
- [x] Map Sparkless JSON (`input_data`, `expected_output`) → robin-sparkless (`input`, `operations`, `expected`)
- [x] Handle operation mapping: filter_operations, groupby, join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed
- [x] Target: Convert 10–20 high-value Sparkless parity tests (run `make sparkless-parity` with `SPARKLESS_EXPECTED_OUTPUTS` set)

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

## Phase 3: DataFrame Methods (3–4 weeks) ✅ **COMPLETED**

**Goal**: Implement methods needed for Sparkless DataFrame pipelines.

| Method | Polars API | Status |
|--------|------------|--------|
| `union` / `unionAll` | `concat` (LazyFrame) | ✅ Done |
| `unionByName` | `concat` with schema alignment by name | ✅ Done |
| `distinct` / `dropDuplicates` | `LazyFrame.unique()` | ✅ Done |
| `drop` | `DataFrame.select()` (exclude columns) | ✅ Done |
| `dropna` | `LazyFrame.drop_nulls()` | ✅ Done |
| `fillna` | `LazyFrame.with_columns` + `fill_null` | ✅ Done |
| `limit` | `DataFrame.head(n)` | ✅ Done |
| `withColumnRenamed` | `DataFrame.rename()` | ✅ Done |
| `replace` | `LazyFrame.replace()` | Medium |
| `crossJoin` | `LazyFrame.join(..., how=Cross)` | Medium |
| `describe` | `LazyFrame.describe()` | Medium |
| `cache` / `persist` | Materialize and store; `unpersist` | Medium |
| `subtract` / `except` | Anti-join or set diff | Medium |
| `intersect` / `intersectAll` | Set operations | Low |

---

## Phase 4: PyO3 Bridge (4–6 weeks) ✅ **COMPLETED**

**Goal**: Enable Sparkless (Python) to call robin-sparkless (Rust) for execution.

### 4.1 Crate Layout

- [x] Optional `pyo3` feature in main crate; `src/python/mod.rs` compiled when `pyo3` enabled
- [x] Expose `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `Column`, `GroupedData` as Python classes (PySpark-like names)
- [x] Data transfer: `create_dataframe` (list of 3-tuples); `collect` → list of dicts

### 4.2 API Surface

- [x] `SparkSession.builder().app_name(...).get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`
- [x] `DataFrame`: `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect`
- [x] `Column` and module-level: `col`, `lit`, `when().then().otherwise()`, `coalesce`, `sum`, `avg`, `min`, `max`, `count`
- [x] `GroupedData`: `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)`

### 4.3 Sparkless Integration (out of scope in this repo)

- [ ] Sparkless `BackendFactory` adds "robin" backend option (implemented in Sparkless repo)
- [ ] When "robin" selected, Sparkless delegates to robin-sparkless via PyO3
- [ ] Fallback: if robin-sparkless doesn't support an op, raise or fall back to Python Polars

See [PYTHON_API.md](PYTHON_API.md) for the API contract Sparkless maintainers need.

### 4.4 Risks

- **Schema round-trip**: PySpark/Sparkless types ↔ Polars ↔ Arrow; ensure nullability and types align
- **Performance**: PyO3 overhead vs. native Python Polars; benchmark
- **Error handling**: Rust errors → Python exceptions with useful messages

---

## Phase 5: Test Conversion (2–3 weeks)

**Goal**: Run Sparkless parity tests against robin-sparkless backend.

- [x] Fixture converter produces robin-sparkless fixtures from Sparkless expected_outputs (join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed; output to `tests/fixtures/converted/` with `--output-subdir`)
- [x] Identify tests that use only supported ops; run those first (run `make sparkless-parity` with `SPARKLESS_EXPECTED_OUTPUTS` set when Sparkless repo available)
- [x] CI: `make sparkless-parity` runs converted tests (converter when path set, then `cargo test pyspark_parity_fixtures`; parity discovers `tests/fixtures/` and `tests/fixtures/converted/`)
- [x] Target: 50+ tests passing on robin-sparkless (82 hand-written passing; document in [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md); add converted when Sparkless expected_outputs used)
- [x] Document which tests fail and why (missing function, semantic difference) in [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md); fixtures can use `skip: true` + `skip_reason`

---

## Phase 6: Broad Function Parity (8–12 weeks)

**Goal**: Implement remaining high-usage functions from PYSPARK_FUNCTION_MATRIX.

### 6.1 Array Functions (Phase 6a done)

- [x] `array`, `array_contains`, `array_join`, `array_max`, `array_min`
- [x] `array_size`, `array_sort`, `element_at`, `explode`
- [x] `array_slice`, `size` (alias for array_size)
- [x] `array_position`, `array_remove`, `posexplode` (implemented via Polars list.eval; Rust + Python)
- [x] `array_repeat`, `array_flatten` (Phase 8; implemented via Expr::map UDFs)
- [x] `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean` (list.eval / list_any_all)
- [ ] `aggregate` (array_aggregate; optional follow-up)

### 6.2 Map Functions ✅ (Phase 8)

- Map represented as `List(Struct{key, value})`. **Phase 8 completed**: `create_map` (as_struct + concat_list), `map_keys`/`map_values` (list.eval + struct.field_by_name), `map_entries` (identity), `map_from_arrays` (zip UDF).

### 6.3 JSON & Binary → Phase 8

- JSON/binary deferred to **Phase 8**. Polars JSON support is behind optional features.
- *Phase 8*: `get_json_object`, `from_json`, `to_json`, `base64`, `unbase64`, etc.

### 6.4 Additional String ✅

- [x] `regexp_extract_all`, `regexp_like` (Phase 6e)
- [x] `regexp_replace` (already present)
- [x] `mask`, `translate`, `substring_index` (Phase 10)
- [x] `soundex`, `levenshtein`, `crc32`, `xxhash64` (Phase 8; UDFs via strsim, crc32fast, twox-hash, soundex crates)

### 6.5 Window Extensions (partial; fixture simplification → Phase 8)

- [x] `percent_rank`, `first_value`, `last_value` (Phase 6d)
- [x] **API** `cume_dist`, `ntile`, `nth_value` (Rust + Python; parity fixtures skipped: Polars does not allow combining rank().over() and count().over() in one expr)
- *Phase 8*: Enable percent_rank/cume_dist/ntile/nth_value parity fixtures when Polars allows, or keep multi-step workaround

### 6.6 Deferred / Out of Scope

- **UDFs**: Python UDFs require Python runtime; document as out of scope; consider pure-Rust UDFs
- **SQL**: Phase 7.1 ✅ — optional `sql` feature: sqlparser + translation to DataFrame ops; temp views
- **Delta Lake**: Phase 7.2 ✅ — optional `delta` feature: read_delta, write_delta, time travel
- **XML**: `xpath_*`; low priority
- **Specialized**: `histogram_numeric`, `hll_*`, `count_min_sketch`; defer

---

## Phase 7: SQL & Advanced (Ongoing)

### 7.1 SQL Support (Optional) ✅

- [x] `SparkSession::sql(query)` → parse SQL, translate to DataFrame ops (when `sql` feature enabled)
- [x] Use `sqlparser` for parsing; execution via existing DataFrame API
- [x] Support: SELECT, FROM (single table or JOIN), WHERE, GROUP BY + aggregates (COUNT, SUM, AVG, MIN, MAX), ORDER BY, LIMIT
- [x] Temporary views: `createOrReplaceTempView`, `table()`
- **Limits (first iteration)**: No subqueries in FROM, no CTEs, no DDL, no HAVING; document unsupported constructs with clear errors.

### 7.2 Delta Lake (Optional) ✅

- [x] Read/write Delta tables (optional `delta` feature; deltalake + Polars)
- [x] Time travel: `read_delta_with_version(path, version)` (read by version)
- [x] Overwrite/append: `write_delta(path, overwrite)`
- [ ] Schema evolution, MERGE (deferred; document as Phase 7 follow-up)

### 7.3 Performance & Robustness ✅

- [x] Benchmarks: robin-sparkless vs. plain Polars (`cargo bench`; criterion)
- [x] Ensure within 2x of Polars for measured pipelines
- [x] Error messages with context (column names, hints); Troubleshooting in QUICKSTART.md
- [ ] Memory profiling, large-dataset handling (optional follow-up)

---

## Phase 8: Remaining Parity ✅ **COMPLETED** (February 2026)

**Goal**: Implement or enable features deferred from Phase 6.

| Item | Status |
|------|--------|
| **array_repeat** | ✅ Implemented via `Expr::map` UDF (list try_apply_amortized + extend). |
| **array_flatten** | ✅ Implemented via `Expr::map` UDF (list-of-lists flatten per row). |
| **Map (6b)** | ✅ `create_map` (as_struct + concat_list), `map_keys`/`map_values` (list.eval + struct.field_by_name), `map_entries` (identity), `map_from_arrays` (zip UDF with list builder). Map represented as `List(Struct{key, value})`. |
| **JSON (6c)** | ✅ get_json_object, from_json, to_json (Phase 10). |
| **String 6.4** | ✅ mask, translate, substring_index (Phase 10); **soundex**, **levenshtein**, **crc32**, **xxhash64** via `Expr::map`/`map_many` UDFs (strsim, crc32fast, twox-hash, soundex crates). |
| **Window fixture simplification** | ✅ percent_rank, cume_dist, ntile, nth_value covered via multi-step workaround in harness. |
| **Documentation of differences** | ✅ [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) updated; no Phase 8 stubs remaining. |

---

## Success Metrics

| Metric | Current | After Phase 13 | After Phase 15 | After Phase 16 (crate) | Full Backend (Phase 17) |
|--------|---------|-----------------|----------------|------------------------|-------------------------|
| Parity fixtures | 82 | 82+ | 150+ | 150+ | 150+ |
| Functions implemented | ~120+ | ~120 | 403 | 403 | 403 |
| DataFrame methods | ~35+ | 85 | 85 | 85 | 85 |
| Crate on crates.io | No | — | — | Yes | Yes |
| Sparkless tests passing (robin backend) | 0 | — | — | — | 200+ |
| PyO3 bridge | ✅ Yes (optional) | Yes | Yes | Yes | Yes |

---

## Path to 100% Before Sparkless Integration (ROADMAP Phases 12–17)

To reach **100% feature parity** and a published crate before wiring the robin backend into Sparkless, [ROADMAP.md](ROADMAP.md) defines the following phases between Phase 11 (done) and integration:

| ROADMAP Phase | Goal | Est. Effort |
|---------------|------|-------------|
| **12** | **DataFrame methods parity**: Implement remaining ~50–60 methods (sample, randomSplit, stat, summary, checkpoint, toJSON, na sub-API, etc.) → 85 total | ✅ **COMPLETED** |
| **13** | **Functions batch 1**: String, binary, collection (~80 new functions → ~200 total). String (ascii, base64, format_string, overlay, etc.), binary (sha1, sha2, md5, aes_*), collection fill-out | ✅ **COMPLETED** |
| **14** | **Functions batch 2**: Math, datetime, type/conditional (~100 new → ~300 total). Full math (sin/cos/tan, degrees/radians, signum), datetime (quarter, weekofyear, add_months, months_between, next_day), casts and conditionals | 4–6 weeks |
| **15** | **Functions batch 3 + fixture growth**: Remaining ~103 functions → 403; parity fixtures 82 → 150+; convert more Sparkless expected_outputs, extend harness for new types | 6–8 weeks |
| **16** | **Publish Rust crate**: Prepare and publish robin-sparkless on crates.io; API stability, docs, release workflow; optional PyPI wheel | 2–3 weeks |
| **17** | **Sparkless integration**: BackendFactory "robin", 200+ tests passing, PyO3 surface updated for all new functions | 4–6 weeks |

Detail for each phase is in [ROADMAP.md](ROADMAP.md) (§ Phase 12–17).

---

## Implementation Order (Summary)

1. **Phase 1**: Fixture converter, case sensitivity, structural split ✅
2. **Phase 2**: String (length, trim, regexp_*), datetime (to_date, date_add, etc.), math (stddev, variance) ✅
3. **Phase 3**: union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed ✅
4. **Phase 4**: PyO3 bridge ✅ **COMPLETED**
5. **Phase 5**: Convert Sparkless tests, CI integration
6. **Phase 6**: Array (6a ✅; array_position, array_remove, posexplode implemented; array_repeat → Phase 8), Map (6b → Phase 8), JSON (6c → Phase 8), additional string (6e ✅; 6.4 → Phase 8), window extensions (6d ✅; fixture simplification → Phase 8).
7. **Phase 7**: SQL, Delta, performance ✅ **Completed** (optional features; see §7)
8. **Phase 8**: ✅ **COMPLETED** – array_repeat, array_flatten, Map (6b), String 6.4 (soundex/levenshtein/crc32/xxhash64), window fixtures, documentation (see Phase 8 section above)
9. **Phase 9**: High-value functions (datetime, string repeat/reverse/lpad/rpad, math sqrt/pow/exp/log, nvl/nullif/nanvl, first/last/approx_count_distinct) + DataFrame methods (replace, cross_join, describe, cache/persist/unpersist, subtract, intersect) ✅ **COMPLETED**
10. **Phase 10**: Complex types (Map, JSON, array_repeat, string 6.4) + window fixture simplification ✅ **COMPLETED**
11. **Phase 11–13**: Parity scale (82 fixtures), harness date/datetime/boolean, Phase 12 DataFrame methods, Phase 13 functions batch 1; converter + CI ✅ **COMPLETED**
12. **ROADMAP Phase 12–17**: Path to 100% before integration — **Phase 12 completed** (DataFrame methods ~55+). Remaining: DataFrame methods → 85, functions in 3 batches (403), fixtures (150+), **Phase 16: prepare and publish crate (crates.io, docs, release)**, then Phase 17: Sparkless integration (see § Path to 100% above).

---

## Related Docs

- [ROADMAP.md](ROADMAP.md) – High-level roadmap and current status
- [PARITY_STATUS.md](PARITY_STATUS.md) – Parity matrix and fixtures
- [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) – Architecture mapping
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – How to add fixtures
