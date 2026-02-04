# Full Sparkless Backend Roadmap

This document plans the path for **robin-sparkless** to become a complete backend replacement for [Sparkless](https://github.com/eddiethedean/sparkless). Sparkless implements 403+ PySpark functions and 85+ DataFrame methods; robin-sparkless currently covers ~283 functions with 159 parity fixtures (Phases 11–24 + signature alignment). **Phase 25** ✅ **completed** (plan interpreter, expression interpreter, logical plan schema, plan fixtures, create_dataframe_from_rows). Next: Phase 26 (publish crate), Phase 27 (integration).

**Reference**: [PYSPARK_FUNCTION_MATRIX](https://github.com/eddiethedean/sparkless/blob/main/PYSPARK_FUNCTION_MATRIX.md) catalogs all functions/methods; [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) describes architecture mapping.

---

## Current State (February 2026)

| Area | Robin-Sparkless | Sparkless | Gap |
|------|-----------------|-----------|-----|
| **Functions** | ~283 (Phase 23: isin, url_decode, url_encode, json_array_length, parse_url, hash, shift_left, shift_right, version, equal_null, stack; Phase 22: curdate, now, localtimestamp, date_diff, dateadd, datepart, extract, unix_micros/millis/seconds, dayname, weekday, make_timestamp, timestampadd, timestampdiff, from_utc_timestamp, to_utc_timestamp, etc.; Phase 21: btrim, locate, conv, hex, unhex, bin, getbit, to_char, to_varchar, to_number, try_to_number, try_to_timestamp, str_to_map, arrays_overlap, arrays_zip, explode_outer, posexplode_outer, array_agg; Phase 20: asc/desc with nulls, median, mode, stddev_pop, var_pop, try_sum, try_avg, bround, negate, positive, cot, csc, sec, e, pi; Phase 19–8: aggregates, try_*, Map, JSON, array extensions, string 6.4 — all implemented) | 403 | ~118 |
| **DataFrame methods** | ~55+ (Phase 12: filter, select, orderBy, groupBy, withColumn, join, union, unionByName, distinct, drop, dropna, fillna, limit, withColumnRenamed, collect, count, show, read_*; sample, random_split, first, head, tail, take, is_empty, to_json, to_pandas, explain, print_schema, checkpoint, repartition, coalesce, offset, summary, to_df, select_expr, col_regex, with_columns, with_columns_renamed, stat, na, freq_items, approx_quantile, crosstab, melt, except_all, intersect_all, sample_by, no-ops; Phase 20: order_by_exprs) | 85 | ~30 |
| **Parity fixtures** | 159 passing | 270+ expected_outputs | 118+ |
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
- [x] Target: 50+ tests passing on robin-sparkless (93 hand-written passing; document in [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md); add converted when Sparkless expected_outputs used)
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

| Metric | Current | After Phase 19 | After Phase 24 (full parity) | After Phase 26 (crate) | Full Backend (Phase 27) |
|--------|---------|----------------|------------------------------|------------------------|-------------------------|
| Parity fixtures | 159 | 159 | 180+ | 180+ | 180+ |
| Functions implemented | ~283 | ~283 | ~400 | ~400 | ~400 |
| DataFrame methods | ~55+ | ~55+ | ~55+ | ~55+ | 85 |
| Crate on crates.io | No | — | — | Yes | Yes |
| Sparkless tests passing (robin backend) | 0 | — | — | — | 200+ |
| PyO3 bridge | ✅ Yes (optional) | Yes | Yes | Yes | Yes |

---

## Path to 100% Before Sparkless Integration (ROADMAP Phases 12–22)

To reach **100% feature parity** and a published crate before wiring the robin backend into Sparkless, [ROADMAP.md](ROADMAP.md) defines the following phases between Phase 11 (done) and integration:

| ROADMAP Phase | Goal | Est. Effort |
|---------------|------|-------------|
| **12** | **DataFrame methods parity**: Implement remaining ~50–60 methods → 85 total | ✅ **COMPLETED** |
| **13** | **Functions batch 1**: String, binary, collection (ascii, base64, overlay, sha1, sha2, md5, array_compact, etc.) | ✅ **COMPLETED** |
| **14** | **Functions batch 2**: Math, datetime, type/conditional (sin/cos/tan, quarter, add_months, cast, try_cast, greatest, least) | ✅ **COMPLETED** |
| **15** | **Functions batch 3** ✅ **COMPLETED**: Batches 1–4 (aliases, string left/right/replace/like, math cosh/cbrt/etc., array_distinct); 88 fixtures. | — |
| **16** | **Remaining gaps 1** ✅ **COMPLETED**: String/regex (regexp_count, regexp_instr, regexp_substr, split_part, find_in_set, format_string, printf); 93 fixtures. | — |
| **17** | **Remaining gaps 2** ✅ **COMPLETED**: Datetime/unix (unix_timestamp, from_unixtime, make_date, timestamp_*, pmod, factorial). | — |
| **18** | **Remaining gaps 3** ✅ **COMPLETED**: array/map/struct (map_filter, zip_with, map_zip_with); 124 fixtures. | — |
| **19** | **Remaining gaps 4** ✅ **COMPLETED**: aggregates, try_*, misc; 128 fixtures. | — |
| **20** | **Full parity 1**: ordering, aggregates, numeric | ✅ **COMPLETED** |
| **21** | **Full parity 2**: string, binary, type, array/map/struct | ✅ **COMPLETED** |
| **22** | **Full parity 3**: datetime extensions | ✅ **COMPLETED** |
| **23** | **Full parity 4**: JSON, CSV, URL, misc | ✅ **COMPLETED** |
| **24** | **Full parity 5**: bit, control, JVM stubs, random, crypto | ✅ **COMPLETED** |
| **25** | **Publish Rust crate**: crates.io, API stability, docs, release; optional PyPI wheel | 2–3 weeks |
| **26** | **Sparkless integration**: BackendFactory "robin", 200+ tests passing, PyO3 surface | 4–6 weeks |

Detail for each phase is in [ROADMAP.md](ROADMAP.md) (§ Phase 12–26).

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
11. **Phase 11–25**: Parity scale (159 fixtures), harness date/datetime/boolean, Phase 12 DataFrame methods, Phase 13–17 functions batches, **Phase 18** array/map/struct, **Phase 19** aggregates, try_*, misc; **Phase 20** ordering, aggregates, numeric; **Phase 21** string, binary, type, array/map/struct; **Phase 22** datetime extensions; **Phase 23** JSON/CSV/URL/misc; **Phase 24** ✅ (bit, control, JVM stubs, rand/randn, AES crypto); **Phase 25** ✅ **COMPLETED** (plan interpreter, expression interpreter, [LOGICAL_PLAN_FORMAT.md](LOGICAL_PLAN_FORMAT.md), plan fixtures, create_dataframe_from_rows). Phase 26 (publish), Phase 27 (integration). See [ROADMAP.md](ROADMAP.md).
12. **ROADMAP Phase 12–27**: Path to 100% — **Phases 12–25 done** (DataFrame methods ~55+, ~283 functions, 159 fixtures; plan interpreter, plan fixtures in `tests/fixtures/plans/`). **Phase 26**: prepare and publish crate (crates.io, docs, release). **Phase 27**: Sparkless integration (see § Path to 100% above).

---

## Related Docs

- [ROADMAP.md](ROADMAP.md) – High-level roadmap and current status
- [PARITY_STATUS.md](PARITY_STATUS.md) – Parity matrix and fixtures
- [SPARKLESS_INTEGRATION_ANALYSIS.md](SPARKLESS_INTEGRATION_ANALYSIS.md) – Architecture mapping
- [SPARKLESS_REFACTOR_PLAN.md](SPARKLESS_REFACTOR_PLAN.md) – Refactor plan for Sparkless (serializable logical plan)
- [READINESS_FOR_SPARKLESS_PLAN.md](READINESS_FOR_SPARKLESS_PLAN.md) – Robin-sparkless readiness (plan interpreter, fixtures) before merge
- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) – How to add fixtures
