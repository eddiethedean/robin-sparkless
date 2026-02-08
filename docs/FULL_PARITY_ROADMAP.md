# Full Parity Roadmap: Robin-Sparkless vs PySpark

A phased plan to achieve full API and behavioral parity between robin-sparkless and Apache PySpark. Based on [GAP_ANALYSIS_PYSPARK_REPO.md](GAP_ANALYSIS_PYSPARK_REPO.md), [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md), and [ROADMAP.md](ROADMAP.md).

---

## Current State (February 2026)

| Area | Robin-Sparkless | PySpark | Gap |
|------|-----------------|---------|-----|
| **Functions** | ~295+ implemented | ~415 | ~120 (many are aliases or param-name mismatches) |
| **DataFrame methods** | ~68+ | ~95 | ~27 |
| **DataFrameReader** | Partial (read_csv, read_parquet, read_json) | 12+ methods | ~12 |
| **DataFrameWriter** | Partial (format/mode/save) | 16+ methods | ~16 |
| **Column methods** | Many as module functions | 17 in class | Structural (robin uses F.xxx style) |
| **GroupedData** | Strong | 10 methods | ~2 |
| **SparkSession** | Core | 36 methods | ~25 |
| **Catalog** | None | 27 methods | 27 |
| **Window** | Core | 4 WindowSpec methods | 4 |

**Parity fixtures:** 159 passing. Target: 200+ for full confidence.

---

## Phases Overview

| Phase | Focus | Est. Effort | Priority |
|-------|-------|-------------|----------|
| **A** | Signature alignment (param names) | 2–3 weeks | High |
| **B** | Missing high-value functions | 3–4 weeks | High |
| **C** | DataFrameReader / DataFrameWriter parity | 2–3 weeks | High |
| **D** | DataFrame method gaps | 2–3 weeks | Medium |
| **E** | SparkSession & Catalog stubs | 1–2 weeks | Medium |
| **F** | Behavioral alignment (diverges) | 2–3 weeks | Medium |
| **G** | Parity fixture expansion | 2–3 weeks | High |
| **H** | Deferred / optional (XML, UDF, streaming) | — | Low |

---

## Phase A: Signature Alignment (2–3 weeks) — COMPLETED

**Goal:** Align parameter names and optional args so existing PySpark code works with minimal changes.

**Scope:** 205 functions/methods classified as "partial" (same logic, different param names).

| Category | PySpark style | Robin style | Action |
|----------|---------------|-------------|--------|
| Column arg | `col` | `column` | Add `col` as alias or primary |
| Numeric args | `months`, `days` | `n` | Add PySpark param names |
| Optional args | `errMsg=None` | (missing) | Add optional params with defaults |
| Array/struct | `cols` / `*cols` | varargs | Align signatures |

**Deliverables (done):**
- PyO3 bindings use `col` and PySpark camelCase param names (errMsg, fromBase, dayOfWeek, etc.)
- Optional params (e.g. `assert_true(col, errMsg=None)`) with PySpark defaults
- Gap analysis partial count reduced from 205 to 21 (exact: 199). See [GAP_ANALYSIS_PYSPARK_REPO.md](GAP_ANALYSIS_PYSPARK_REPO.md)
- Enhanced `extract_robin_api_from_source.py` parses `#[pyo3(signature=...)]` for accurate comparison
- `make gap-analysis-runtime` target added for introspection-based gap analysis
- Default normalization in gap analysis (0/'0', None/'None') for fair comparison
- PyO3 signatures: bround(scale=0), locate(pos=1), btrim(trim=None), from_unixtime(format=None), overlay(len=-1)
- PySpark param renames: conv(fromBase, toBase), convert_timezone(sourceTz, targetTz, sourceTs), sha2/shift_left/shift_right(numBits), months_between(roundOff), next_day(dayOfWeek), like/ilike(escapeChar), parse_url(partToExtract), assert_true/raise_error(errMsg), json_tuple(*fields), regexp_extract_all(str, regexp), make_timestamp(years, months, ...)

---

## Phase B: Missing High-Value Functions (3–4 weeks)

**Goal:** Implement functions that appear in PySpark gap analysis as "missing" but are commonly used.

**High priority (implement):**
- `abs` (if not already present as alias)
- `array(cols)` — construct array from columns
- `ceil` (alias for ceiling if needed)
- `char_length`, `character_length` (aliases for length)
- `date_add`, `date_sub` (aliases if different from current)
- `date_format` (ensure PySpark format string compatibility)
- `current_date`, `current_timestamp` (ensure exposed)
- `aggregate` (array fold) — full semantics if not yet complete
- PySpark aliases: `sign` → signum, `std` → stddev, `mean` → avg, `date_trunc` → trunc

**Medium priority (evaluate):**
- `bucket`, `call_function` (stub or narrow implementation)
- `cume_dist`, `percent_rank` (window) — ensure full parity
- `window`, `window_time` — thin wrappers over `.over()`

**Deliverables:**
- ~30–50 "missing" functions implemented or aliased
- Parity fixtures for new functions

---

## Phase C: DataFrameReader / DataFrameWriter Parity (2–3 weeks)

**Goal:** Expose full Reader/Writer API so `spark.read.*` and `df.write.*` match PySpark.

**DataFrameReader (12 missing):**
- `schema`, `option`, `options`, `format`, `load`
- `table`, `jdbc`, `orc`, `text`, `orc`, `csv` (options)
- `parquet`, `json` (option variants)
- Map existing `read_csv`, `read_parquet`, `read_json` to Reader API

**DataFrameWriter (16 missing):**
- `partitionBy`, `bucketBy`, `sortBy`
- `option`, `options`, `mode`, `format`, `save`
- `saveAsTable`, `insertInto`
- `parquet`, `orc`, `text`, `csv`, `json` (format helpers)
- Map existing write logic to Writer API

**Deliverables:**
- `spark.read.csv(...)` / `spark.read.parquet(...)` etc. match PySpark
- `df.write.mode("overwrite").parquet(path)` etc. match PySpark
- Reader/Writer options (header, inferSchema, etc.) aligned

---

## Phase D: DataFrame Method Gaps (2–3 weeks)

**Goal:** Implement remaining DataFrame methods from gap analysis (52 missing).

**High priority:**
- `createTempView`, `createOrReplaceTempView`, `createGlobalTempView` (stubs or full)
- `corr` (scalar and matrix)
- `checkpoint`, `localCheckpoint`
- `toDF`, `toJSON`, `toPandas` (ensure aliases)
- `observe`, `withWatermark` (stub)

**Medium priority:**
- `hint`, `repartitionByRange`, `sortWithinPartitions` (no-op or minimal)
- `sameSemantics`, `semanticHash`
- `writeTo` (DataFrameWriterV2 stub)

**Already stubbed:** `rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions`, `storageLevel`, `isStreaming`

**Deliverables:**
- ~25 DataFrame methods added or confirmed
- No-op stubs for distributed/streaming methods

---

## Phase E: SparkSession & Catalog Stubs (1–2 weeks)

**Goal:** Expose SparkSession and Catalog methods for API compatibility.

**SparkSession (25 missing):**
- `sql`, `table`, `createDataFrame`, `range` (core — likely done)
- `catalog`, `conf`, `sparkContext` (stub)
- `newSession`, `getActiveSession`, `getDefaultSession`
- `udf`, `udf.register` (stub)
- Version/setActiveSession etc.

**Catalog (27 methods):**
- `listDatabases`, `listTables`, `listFunctions`, `currentDatabase`
- `createTable`, `dropTempView`, `dropGlobalTempView`
- `cacheTable`, `uncacheTable`, `refreshTable`
- Stub most as no-op or return empty/fixed values

**Deliverables:**
- Catalog class with stub methods
- SparkSession methods present (stub where no local equivalent)

---

## Phase F: Behavioral Alignment (2–3 weeks)

**Goal:** Reduce semantic divergences documented in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

| Item | Current | Target |
|------|---------|--------|
| `assert_true` | Fails on any false | Match PySpark null/false semantics |
| `raise_error` | Always errors | Match PySpark |
| `rand` / `randn` | Per-row in with_column | Document; ensure consistency |
| `from_utc_timestamp` / `to_utc_timestamp` | Identity for UTC | Full timezone conversion if feasible |
| `unix_timestamp` / `from_unixtime` | chrono-based | Document timezone assumptions |
| AES crypto | AES-128-GCM | Document vs PySpark modes |
| `create_dataframe` | 3-tuple only | `create_dataframe_from_rows` for arbitrary schemas (done) |

**Deliverables:**
- Divergences doc updated; behavior aligned where practical
- Test fixtures for edge cases (null, timezone)

---

## Phase G: Parity Fixture Expansion (2–3 weeks)

**Goal:** Grow parity coverage from 159 to 200+ fixtures.

**Actions:**
- Convert more Sparkless expected_outputs via `convert_sparkless_fixtures.py`
- Add fixtures for new functions (Phase B)
- Add fixtures for Reader/Writer options (Phase C)
- Add fixtures for signature alignment (Phase A)
- Run `make sparkless-parity` in CI

**Deliverables:**
- 200+ parity fixtures passing
- CI runs full parity suite

---

## Phase H: Deferred / Optional

**Explicitly out of scope for full parity (document only):**
- **XML / XPath:** `from_xml`, `to_xml`, `schema_of_xml`, `xpath*` — require XML parser
- **UDF / UDTF:** `udf`, `pandas_udf`, `udtf`, `call_udf` — no Python UDF support
- **Streaming:** `withWatermark`, `session_window` — no streaming execution
- **Sketch aggregates:** HLL, count-min sketch — optional
- **RDD / distributed:** `rdd`, `foreach`, `mapInPandas` — eager execution only
- **Catalog DDL:** `CREATE TABLE`, etc. — use write to path

---

## Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Parity fixtures passing | 159 | 200+ |
| Functions (exact + partial) | ~220 | ~380+ |
| DataFrame methods | ~68 | ~90 |
| DataFrameReader methods | Partial | 12 |
| DataFrameWriter methods | Partial | 16 |
| Signature "exact" count | 15 | 100+ |
| GAP_ANALYSIS "missing" count | 195 | &lt;50 |

---

## Dependencies

- [GAP_ANALYSIS_PYSPARK_REPO.md](GAP_ANALYSIS_PYSPARK_REPO.md) — gap matrix (regenerate with `make gap-analysis`)
- [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) — semantic divergences
- [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md) — canonical missing list
- [PARITY_STATUS.md](PARITY_STATUS.md) — fixture coverage

---

## Execution Order

1. **Phase A** (signature alignment) — enables drop-in compatibility
2. **Phase G** (fixtures) — run in parallel; expand as phases complete
3. **Phase B** (missing functions) — highest user impact
4. **Phase C** (Reader/Writer) — completes IO surface
5. **Phase D** (DataFrame methods) — fills remaining DataFrame gaps
6. **Phase E** (SparkSession/Catalog) — API completeness
7. **Phase F** (behavioral alignment) — polish

**Total estimated effort:** 14–21 weeks for Phases A–G (excluding deferred Phase H).
