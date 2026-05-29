# PySpark 4 Parity Plan (with PySpark 3 Backwards Compatibility)

**Status:** Living plan · **Last updated:** May 2026 (breaking-changes research incorporated)  
**Audience:** Maintainers of `sparkless` (Python) and `robin-sparkless` (Rust engine)  
**Related:** [FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md), [PARITY_STATUS.md](PARITY_STATUS.md), [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md), [PYSPARK_VERSION_NOTES.md](PYSPARK_VERSION_NOTES.md), [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md), [python_migration.md](python_migration.md)

---

## 1. Goals

| Goal | Definition of done |
|------|-------------------|
| **PySpark 4 parity** | For in-scope APIs, Sparkless behavior matches **PySpark 4.0–4.2** (latest stable 4.x) under documented configuration, with fixture and issue-test coverage. |
| **PySpark 3 backwards compatibility** | Code written for **PySpark 3.2–3.5** continues to work on Sparkless without changes, except for documented intentional differences and opt-in PySpark 4 semantics. |
| **Sustainable maintenance** | One test suite, two oracle modes (3.x and 4.x), automated API/signature diffing, and explicit compatibility tiers in docs and CI. |

**Non-goals** (unchanged; see [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md)): RDD, distributed execution, full Structured Streaming, Mesos/K8s cluster semantics, full Hive metastore DDL, XML/XPath (unless promoted), sketch/HLL aggregates, UDTF.

**Naming note:** The Python package **Sparkless v4** (`pip install sparkless>=4`) refers to the Rust-backed product version, not Apache PySpark 4. This plan is about **Apache PySpark 4.x** API and behavior.

---

## 2. Current baseline (May 2026)

| Dimension | Today |
|-----------|--------|
| **Parity oracle (fixtures)** | PySpark **3.5.x** (`tests/gen_pyspark_cases.py`, `tests/requirements-pyspark.txt`) |
| **Parity fixtures** | **212** JSON fixtures passing ([PARITY_STATUS.md](PARITY_STATUS.md)) |
| **Dual-mode pytest** | `sparkless` (default) vs `SPARKLESS_TEST_BACKEND=pyspark` |
| **Version matrix (Docker)** | Python 3.9–3.13 × PySpark **3.2, 3.3, 3.4, 3.5** ([tests/compatibility_matrix/](tests/compatibility_matrix/README.md)) |
| **API gap vs PySpark repo** | ~120 functions/methods still partial or missing ([FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md), [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md)) |
| **Signature alignment** | Largely complete for 3.5 ([SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md)); re-baseline needed for 4.x |
| **Documented divergences** | [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) |

PySpark 4.x is **not** yet a CI oracle or matrix column. Closing API gaps from the 3.5-era roadmap remains prerequisite work shared by both targets.

---

## 3. Compatibility model

### 3.1 Two axes

1. **API surface** — names, signatures, presence of methods (`createDataFrame`, `read.jdbc`, `F.try_divide`, etc.).
2. **Runtime semantics** — null handling, overflow, cast rules, type coercion, collect() shapes, map key normalization.

PySpark 4 changes **both**. Sparkless must separate **“works on 3.x code”** from **“matches 4.x semantics.”**

### 3.2 Compatibility tiers (target policy)

| Tier | PySpark versions | Sparkless default | Use case |
|------|------------------|-------------------|----------|
| **A — Legacy 3.x** | 3.2–3.5 | **Yes (initial default)** | Existing tests and apps migrated from PySpark 3 / Sparkless 3.x |
| **B — Transitional** | 3.5 + 4.x with explicit config | Opt-in | Teams validating Spark 4 before cutover |
| **C — PySpark 4** | 4.0–4.2 | Future default (major release) | New projects targeting Spark 4 |

**Principle:** Do not silently change Tier A behavior when implementing Tier C. Use **configuration profiles**, not breaking changes in patch releases.

### 3.3 Proposed configuration surface

Introduce a single session-level profile (names are illustrative; finalize in implementation):

```python
# Tier A (default): PySpark 3.5-like semantics where Sparkless differs from 4.0
spark.conf.set("sparkless.pyspark.compat", "3.5")

# Tier B/C: PySpark 4.0+ semantics for ANSI, map keys, collect intervals, etc.
spark.conf.set("sparkless.pyspark.compat", "4.0")
```

Mirror important Spark 4 configs when they affect local engine behavior:

| Config | PySpark 3.5 typical | PySpark 4.0 default | Sparkless action |
|--------|---------------------|---------------------|------------------|
| `spark.sql.ansi.enabled` | `false` | `true` | Honor in expression engine when `compat=4.0`; default `false` for `compat=3.5` |
| `spark.sql.storeAssignmentPolicy` | `ANSI` (3.x) | `ANSI` | Document; align insert/cast strictness per profile |
| `spark.sql.legacy.*` | various | various | Map only flags that affect **local** SQL/expressions; document no-ops for cluster-only flags |
| `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` | N/A (4.0+) | `false` | Implement map schema inference policy for `create_map` / `map_from_arrays` |
| `PYSPARK_YM_INTERVAL_LEGACY` | N/A | env `1` restores 3.x collect | Match `collect()` interval shapes per profile |
| `spark.sql.legacy.disableMapKeyNormalization` | N/A (4.0+) | `false` (normalize -0.0) | Map key behavior when `compat=4.0` |
| `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled` | N/A | `true` restores 3.x inference | Map type inference in `create_map` / PySpark map columns |
| `spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled` | 3.4+ | `true` restores first-element only | Array schema inference (3.4 change; still relevant) |
| `SPARK_ANSI_SQL_MODE` / `SPARK_SQL_LEGACY_CREATE_HIVE_TABLE` | env mirrors | env mirrors | Document for users mirroring cluster configs |

Environment override for CI: `SPARKLESS_PYSPARK_COMPAT=3.5|4.0`.

When `sparkless.pyspark.compat` is set, Sparkless should apply the **bundle** of defaults above (not only `spark.sql.ansi.enabled`) so one knob matches user expectations.

---

## 4. Research sources (authoritative)

| Source | URL | Use in this plan |
|--------|-----|------------------|
| **Upgrading PySpark** | [migration_guide/pyspark_upgrade](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) | Python API removals, pandas-on-Spark, collect/import changes (3.5→4.0, 4.0→4.1) |
| **Spark SQL migration guide** | [sql-migration-guide.md](https://github.com/apache/spark/blob/master/docs/sql-migration-guide.md) | SQL/expression semantics, JDBC, functions (3.5→4.0, 4.0→4.1) |
| **ANSI compliance** | [sql-ref-ansi-compliance](https://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html) | Default-on ANSI: arithmetic, cast, division, `try_*` guidance |
| **Spark 4.0.0 release notes** | [spark-release-4-0-0](https://spark.apache.org/releases/spark-release-4-0-0.html) | VARIANT, SQL UDFs, new DataFrame APIs, dependency baselines |

---

## 5. Breaking changes catalog (PySpark 3 → 4)

Each row is tagged for Sparkless planning:

- **Relevance:** `HIGH` = must implement or emulate per compat profile · `MED` = implement if feature exists · `LOW` = document / no-op · `N/A` = cluster/JVM only  
- **Tier:** which compat profile first targets the 4.x behavior (`4.0` / `4.1` / both)

### 5.1 Spark SQL semantics (3.5 → 4.0) — HIGH priority for engine

These come from the [Spark SQL 3.5 → 4.0 migration guide](https://github.com/apache/spark/blob/master/docs/sql-migration-guide.md) and [ANSI compliance](https://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html).

| Change | PySpark 3.5 | PySpark 4.0 | Legacy restore | Sparkless |
|--------|-------------|-------------|----------------|-----------|
| **ANSI mode default** | `spark.sql.ansi.enabled=false` | **`true`** | Set config or `SPARK_ANSI_SQL_MODE=false` | **HIGH** · Phase 4A · overflow/cast/divide throw vs null |
| **Arithmetic overflow** | Wraps (e.g. `INT_MAX+1` → negative) | **Exception** (or use `try_add`, etc.) | `ansi.enabled=false` | **HIGH** · same |
| **Invalid CAST** | Often `NULL` | **Exception** (or `try_cast`) | `ansi.enabled=false` | **HIGH** · str→int, numeric narrowing |
| **Division by zero** | `NULL` in many paths | **Exception** (or `try_divide`) | `ansi.enabled=false` | **HIGH** |
| **Store assignment** | `spark.sql.storeAssignmentPolicy=ANSI` (3.x) | Still **ANSI** default | `legacy` / `strict` policies | **MED** · insert/cast strictness if we add INSERT |
| **Map key -0.0** | No normalization | Normalize to `0.0` in `create_map`, `map_from_arrays`, `map_from_entries`, `map_concat` | `spark.sql.legacy.disableMapKeyNormalization=true` | **HIGH** · Phase 4B |
| **Map schema inference** | First non-null pair (PySpark) | Merge all pairs | `spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled=true` | **HIGH** · Phase 4B |
| **Array schema inference** (3.4+, still relevant) | First element | Merge all elements | `spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled=true` | **MED** · createDataFrame / arrays |
| **`encode` / `decode` charsets** | JDK charsets | Only US-ASCII, ISO-8859-1, UTF-8/16/32 variants | `spark.sql.legacy.javaCharsets=true` | **MED** · we implement UTF-8/hex; audit others |
| **`encode` / `decode` errors** | Mojibake replacement | **`MALFORMED_CHARACTER_CODING`** | `spark.sql.legacy.codingErrorAction=true` | **MED** |
| **`format_string` indexes** | `0$` allowed historically | **1-based only** (`1$`, `2$`, …) | `spark.sql.legacy.allowZeroIndexInFormatString` (deprecated) | **MED** · already partially aligned |
| **`array_insert` negative index** | Legacy index rules | **1-based**; `-1` inserts at end (3.5+) | `spark.sql.legacy.negativeIndexInArrayInsert=true` | **LOW** · verify current behavior |
| **Timestamp → int cast overflow** | Wrapping | **NULL** (non-ANSI path) | — | **MED** · cast path |
| **Time parser policy default** | `EXCEPTION` | **`CORRECTED`** | — | **MED** · `to_timestamp` / parsing |
| **CTE precedence** | `EXCEPTION` on conflict | **`CORRECTED`** (inner wins) | `spark.sql.legacy.ctePrecedencePolicy=EXCEPTION` | **LOW** · SQL parser |
| **SQL `!` as NOT** | Bug allowed `! IN`, `! BETWEEN` | **Syntax error** | `spark.sql.legacy.bangEqualsNot=true` | **LOW** · SQL only |
| **`sentences()` locale** | `Locale.US` when country null | **`Locale(language)`** | — | **LOW** · if implemented |
| **Parquet/ORC defaults** | ORC snappy | ORC **zstd** | `spark.sql.orc.compression.codec=snappy` | **N/A** · write paths |
| **Datetime rebasing config names** | `spark.sql.legacy.parquet.*` | Renamed to `spark.sql.parquet.*` | Use new names | **LOW** · document for users |
| **CREATE TABLE default provider** | Hive | `spark.sql.sources.default` | `spark.sql.legacy.createHiveTableByDefault=true` | **LOW** · SQL DDL subset |

**ANSI `try_*` functions** (many added in 3.5, behavior clarified under ANSI-on in 4.x): `try_add`, `try_subtract`, `try_multiply`, `try_divide`, `try_cast`, `try_to_timestamp`, `try_to_number`, `try_aes_decrypt`, etc. Under ANSI-on, these return **NULL** on failure instead of throwing. Sparkless should implement these as **null-propagating** variants regardless of profile, and ensure they match 4.x when `compat=4.0` and `ansi.enabled=true`. See [try_divide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.try_divide.html).

**Function behavior changes (4.0 highlights from release notes):**

| Function / area | Change | Sparkless |
|-----------------|--------|-----------|
| `mode()` | New `deterministic` arg (4.0) | **MED** · signature + behavior |
| `array_insert()` | 1-based negative indexes | **LOW** |
| `encode`/`decode` | Stricter charset + errors | **MED** |
| `to_csv` | Arrays/maps/binary as pretty strings | **LOW** |
| `parse_json` / VARIANT | New in Spark 4.0 | **Defer** VARIANT; evaluate `parse_json` |
| `listagg`, `mergeInto`, `groupingSets`, lateral joins | New DataFrame/SQL APIs | **MED** · per [ROBIN_SPARKLESS_MISSING](ROBIN_SPARKLESS_MISSING.md) |

### 5.2 PySpark Python API (3.5 → 4.0)

From [Upgrading PySpark — 3.5 to 4.0](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html).

#### Environment and dependencies

| Change | Detail | Sparkless |
|--------|--------|-----------|
| Python 3.8 dropped | 4.0+ requires newer Python | **LOW** · document; CI 3.10+ for PySpark 4 oracle |
| Pandas ≥ 2.0 | Was ≥ 1.0.5 | **LOW** · `toPandas` tests only |
| NumPy ≥ 1.21 | Was ≥ 1.15 | **LOW** |
| PyArrow ≥ 11.0 | Was ≥ 4.0.0 | **MED** · `createDataFrame(pyarrow.Table)` target |
| JDK for PySpark install | **Eliminated** in 4.0 for pip install (cluster still needs Java) | **N/A** · Sparkless has no JVM |

#### `pyspark.sql` / DataFrame (in scope)

| Change | Detail | Sparkless |
|--------|--------|-----------|
| `from pyspark.sql.functions import *` | **No longer** exports `DataFrame`, `Column`, `StructType`, etc. | **LOW** · we already use explicit imports in docs |
| `createDataFrame(pyarrow.Table)` | Supported in 4.0 | **HIGH** · [CREATEDATAFRAME_GAPS](CREATEDATAFRAME_GAPS.md) |
| Map schema inference | Merge all dict pairs (see §5.1) | **HIGH** |
| `collect()` + `YearMonthIntervalType` | No longer raw integers | `PYSPARK_YM_INTERVAL_LEGACY=1` | **HIGH** · Phase 4B |
| `dropDuplicates` / `dropDuplicatesWithinWatermark` | Accept **var-args** columns | **MED** · signature |
| `DataFrame.mergeInto` | New in 4.0 | **MED** · defer or stub |
| `groupingSets` | DataFrame API | **MED** |
| Lateral join DataFrame API | New | **LOW** · defer |
| `parse_json` column API | New | **MED** |
| `VariantVal` / VARIANT in UDFs | New | **Defer** |
| `DataType.fromDDL` | New | **MED** · overlaps DDL parser work |
| Parameterized `spark.sql(..., args=)` | 3.4+ named params | **LOW** · SQL subset |
| `Column` accepts Python `Enum` | 4.0 | **LOW** |
| Bare literals in `Column &` / `\|` | 4.0 | **LOW** |

#### Pandas API on Spark (`pyspark.pandas`) — out of scope

Large removal/rename surface (Koalas removal, `to_koalas` → `pandas_api`, `iteritems` → `items`, many parameter renames, `assertPandasOnSparkEqual` removed, etc.). **Sparkless does not implement `pyspark.pandas`.** Document that users migrating **only** classic `pyspark.sql` are unaffected; pandas-on-Spark migrations are a separate project.

Notable 4.0 pandas-on-Spark interaction: **raises if Spark runs with ANSI on** unless `compute.fail_on_ansi_mode=False` or ANSI disabled — irrelevant to Sparkless unless we add pandas API later.

### 5.3 PySpark / Spark SQL (4.0 → 4.1)

From [Upgrading PySpark — 4.0 to 4.1](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) and SQL migration guide § “3.5 to 4.0” successor.

| Change | Detail | Sparkless |
|--------|--------|-----------|
| **Python 3.9 dropped** | 4.1+ | **LOW** · matrix / CI |
| **PyArrow ≥ 15.0** | Was 11.0 | **MED** · Arrow createDataFrame tests |
| **Pandas ≥ 2.2** | Was 2.0 | **LOW** |
| **Spark Connect: `DataFrame['col']`** | No longer eagerly validates name | `PYSPARK_VALIDATE_COLUMN_NAME_LEGACY=1` | **LOW** · if we mimic Connect |
| **Arrow UDF: UDT support** | Was fallback | `spark.sql.execution.pythonUDF.arrow.legacy.fallbackOnUDT` | **N/A** · UDF execution model differs |
| **Arrow/pandas conversion removed** | Type coercion changes | legacy pandas conversion configs | **N/A** / **LOW** |
| **`BinaryType` → Python `bytes`** | Default in 4.1 (was `bytearray` in many paths) | `spark.sql.execution.pyspark.binaryAsBytes=false` | **MED** · collect / UDF boundaries |
| **`convertToArrowArraySafely` default true** | Overflow/truncation errors in Arrow | set to `false` to restore | **MED** · if Arrow ingest added |
| **pandas-on-Spark ANSI** | `compute.ansi_mode_support=True` default | — | **N/A** |
| **Parquet struct nullness (SQL 4.1)** | No longer assume all-null struct if fields missing | `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing=true` | **LOW** · Parquet read |
| **Thrift Server column ordinal (4.1)** | 1-based ORDINAL fix | legacy hive thrift config | **N/A** |
| **Log4j 1 → 2 (Spark 4.1)** | Cluster logging | **N/A** |

### 5.4 JDBC type mapping (3.5 → 4.0) — MED for JDBC feature

If Sparkless JDBC is in use, reproduce per datasource with profile `compat=4.0` and legacy flags:

| Datasource | Change (summary) | Legacy flag |
|------------|------------------|-------------|
| **PostgreSQL** | Read/write TIMESTAMP WITH TIME ZONE vs NTZ rules | `spark.sql.legacy.postgres.datetimeMapping.enabled` |
| **MySQL** | TIMESTAMP→TimestampType; SMALLINT→Short; FLOAT→Float; BIT(n>1)→Binary; write Short as SMALLINT; NTZ write as DATETIME | `spark.sql.legacy.mysql.*` |
| **Oracle** | Timestamp write as TIMESTAMP WITH LOCAL TIME ZONE | `spark.sql.legacy.oracle.timestampMapping.enabled` |
| **SQL Server** | TINYINT→Short; DATETIMEOFFSET→Timestamp | `spark.sql.legacy.mssqlserver.*` |
| **DB2** | SMALLINT→Short; Boolean write as BOOLEAN | `spark.sql.legacy.db2.*` |

**Phase:** add JDBC regression pack tagged `pyspark4_only` + `compat=4.0` (extends existing testcontainers tests).

### 5.5 Cluster / runtime changes — document only (N/A for engine)

| Change | Note |
|--------|------|
| **Java 17+ default** (Spark 4.0) | PySpark 4 oracle CI should use Java 17 |
| **Scala 2.13 only** | N/A |
| **Mesos removed** | N/A |
| **ANSI default** | Emulated via config (§3.3) |
| **Hive &lt; 2.0 dropped** | N/A |
| **Structured Streaming `Trigger.Once` deprecated** | Stubs remain |
| **Spark Connect / pyspark-client** | Separate distribution; optional future testing |

### 5.6 Spark 4.0 new capabilities (parity backlog, not all “breaking”)

From [Spark 4.0.0 release notes](https://spark.apache.org/releases/spark-release-4-0-0.html) — track in [ROBIN_SPARKLESS_MISSING](ROBIN_SPARKLESS_MISSING.md):

| Feature | Parity priority |
|---------|-----------------|
| **VARIANT** type + semi-structured SQL | Defer / stub unless Polars path exists |
| **SQL user-defined functions** | Partial (session UDFs exist); align SQL `CREATE FUNCTION` |
| **Session variables**, pipe syntax, collations | LOW / defer for local SQL |
| **Built-in XML datasource** | Defer ([DEFERRED_SCOPE](DEFERRED_SCOPE.md)) |
| **`parse_json`**, `to_json` enhancements | MED |
| **`DataFrame.mergeInto`**, `writeTo` / DSv2 | MED / stub |
| **Python Data Source API** | N/A (cluster) |
| **Python UDTF** | Defer |
| **PySpark Plotting API** | N/A |
| **`applyInArrow` on groupBy/cogroup** | Defer |
| **Time travel on `df.read`** | LOW |

### 5.7 Summary matrix — what to implement first

| Priority | Items |
|----------|--------|
| **P0 (blocking 4.0 semantics)** | ANSI default bundle, map keys + inference, interval collect, `try_*` under ANSI-on |
| **P1 (common API gaps)** | PyArrow `createDataFrame`, JDBC 4.0 mappings, `encode`/`decode` strictness |
| **P2 (4.0 API additions)** | `mergeInto`, `groupingSets`, `parse_json`, `mode(deterministic=)`, var-args `dropDuplicates` |
| **P3 (4.1 polish)** | `BinaryType`→`bytes`, Arrow safe conversion defaults |
| **Defer** | VARIANT, pyspark.pandas, streaming, RDD, XML datasource, Python Data Source |

---

## 6. PySpark 3 vs 4 — Sparkless impact (condensed)

Section **§5** is the researched catalog; this table is the maintainer quick view.

| Area | PySpark 3.x | PySpark 4.x | Sparkless phase |
|------|-------------|-------------|-----------------|
| **ANSI SQL** | Off by default | **On by default** | 4A |
| **Map keys / inference** | Legacy | Normalized keys; merged schemas | 4B |
| **`collect()` intervals** | Legacy integers | New types / env legacy | 4B |
| **try_*** | 3.5+ | Required for ANSI-on ergonomics | 4A + existing functions |
| **JDBC types** | 3.5 mappings | Per-DB breaking mappings | 4C + JDBC tests |
| **PyArrow Table** | 4.0+ | Supported | P1 / CREATEDATAFRAME |
| **Wildcard import** | Broader `import *` | Functions only | Docs only |
| **Python** | 3.8+ (3.5) | 3.10+ (4.1) | CI matrix |

### API surface audit (unchanged process)

Run periodic extraction (extend [scripts/extract_pyspark_tests.py](scripts/extract_pyspark_tests.py) / gap scripts):

- Re-point [GAP_ANALYSIS_PYSPARK_REPO.md](GAP_ANALYSIS_PYSPARK_REPO.md) from `v3.5.0` to `v4.1.x` branch.
- Refresh [signatures_pyspark.json](signatures_pyspark.json) and [SIGNATURE_GAP_ANALYSIS.md](SIGNATURE_GAP_ANALYSIS.md).
- Categorize each delta: **3-only**, **4-only**, **shared**, **semantic-only** — cross-link to §5 rows.

**Known 4.x surface still open:** see §5.6 and [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md), [CREATEDATAFRAME_GAPS.md](CREATEDATAFRAME_GAPS.md).

---

## 7. Testing strategy

### 5.1 Oracle dual-track

| Track | PySpark install | Purpose |
|-------|-----------------|--------|
| **Primary (keep)** | `pyspark>=3.5,<3.6` | Regression guard for Tier A; existing 212 fixtures |
| **Secondary (add)** | `pyspark>=4.0,<4.2` on Python 3.10+ | Tier B/C oracle; new fixtures for 4-only behavior |

**Deliverables:**

1. `tests/requirements-pyspark4.txt` — PySpark 4 + `delta-spark>=4.0,<5`.
2. CI job: `SPARKLESS_PYSPARK_COMPAT=4.0 SPARKLESS_TEST_BACKEND=pyspark pytest tests/parity tests/dataframe -m "not jdbc"` (scope TBD).
3. Extend `tests/gen_pyspark_cases.py` to accept `--pyspark-version 3.5|4.0` and tag fixture metadata.

### 5.2 Compatibility matrix

Extend [run_matrix_tests.py](tests/compatibility_matrix/run_matrix_tests.py):

| Python | PySpark | Status |
|--------|---------|--------|
| 3.9–3.13 | 3.2–3.5 | **Keep** (Tier A) |
| 3.10–3.13 | 4.0–4.2 | **Add** (Tier B); drop 3.9 for this row |

### 5.3 Test taxonomy (markers)

Define pytest markers (document in [TESTING_GUIDE.md](TESTING_GUIDE.md)):

- `pyspark3_only` — behavior differs on 4.x; skip when oracle is 4.0.
- `pyspark4_only` — requires ANSI-on or 4.x APIs; skip when oracle is 3.5.
- `compat_profile("3.5")` / `compat_profile("4.0")` — run with `sparkless.pyspark.compat` set.

**Rule:** Every semantic change for PySpark 4 ships with **at least one** fixture or issue test per profile.

### 5.4 sparkless.testing dual mode

Extend [sparkless.testing](python/sparkless/sparkless/testing/) (see [TESTING_GUIDE.md](TESTING_GUIDE.md)):

```bash
# Today
SPARKLESS_TEST_BACKEND=pyspark pytest ...

# Proposed
SPARKLESS_TEST_BACKEND=pyspark SPARKLESS_PYSPARK_COMPAT=4.0 pytest ...
SPARKLESS_TEST_BACKEND=sparkless SPARKLESS_PYSPARK_COMPAT=4.0 pytest ...
```

Comparison helpers should use **profile-aware** tolerances (e.g. exception vs null under ANSI).

---

## 8. Implementation phases

Phases **P0–P3** close shared API gaps. Phases **4A–4D** are PySpark 4-specific. Phases **M1–M3** are maintenance.

### P0 — Inventory and gates (2–3 weeks)

- [ ] Extract PySpark **4.1.x** API into `docs/pyspark_api_4.x.json` (parallel to [pyspark_api_from_repo.json](pyspark_api_from_repo.json)).
- [ ] Produce **3.5 vs 4.1 delta report**: API added/removed/changed; link to [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md).
- [ ] Define `sparkless.pyspark.compat` spec and list of configs honored per profile.
- [ ] Update [PYSPARK_VERSION_NOTES.md](PYSPARK_VERSION_NOTES.md) with dual-oracle instructions.

**Exit criteria:** Published delta report; compat config RFC merged; no code behavior change yet.

### P1 — Finish shared API parity (ongoing; see FULL_PARITY_ROADMAP)

Continue [FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md) remaining items:

- Reader/Writer: `orc`, `text`, `bucketBy`, `insertInto` (if in scope).
- Catalog: persistent DDL beyond in-memory ([DEFERRED_SCOPE.md](DEFERRED_SCOPE.md) boundaries).
- Functions: deferred list in [ROBIN_SPARKLESS_MISSING.md](ROBIN_SPARKLESS_MISSING.md).
- `createDataFrame`: PyArrow Table, nested struct edge cases ([CREATEDATAFRAME_GAPS.md](CREATEDATAFRAME_GAPS.md)).

**Exit criteria:** Gap report “missing” count not regressed; parity fixtures ≥ 212 still green on 3.5 oracle.

### P2 — Signature re-baseline for 4.x (2–4 weeks)

- [ ] Re-run signature gap tooling against PySpark 4.1.
- [ ] Fix PyO3 `#[pyo3(signature = ...)]` mismatches in `python/src/lib.rs`.
- [ ] Add optional parameters introduced in 4.x without breaking 3.x call sites (defaults preserved).

**Exit criteria:** ≥ 95% of **shared** functions exact match on 4.1 introspection; documented exceptions in [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md).

### P3 — Documentation and user contract (1 week)

- [ ] New section in [python_migration.md](python_migration.md): “Targeting PySpark 4”.
- [ ] Extend [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) with **Compatibility profiles** table.
- [ ] Read the Docs nav entry for this plan.

**Exit criteria:** Users can choose Tier A vs B explicitly; no ambiguous “we match PySpark” claims.

---

### 4A — ANSI and arithmetic semantics (3–5 weeks)

**Owner:** `robin-sparkless-polars` expression + type coercion paths. **Scope:** §5.1 ANSI rows + `try_*` functions.

- [ ] Implement `spark.sql.ansi.enabled` behavior for: overflow, divide by zero, cast failures, string-to-number parse (align with [ANSI compliance](https://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html) subset used in tests).
- [ ] Default **off** when `compat=3.5`; **on** when `compat=4.0`.
- [ ] Port/adapt tests from Spark’s ANSI suites where feasible (Rust unit tests + Python issue tests).

**Exit criteria:** Curated ANSI fixture pack passes on both profiles; documented divergences ≤ agreed list.

### 4B — Type system and collect paths (2–4 weeks)

**Scope:** §5.1 map/interval rows, §5.2 PySpark collect changes.

- [ ] Map key normalization (`-0.0` / `0.0`) behind `compat=4.0` and `spark.sql.legacy.disableMapKeyNormalization`.
- [ ] Map schema inference (first pair vs merge) for `create_map` / struct maps.
- [ ] `YearMonthIntervalType` / `DayTimeIntervalType` collect shapes per `PYSPARK_YM_INTERVAL_LEGACY` and compat profile.
- [ ] Evaluate **VARIANT** — default: **stub + defer** in [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md) unless product priority changes.

**Exit criteria:** Issue tests for map + interval collect; parity fixtures tagged `pyspark4_only` where needed.

### 4C — PySpark 4 oracle CI (2–3 weeks)

- [ ] `tests/requirements-pyspark4.txt` + CI job on Ubuntu, Python 3.11, Java 17.
- [ ] Subset of `tests/parity/` and high-value `tests/dataframe/test_issue_*.py` green against real PySpark 4.1.
- [ ] Matrix row: Py 3.10–3.13 × PySpark 4.0–4.2.

**Exit criteria:** CI badge “PySpark 4 oracle”; failure budget documented (target: 0 for parity dir).

### 4D — Tier C default (major release only)

- [ ] **Sparkless 5.x** (or agreed major): default `sparkless.pyspark.compat=4.0`.
- [ ] Migration guide: enable `compat=3.5` for one release cycle with deprecation warning.
- [ ] Changelog breaking section.

**Exit criteria:** Semver-major release with clear migration path; Tier A available via config for ≥ 12 months.

---

### M1 — Ongoing parity hygiene

- Weekly: run `make gap-analysis-runtime` (or successor) on 3.5 and 4.1.
- Per PR: new PySpark-facing APIs require fixture + [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) entry if behavior differs.
- Per release: refresh [PARITY_STATUS.md](PARITY_STATUS.md) counts for both oracles.

### M2 — Upstream test port

- [scripts/extract_pyspark_tests.py](scripts/extract_pyspark_tests.py) — branch `v4.1.x`; translate to issue tests ([PYSPARK_TEST_TRANSLATION.md](PYSPARK_TEST_TRANSLATION.md)).
- Prioritize: ANSI, try_*, map, interval, createDataFrame.

### M3 — Issue-driven backlog

- GitHub labels: `pyspark-4`, `compat-3.5`, `ansi`.
- Triage [SPARKLESS_PARITY_ISSUES_REPORTED.md](SPARKLESS_PARITY_ISSUES_REPORTED.md) against 4.1 reproduction steps.

---

## 9. Release and semver policy

| Change type | Semver | Example |
|-------------|--------|---------|
| New function matching 3.5 and 4.0 | Minor | `F.new_fn` |
| Bug fix aligning to 3.5 oracle | Patch | Fix `filter` null semantics |
| New **opt-in** `compat=4.0` behavior | Minor | ANSI throws when enabled |
| **Default** switch to PySpark 4 semantics | **Major** | Sparkless 5.0 default compat |
| Remove 3.x-only API | **Major** | Only after deprecation |

**Sparkless package version (4.x)** can continue shipping **Tier A defaults** until Phase **4D**.

---

## 10. Success metrics

| Metric | Target (Tier B) | Target (Tier C / 4D) |
|--------|-----------------|-------------------------|
| Parity fixtures vs oracle | 212+ pass on **3.5** | 212+ pass on **4.1** (allow additional 4-only fixtures) |
| `tests/parity/` PySpark 4 CI | ≥ 95% pass | 100% pass |
| Signature exact match (shared API) | — | ≥ 95% vs 4.1 |
| Compatibility matrix | 3.2–3.5 green | + 4.0–4.2 green on Py 3.10+ |
| Documented intentional diffs | Stable list in PYSPARK_DIFFERENCES | Same + profile column |

---

## 11. Risks and mitigations

| Risk | Mitigation |
|------|------------|
| ANSI semantics diverge from Polars | Implement ANSI layer in `expr_ir` / coercion, not only Polars defaults; Rust unit tests |
| Dual oracle doubles CI time | Sharded jobs; parity on 4.x nightly, 3.5 on every PR |
| Users expect cluster Spark 4 | Document “local engine subset”; link JVM-only items to DEFERRED_SCOPE |
| VARIANT / new types blocked on Polars | Explicit defer; stub types for parse-only SQL if needed |
| Confusion: Sparkless v4 vs PySpark 4 | Glossary in README and [python_migration.md](python_migration.md) |
| Breaking-change drift in upstream Spark | Pin research to Spark **4.1.x** docs; re-diff on minor releases (§4) |

---

## 12. Immediate next steps (recommended order)

1. **P0** — PySpark 4.1 API extraction; cross-link each §5 row to gap tracker / GitHub issues.  
2. **P0** — Publish `sparkless.pyspark.compat` RFC mapping §3.3 + §5.1 legacy flags.  
3. **4A** — ANSI bundle (§5.1): overflow, cast, divide + `try_*` fixtures with `ansi.enabled` on/off.  
4. **4B** — Map keys + inference + interval collect (§5.1–5.2).  
5. **P1** — Shared API gaps + PyArrow `createDataFrame` (§5.2).  
6. **4C** — `requirements-pyspark4.txt`, Java 17 CI oracle, JDBC 4.0 test pack (§5.4).

---

## 13. Reference index

| Document | Role in this plan |
|----------|------------------|
| [FULL_PARITY_ROADMAP.md](FULL_PARITY_ROADMAP.md) | Shared API phases A–H (mostly 3.5-era) |
| [PARITY_STATUS.md](PARITY_STATUS.md) | Fixture matrix and phase manifest |
| [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) | Intentional divergences; add profile column |
| [PYSPARK_VERSION_NOTES.md](PYSPARK_VERSION_NOTES.md) | PySpark mode test setup |
| [DEFERRED_SCOPE.md](DEFERRED_SCOPE.md) | Out-of-scope boundaries |
| [TESTING_GUIDE.md](TESTING_GUIDE.md) | Dual-backend testing |
| [GAP_ANALYSIS_PYSPARK_REPO.md](GAP_ANALYSIS_PYSPARK_REPO.md) | Re-run for v4.1 |
| [CREATEDATAFRAME_GAPS.md](CREATEDATAFRAME_GAPS.md) | PyArrow / schema gaps |
| [tests/compatibility_matrix/README.md](tests/compatibility_matrix/README.md) | Version matrix extension |

**External (breaking-change research)**

- [Upgrading PySpark](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) — §5.2, §5.3  
- [Spark SQL migration guide](https://github.com/apache/spark/blob/master/docs/sql-migration-guide.md) — §5.1, §5.4  
- [ANSI compliance (Spark 4.0)](https://spark.apache.org/docs/4.0.0/sql-ref-ansi-compliance.html) — §5.1  
- [Spark 4.0.0 release notes](https://spark.apache.org/releases/spark-release-4-0-0.html) — §5.6

---

## Appendix A — Glossary

| Term | Meaning |
|------|---------|
| **Oracle** | Real PySpark used to generate expected outputs or run comparison tests |
| **Fixture** | JSON test case under `tests/fixtures/` |
| **Profile / compat** | `3.5` or `4.0` semantic mode for Sparkless |
| **Tier A/B/C** | Backwards-compat policy levels (§3.2) |
| **Sparkless v4** | Python package major version with Rust engine (not PySpark 4) |

## Appendix B — Suggested CI layout (sketch)

```yaml
# PR: fast path
- sparkless backend, all tests, compat=3.5 (default)
- rust: make check

# PR: pyspark oracle 3.5 (subset or full)
- SPARKLESS_TEST_BACKEND=pyspark, pyspark 3.5, Java 17

# Nightly
- SPARKLESS_TEST_BACKEND=pyspark, pyspark 4.1, compat=4.0, Python 3.11
- compatibility_matrix including pyspark 4.x row
```

---

*Maintainers: update the **Current baseline** table and checkboxes when phases complete.*
