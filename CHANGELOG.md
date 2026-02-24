# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

(none yet)

### Fixed

(none yet)

## [0.17.0] - 2026-02-24

### Fixed

- **#985 – Window orderBy empty list** — Clear error when `Window.orderBy()` is called with an empty list.
- **#986 – isin with string column and numeric values** — Plan interpreter and Column API: `isin` with a string column and numeric literals now coerces correctly (string column vs numeric list).
- **#987 – between with string column and numeric bounds** — Plan interpreter applies string–numeric coercion so `col("val").between(1, 10)` works when `val` is string.
- **#988 – Numeric column == string literal** — Comparison with numeric column and string literal now coerces (e.g. string parsed to number) instead of type error.
- **#989 – String to boolean cast (empty string)** — Casting empty string to boolean now yields `false` (PySpark parity).
- **#990 – String column arithmetic** — Plan interpreter rejects or coerces string arithmetic appropriately; error messages improved.
- **#991 – Bitwise NOT on CaseWhen (Unknown)** — Bitwise NOT (`~`) on `when().then().otherwise()` expressions with unknown type now works.
- **Parity type mismatches** — `width_bucket`, `bit_length`, `dayofweek`, and `position`/`locate` now return int/bigint (explicit casts) so parity schema checks pass (datetime_quarter_week, width_bucket, position_start, bit_length_function fixtures).

## [0.15.0] - 2026-02-18

### Changed

- **Version** — Bump to 0.15.0 across robin-sparkless, robin-sparkless-core, and robin-sparkless-polars.
- **Docs** — READMEs and docs updated for 0.15.0; RELEASING.md and POLARS_CRATE_ISOLATION.md current-state references updated.

## [0.14.0] - 2026-02-20

### Added

- **spark-sql-parser** — SQL parsing extracted to a separate crate `spark-sql-parser`; robin-sparkless uses it when the `sql` feature is enabled. Release CI now publishes spark-sql-parser, then core, polars, and main.

### Changed

- **Release workflow** — Publish order: spark-sql-parser, robin-sparkless-core, robin-sparkless-polars, robin-sparkless. See docs/RELEASING.md.

## [0.13.0] - 2026-02-20

### Added

- **Workspace / crate split** — The crate is now a Cargo workspace with three publishable crates: `robin-sparkless` (root, main facade), `robin-sparkless-core` (schema, config, error, date utils), and `robin-sparkless-expr` (column, expressions, functions, type coercion, UDFs). The root crate re-exports the public API; advanced or minimal-use cases can depend on the subcrates directly. Release workflow publishes core, then expr, then main to crates.io in order.

### Changed

- **RELEASING.md** — Updated for workspace: version bump in all three `Cargo.toml` files, tag triggers CI which publishes all three crates.
- **cargo audit** — Ignore bincode unmaintained advisory (RUSTSEC-2025-0141) in `.cargo/audit.toml` so `cargo audit` is quiet when used with polars 0.53.

## [0.12.2] - 2026-02-19

### Fixed

- **#627 – create_dataframe_from_rows map type** — Map columns (e.g. `map<string,string>`) now accepted; JSON object (dict) per row, stored as List(Struct{key, value}). PySpark parity.
- **#628 – between() string vs numeric** — Plan interpreter applies string–numeric coercion in `between(lower, upper)` so `col("val").between(1, 10)` works when `val` is string.
- **#629 – Temp view resolution** — Added test and docs: temp views are session-scoped; bindings must use the same SparkSession for `create_or_replace_temp_view` and `table()` / `sql()`.

## [0.12.1] - 2026-02-19

### Fixed

- **#624 – create_dataframe_from_rows empty schema with non-empty rows** — When schema is empty but rows are not (e.g. Sparkless/PyO3 sends empty schema), the crate now infers schema from the rows (column names `c0`, `c1`, … and types from first non-null per column). PySpark parity. Fixes #624.
- **#625 – create_dataframe_from_rows array column (list/object)** — Array columns now accept JSON array, Object with `"0"`, `"1"`, … keys (Python list serialization), or string that parses as JSON array. PySpark accepts Python lists for array columns. Fixes #625.

## [0.12.0] - 2026-02-19

### Added

- **Embedding / bindings helpers** — Expanded surface so downstream bindings (PyO3, Node, CLI) can depend only on robin-sparkless without Polars types in the public API.
  - **Schema:** `DataType` re-exported from crate root and prelude. `DataFrame::get_column_data_type(name)` returns `Option<schema::DataType>` (Polars-free). `StructType::to_json()` / `to_json_pretty()` serialize schema to JSON. `schema_from_json(json)` parses a schema from a JSON string (e.g. from host).
  - **EngineError API:** New `*_engine()` methods that return `Result<T, EngineError>` instead of `PolarsError`: on **DataFrame** — `schema_engine`, `columns_engine`, `count_engine`, `select_engine`, `filter_engine`, `with_column_engine`, `group_by_engine`, `limit_engine`, `collect_as_json_rows_engine`; on **SparkSession** — `create_dataframe_engine`, `create_dataframe_from_rows_engine`, `read_csv_engine`, `read_parquet_engine`, `read_json_engine`, `table_engine`. Documented in `docs/EMBEDDING.md` as the recommended embedding API.
  - **prelude::embed:** Now re-exports `StructType`, `StructField`, `DataType`, `SparklessConfig`, `EngineError`, `DataFrameReader`, and `lit_f64`, `lit_i32` in addition to existing types and functions.
  - **Traits:** `IntoRobinDf` implemented for `Vec<(i64, String)>` and `Vec<(i64, i64, i64, String)>` (default column names `c0`, `c1`, …). `FromRobinDf` implemented for `Vec<Vec<JsonValue>>` (rows as arrays in column order). `FromRobinDf` for `Vec<HashMap<String, JsonValue>>` now uses `collect_as_json_rows_engine`.
- **cargo-deny** — Allowed transitive git sources in `deny.toml` (RustCrypto traits/block-ciphers, dropbox rust-alloc-no-stdlib) so `cargo deny check` reports sources ok without warnings.

### Fixed

- **SQL (sql feature)** — Removed unused imports `Series` and `IntoSeries` in `src/sql/translator.rs`, eliminating compiler warnings under `--all-features`.

## [0.11.12] - 2026-02-19

### Fixed

- **#610 – create_dataframe_from_rows struct (object or array)** — Struct columns accept values that are JSON strings parsing to object or array (e.g. Python tuple as `"[1, \"y\"]"`). Fixes #610.
- **#611 – create_dataframe_from_rows array column** — Array columns accept a single non-array value as a one-element list (PySpark parity). Fixes #611.
- **#612 – Filter predicate string vs numeric coercion** — Plan expressions: when one side of a comparison is a literal and the other is a column, assume column is String so string–numeric coercion is applied. Fixes #612.
- **#613 – unionByName type coercion** — `find_common_type` fallback for string vs other; when one side’s dtype is unknown, use String so string→int cast is never attempted. Fixes #613.
- **#614 – Join column resolution / case sensitivity** — After aliasing right key to left name (e.g. ID → id), drop right’s original key column so the result has only the left key name and collect does not fail with "not found: ID". Fixes #614.
- **#615 – Date vs datetime comparison** — When comparing Date with Datetime, cast Date to Datetime so both sides are comparable (PySpark: date as start-of-day). Fixes #615.

## [0.11.10] - 2026-02-18

### Fixed

- **SQL translator (sql feature)** — GROUP BY expression collection now compiles correctly: collect into `Vec<(Expr, String)>` then unzip to `(Vec<Expr>, Vec<String>)`.

### Changed

- **Makefile** — `check-full` / `check` now runs `cargo build --all-features` so feature-gated code (sql, delta) is compiled and type errors are caught.

## [0.11.9] - 2026-02-18

### Fixed

- **#590 – WHERE LIKE and IN (PySpark parity)** — SQL WHERE clause now supports `LIKE` (and `NOT LIKE`) with string-literal pattern and `IN` / `NOT IN` with literal lists. Fixes #590.
- **#589 – HAVING arbitrary aggregates (PySpark parity)** — HAVING clause now accepts arbitrary aggregate expressions (e.g. `HAVING AVG(salary) > 55000`). Aggregate calls in HAVING are added to the aggregation and substituted with result columns. Fixes #589.
- **#588 – GROUP BY expression (PySpark parity)** — GROUP BY now accepts expressions (e.g. `GROUP BY (age > 30)`) in addition to column names. Fixes #588.
- **#587 – SQL scalar aggregation (PySpark parity)** — `SELECT AVG(salary) FROM t` (no GROUP BY) now works; projection of only aggregate functions is executed as scalar aggregation. Fixes #587.
- **#583 – concat and contains as expression ops (PySpark parity)** — Plan interpreter now accepts `concat` and `contains` when sent as op with args (e.g. `F.concat(a, b)`, `F.col("name").contains("lic")`). Fixes #583.
- **#582 – regexp_extract literal pattern (PySpark parity)** — Plan interpreter now accepts bare string and number literals for regexp_extract args (pattern and group index), not only `{"lit": ...}`. Fixes #582.

## [0.11.8] - 2026-02-18

### Added

- **#438 – Lazy backend (PySpark parity)** — DataFrame now uses Polars `LazyFrame` internally. Transformations (filter, select, with_column, order_by, join, union, etc.) extend the lazy plan; only actions (collect, show, count, write, stats) trigger materialization. Data sources (read_csv, read_parquet, read_json) return lazy DataFrames. Enables Polars query optimization across the full pipeline and aligns with PySpark lazy-by-default semantics. Fixes #438.
- **#399 – Cast numeric (int/float) to boolean (PySpark parity)** — `cast` and `try_cast` now support casting numeric types (Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64) to boolean. Semantics: 0/0.0 → false, non-zero → true. Fixes #399.
- **#400 – Window.orderBy accept F.desc/F.asc (PySpark parity)** — `Window.orderBy()` now accepts SortOrder from `desc(col)` and `asc(col)`, including `desc("v")` and `asc("v")` with column name strings. Enables `Window.partitionBy("k").orderBy(desc("v"))`. Fixes #400.
- **#401 – filter/where accept Column expression (PySpark parity)** — `filter` and `where` now accept any Column expression (e.g. `col("x") > 1`) and objects with `_robin_column` (Sparkless compat). Fixes #401.
- **#366 – Comparison coercion string/numeric and date/string (column-to-column)** — `df.filter(col("id") == col("label"))` when `id` is numeric and `label` is string now coerces (string parsed via `try_to_number`) instead of raising "cannot compare string with numeric type". Column-to-column comparisons in filter now use `coerce_for_pyspark_comparison` when both sides are columns with differing types (string–numeric, date/datetime–string). Fixes #366.
- **#365 – regexp_extract support lookahead/lookbehind (PySpark parity)** — Patterns with `(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)` are now supported via the `fancy-regex` crate. Polars’ built-in regex does not support lookaround; when detected, extraction uses a UDF. Enables `df.select(rs.regexp_extract(rs.col("s"), r"(?<=hello )\w+", 0))` → `"world"`. Fixes #365.
- **#363 – struct() accept multiple Column arguments (PySpark parity)** — `struct(*cols)` now accepts variadic Column arguments like PySpark `F.struct(col1, col2, ...)`. Previously only a single list argument was accepted. Enables `df.select(rs.struct(rs.col("a"), rs.col("b")).alias("s")).collect()`. Fixes #363.
- **#364 – Row/result use column alias as key (PySpark parity)** — `collect()` Row objects use the column alias as the key when present (e.g. `df.select(rs.lit(42).alias("map_col")).collect()` → `rows[0]["map_col"] == 42`). Regression test added. Fixes #364.
- **#362 – SQL: support DROP TABLE / DROP VIEW (PySpark parity)** — `spark.sql("DROP TABLE IF EXISTS my_schema.my_table")` and `DROP TABLE name` / `DROP VIEW name` are now supported. Removes the table/view from the session catalog (temp views and saved tables). Qualified name `global_temp.xyz` drops from the global temp view catalog. Returns empty DataFrame. Fixes #362.
- **#361 – dropDuplicates(subset=[...]) (PySpark parity)** — `DataFrame.drop_duplicates(subset=None)` and `dropDuplicates(subset=None)` are now supported; both delegate to `distinct(subset=...)`. When subset is provided, one row per distinct key in those columns is kept. Fixes #361.
- **#360 – DataFrame.na.replace() (PySpark parity)** — `df.na.replace(to_replace, value, subset=None)` replaces values in columns. Supports scalar to_replace and value (None, int, float, bool, str). When subset is provided, only those columns are updated; otherwise all columns. Fixes #360.
- **#359 – GroupedData.pivot() for pivot tables (PySpark parity)** — `GroupedData.pivot(pivot_col, values=None)` returns `PivotedGroupedData` with `.sum(column)`, `.avg(column)`, `.min(column)`, `.max(column)`, and `.count()`. Pivot column can be any type (column names use string representation); null in pivot column becomes column `"null"`. When `values` is provided, column order follows the list; otherwise pivot values are sorted for deterministic order. No matching rows for a pivot value yields null (not 0). Fixes #359.
- **#357 – Window constructor and partitionBy/orderBy accept string column names (PySpark parity)** — `Window()` no-arg constructor now creates an unbounded window so `Window().partitionBy("dept").orderBy("salary")` works. `partitionBy` and `orderBy` already accepted str or Column; documented. Enables PySpark-style `w = Window.partitionBy("col").orderBy("col")` with strings. Fixes #357.
- **#358 – GroupedData.avg(), sum(), min(), max() accept Column expression (PySpark parity)** — `GroupedData.avg(*cols)`, `sum(column)`, `min(column)`, and `max(column)` now accept Column expressions (e.g. `col("v")`) in addition to column name strings. Enables `df.group_by("k").avg(rs.col("v"))` and `df.group_by("k").sum(rs.col("v"))`. Fixes #358.
- **#356 – Column getItem, getField, and subscript (PySpark parity)** — `Column` now exposes `.getItem(i)` for 0-based array index, `.getField(name)` for struct field, and `col[i]` / `col["name"]` via `__getitem__`. Fixes #356.
- **#355 – Column.eqNullSafe (PySpark parity)** — `Column` now exposes `.eqNullSafe(other)` as a PySpark-style alias for `.eq_null_safe(other)` (null-safe equality: true if both null or both equal). Fixes #355.
- **#354 – Column `.name` attribute (PySpark parity)** — `Column` now exposes a `.name` property for simple column references and aliased expressions (e.g. `col("salary").name` → `"salary"`, `col("x").alias("y").name` → `"y"`). Fixes #354.
- **#352 – group_by and order_by accept Column expressions (PySpark parity)** — `DataFrame.group_by(cols)` now accepts a single column name (str), a single Column (e.g. `group_by(col("dept"))`), or a list/tuple of str or Column. `DataFrame.order_by(cols)` now accepts a single Column or list of Column (treated as ascending), in addition to existing str, SortOrder, and list of SortOrder. Enables PySpark-style `df.groupBy(F.col("dept")).agg(F.sum(F.col("salary")))` and `df.orderBy(col("x"))`. Fixes #352.
- **#353 – join on accept Column expression(s) (PySpark parity)** — `DataFrame.join(other, on=...)` now accepts `on` as a single Column (e.g. `join(right, col("id"))`) or list/tuple of str or Column, in addition to existing str or list/tuple of str. Enables PySpark-style `left.join(right, F.col("id"))`. Fixes #353.

### Fixed

- **#560 – first() and orderBy semantics (PySpark parity)** — Plan interpreter supports `first` and `last` in groupBy aggs; orderBy after groupBy orders the aggregated result. Fixes #560.
- **#559 – unionByName diamond duplicate rows (PySpark parity)** — unionByName does not deduplicate; (A unionByName B) unionByName A correctly yields A’s rows twice and B’s once. Regression test added. Fixes #559.
- **#558 – Join type coercion (PySpark parity)** — Join already coerces key columns to a common type when left/right dtypes differ (e.g. string vs long); regression test added. Fixes #558.
- **#557 – substr/substring semantics (PySpark parity)** — `substr`/`substring` now use 1-based start; negative start counts from end (e.g. -3 = last 3 chars); length less than 1 returns empty string. Fixes #557.
- **#556 – Reverse-operator arithmetic (PySpark parity)** — Plan interpreter now accepts `{"op": "sub"|"minus"|"-", "left": <expr>, "right": <expr>}` and `{"op": "mul"|"*", "left": <expr>, "right": <expr>}` so literal-on-left expressions like `(1 - col("x"))` and `(100 * col("x"))` work. Fixes #556.
- **#555 – Case-insensitive column resolution in plan (PySpark parity)** — execute_plan uses case-insensitive column resolution by default; `col("age")` in select resolves to schema column `"Age"`. Regression test added. Fixes #555.
- **#554 – Array column and explode (PySpark parity)** — Plan interpreter now accepts `{"op": "explode"|"explode_outer"|"explodeOuter", "args": [<expr>]}` so Sparkless plans using explode as expression op no longer report "unsupported expression op: explode". Fixes #554.
- **#553 – Empty DataFrame schema and table ops (PySpark parity)** — Empty DataFrame with explicit schema is supported: `create_dataframe_from_rows([], schema)`, `saveAsTable`, and `spark.table()` work (PySpark raises "can not infer schema from empty dataset"). Regression test added. Fixes #553.
- **#552 – Inner/left join return 0 rows (PySpark parity)** — Plan interpreter now parses join `on` as array of column refs or eq expressions (Sparkless v4 format) and aligns right join key column names to left’s so inner/left join return correct row counts. Fixes #552.
- **#551 – Union type coercion (PySpark parity)** — `union` now coerces columns to a common type when left and right have different dtypes (e.g. String vs Int64 → String). Fixes #551.
- **#550 – Window function approx_count_distinct (PySpark parity)** — Plan interpreter now supports `approx_count_distinct(col).over(window)` in window expressions. Fixes #550.
- **#549 – format_string and log() as expression op (PySpark parity)** — Plan interpreter now accepts `{"op": "format_string"|"formatString"|"log", "args": [...]}`. Fixes #549.
- **#548 – date_trunc and to_date as expression op (PySpark parity)** — Plan interpreter now accepts `{"op": "date_trunc"|"dateTrunc"|"to_date"|"toDate", "args": [...]}`. Fixes #548.
- **#547 – String functions as expression op (PySpark parity)** — Plan interpreter now accepts translate, substring_index, levenshtein, soundex, crc32, xxhash64, get_json_object, json_tuple, and regexp_extract_all when sent as `{"op": "<name>", "args": [...]}` (and camelCase variants substringIndex, getJsonObject, jsonTuple, regexpExtractAll). Also added get_json_object and json_tuple to expr_from_fn. Fixes #547.
- **#546 – SQL alias in aggregation SELECT (PySpark parity)** — SQL now supports aliased aggregates (e.g. `SELECT grp, COUNT(v) AS cnt FROM t GROUP BY grp`). `ExprWithAlias` in the SELECT list is handled for group columns (validation) and aggregate functions (alias used as output column name). Fixes #546.
- **#545 – UDF expression not supported (PySpark parity)** — Plan interpreter now accepts expression `{"op": "udf", "udf"|"name": "name", "args": [<expr>, ...]}`, so Sparkless plans that use UDF expressions no longer report "unsupported expression op: udf". Fixes #545.
- **#543 – CSV inferSchema behavior (PySpark parity)** — read_csv() and read().option("inferSchema", true).csv() infer integer, double, and boolean from CSV. Option inferSchema=false now disables inference (0 rows). Regression tests verify inferred types. Fixes #543.
- **#542 – create_map semantics / support (PySpark parity)** — Plan interpreter accepts `createMap` (camelCase) in addition to `create_map` for op and fn, so Sparkless create_map expressions are supported. Fixes #542.
- **#541 – withField struct update (PySpark parity)** — Plan interpreter supports `withField` and `with_field` in expressions (args: struct column, field name, value). Also added `get_field` / `getField` for struct field extraction in plans. Fixes #541.
- **#540 – orderBy nulls_first / nulls_last (PySpark parity)** — `order_by(refs, ascending)` now passes null placement to Polars: ASC → nulls first, DESC → nulls last. Plan interpreter `orderBy` accepts optional `nulls_last` array in payload for configurable null ordering. Fixes #540.
- **GroupedData / PivotedGroupedData column resolution** — Aggregation methods (`sum`, `avg`, `min`, `max`, `first`, `last`, etc.) and pivot operations now resolve column names against the schema with case sensitivity. When `case_sensitive` is false, `grouped.sum("V")` works when the schema has `"v"`.
- **subtract / intersect with different column casing** — `subtract` and `intersect` now align right-side column names to left when casing differs (case-sensitive or -insensitive), so set operations work when DataFrames have different column casing.
- **#492 – case-insensitive orderBy on mixed-case column names (PySpark parity)** — Clarify and regression-test that when `spark.sql.caseSensitive` is false (default), `DataFrame::order_by` / `orderBy` resolve column names case-insensitively so `"value"` and `"VALUE"` work for schema column `"Value"`. Fixes #492.

## [0.11.11] - 2026-02-19

### Added

- **#600 – create_dataframe_from_rows struct as object or array (PySpark parity)** — Struct columns accept JSON object (by field name) or array (by position). Clearer error message: "struct value must be object (by field name) or array (by position)". Tests: `test_create_dataframe_from_rows_struct_as_object`, `test_create_dataframe_from_rows_struct_as_array`. Fixes #600.
- **#601 – create_dataframe_from_rows array column (PySpark parity)** — Array columns accept JSON array or string that parses as JSON array (e.g. `"[1,2,3]"`). Error message: "array column value must be null or array (or string that parses as JSON array)". Tests: `test_create_dataframe_from_rows_array_column`, `test_issue_601_array_column_pyspark_parity`. Fixes #601.
- **#602 – filter string column eq numeric literal (PySpark parity)** — Test `issue_602_filter_string_column_eq_numeric_literal` ensures `df.filter(col("str_col") == 123)` succeeds with coercion. Fixes #602.
- **#603 – unionByName type coercion** — `union_by_name` coerces left/right columns to a common type per column when types differ (e.g. Int64 vs String). Test: `union_by_name_coerces_different_column_types`. Fixes #603.
- **#604 – join column resolution (case-insensitive key names)** — Join resolves keys on left and right to physical column names; aliases right key columns to left key names so result has one key name and `col("ID")`/`col("id")` both resolve. Test: `join_column_resolution_case_insensitive`. Fixes #604.

## [0.11.7] - 2026-02-18

### Added

- **#521 – Window functions in plan execution** — Plan interpreter supports `rank`, `dense_rank`, `percent_rank`, `ntile`, `lag`, `lead`, `sum`, and `avg` in the same fn+window path as `row_number`. Enables Sparkless plans with these window functions. Fixes #521.
- **#517 – Window expressions fn/function key** — Plan interpreter accepts `"function"` as alias for `"fn"` in window expressions (Sparkless may send either). Empty `args` allowed when `window` is present. Fixes #517.
- **#522 – get_item in plan execution** — Plan expression interpreter supports `get_item` fn: `[col, index_or_key]`; integer literal → array index, otherwise map key. Fixes #522.
- **#527 – struct/named_struct in plan execution** — Plan interpreter supports `struct` (args = column exprs) and `named_struct` (alternating name literal + value). Fixes #527.
- **#526 – SQL DROP SCHEMA / DROP DATABASE** — Parser and translator support `DROP SCHEMA` / `DROP DATABASE`; session exposes `drop_database`. Fixes #526.

### Fixed

- **#520 – row_number() with empty partition_by/order_by** — Window spec allows empty `order_by` and `partition_by` (single partition, no ordering; PySpark parity). Fixes #520.
- **#523 – regexp_extract non-literal pattern/idx** — Clearer errors when pattern or group index are not literals (column refs not supported). Fixes #523.
- **#528 – regexp_replace op form** — Plan interpreter accepts op form `regexp_replace` (left/pattern/replacement or args array) in addition to fn form. Fixes #528.
- **#524 – Case sensitivity in plan execution** — Plan execution always uses case-insensitive column resolution so `col("ID")` resolves when schema has `"id"`. Fixes #524.
- **#525 – isin mixed types** — isin value list is produced as string series so string column with numeric literals (e.g. `col("s").isin(1, 2)`) works. Fixes #525.
- **#519 – Empty DataFrame + parquet saveAsTable** — Documented that empty rows + non-empty schema yields an empty DataFrame with that schema for `write.format("parquet").saveAsTable()` and append. Fixes #519.

## [0.11.6] - 2026-02-17

### Fixed

- **#516 – Right, outer, semi, anti join row count** — DataFrame join tests added for right_join, left_semi_join, left_anti_join. All join types (Inner, Left, Right, Outer, LeftSemi, LeftAnti) now verified. Fixes #516.
- **#517 – Window expressions Sparkless format** — Plan interpreter `expr_from_row_number_window` now accepts `order_by` and `partition_by` items as `{"col": "name"}` objects (in addition to string column names). Sparkless format `{"fn": "row_number", "args": [], "window": {"partition_by": ["x"], "order_by": [{"col": "y", "asc": true}]}}` is supported. Fixes #517.
- **#518 – isin with empty list** — `col.isin([])` and `isin(col)` with no values now return `lit(false)` (0 rows match) instead of relying on empty-series semantics. Fixes #518.
- **#519 – Empty DataFrame + parquet saveAsTable** — Regression test `test_write_parquet_empty_df_with_schema` added. Empty DataFrame with explicit schema can be written via `write.format("parquet").save(path)` without schema inference. Robin-sparkless uses explicit schema; PySpark fails with "can not infer schema from empty dataset". Fixes #519.

## [0.11.5] - 2026-02-17

### Fixed

- **#512 – create_map() with zero arguments (PySpark parity)** — Plan interpreter `create_map` fn now accepts empty args `[]`; returns empty map `{}` per row. PySpark `F.create_map()` with no args is valid. Fixes #512.
- **#513 – Inner join returns 0 rows (Sparkless plan format)** — Plan interpreter now accepts join `on` as single string (converted to array); schema fields as `fieldName`/`dataType`; `other_data` rows as dicts `[{"col":v},...]` in addition to arrays; and `on` at op level. Fixes #513.

## [0.11.4] - 2026-02-17

### Fixed

- **#510 – Join and union return wrong row counts (Sparkless plan format)** — Plan interpreter now accepts camelCase keys (`otherData`, `otherSchema`) and op-level placement of `other_data`/`other_schema` (sibling to `payload`) for join, union, and unionByName. Fixes incorrect row counts when Sparkless v4 sends plans in these formats. Fixes #510.

## [0.11.3] - 2026-02-17

### Added

- **Column.try_with_field()** — Add `Column::try_with_field(name, col) -> Result<Column, PolarsError>` as a non-panicking variant of `Column::with_field` when the target column is not a struct.

### Fixed

- **#503 – select() with column names (Sparkless plan compatibility)** — Plan interpreter `select` now accepts `payload: {"columns": [...]}` (Sparkless format) and column refs as `{"type": "column", "name": "id"}` objects in addition to the documented array-of-strings format. Fixes #503.
- **Timestamp/date-only parsing robustness** — When parsing timestamp/datetime values from JSON rows, date-only strings (`"YYYY-MM-DD"`) no longer rely on `expect()` for midnight conversion; invalid values now fail that cell parse instead of panicking.

## [0.11.2] - 2026-02-17

### Added

- **#497 – between, power (**), cast in logical plan (PySpark parity)** — Plan expression interpreter supports `between(left, lower, upper)` and binary op `**` / `pow` for filter/withColumn; cast already supported as fn. Plan fixture `between_power_cast.json`. Fixes #497.
- **#496 – groupBy + agg alias in logical plan** — `parse_aggs` accepts optional `alias` per agg so PySpark-style names (e.g. `sum(v)`, `count(v)`) work. Plan fixture `groupby_agg_sum_count.json`. Fixes #496.
- **#498 – row_number() over (partition by col) in logical plan** — Plan supports `{"fn": "row_number", "window": {"partition_by": ["col", ...]}}` in withColumn/select. Plan fixture `window_row_number.json`. Fixes #498.
- **#495 – empty DataFrame with schema + parquet table append** — Regression tests for empty df with schema, saveAsTable(Overwrite), then append one row (in-memory and warehouse paths). Fixes #495.

## [0.11.1] - 2026-02-17

### Changed

- **#176 – QUICKSTART: regexp_extract_all with select expressions** — QUICKSTART.md now documents using `regexp_extract_all` with `DataFrame::select_exprs` for select-with-expressions workflows. Fixes #176.

## [0.11.0] - 2026-02-17

### Changed

- **DDL schema parsing** — Replace hand-rolled DDL parser with [spark-ddl-parser](https://crates.io/crates/spark-ddl-parser) crate. `createDataFrame(data, schema="...")` now uses the crate for full nested struct/array/map/decimal support in DDL strings. Added `"integer"` as alias for `"int"` in `create_dataframe_from_rows` (spark-ddl-parser canonical form).

## [0.10.0] - 2026-02-15

### Added

- **#372 – PySpark-style createDataFrame(data, schema=None) (Python)** — `spark.createDataFrame(data, schema=None)` is now the primary Python API. Accepts `data` as list of dicts (schema inferred from first row keys and types from first non-null per column), list of tuples with column names (e.g. `createDataFrame([("Alice", 25)], ["name", "age"])`), or explicit `schema` as list of `(name, dtype_str)` or StructType-like (`.fields` with `.name` and `.dataType.typeName()`). Empty data with `schema=None` returns empty DataFrame; with column names or full schema returns empty DataFrame with those columns. Fixes #372.

### Changed

- **Documentation** — README, README-Python, USER_GUIDE, UDF_GUIDE, QUICKSTART, PERSISTENCE_GUIDE, PYTHON_API, PYSPARK_DIFFERENCES, ROADMAP, READINESS_FOR_SPARKLESS_PLAN, SPARKLESS_PARITY_STATUS and related docs now use and recommend `createDataFrame` for Python examples and parity text; Rust examples keep `create_dataframe` / `create_dataframe_from_rows`.
- **Python tests** — All Python tests converted from `create_dataframe` / `_create_dataframe_from_rows` to `createDataFrame`.
- **Rust** — `infer_schema_from_json_rows` (session): fix clippy `needless_range_loop` by iterating with `schema.iter_mut().enumerate()`.
- **Tests** — `test_issue_372_create_data_frame.py`: remove unused `pytest` import; use `df.columns()` instead of `print_schema()` for empty-schema assertion (mypy).
- **Documentation** — Fix USER_GUIDE Rust filter example (use `.into_expr()` for filter API). Add `make run-examples` to run Rust examples + Python doc examples with real output. Add `scripts/run_doc_examples.py` for doc example verification.

## [0.9.2] - 2026-02-14

### Added

- **#347 – SQL support for CREATE SCHEMA / CREATE DATABASE (DDL)** — SQL parser and translator now support `CREATE SCHEMA name` and `CREATE DATABASE name`. Executing these DDL statements registers the schema/database name in session state. `SparkSession` exposes `databases` (catalog), `register_database()`, `list_database_names()`, and `database_exists()`. Python catalog `list_databases()` and `database_exists()` use this session state. Fixes #347.

### Changed

- **Makefile** — `check-full` now runs `fmt` and `fmt-python` first; new `fmt-python` target formats Python code with ruff.

## [0.9.1] - 2026-02-13

### Changed

- Bugs and improvements audit: clippy/docs/parity fixes, documented TODOs, high-risk unwrap in session replaced, skip/expect_error fixtures documented in parity harness, new `filter_empty_result` fixture and parity support for empty expected rows.

## [0.9.0] - 2026-02-13

### Added

- **#284 – SparkSession.sql() and SparkSession.table() always exposed (PySpark parity)** — `spark.sql(query)` and `spark.table(name)` are now always present on the Python SparkSession. When built with the `sql` feature they work as before. When built without it they raise a clear `RuntimeError` instead of `AttributeError`, so Sparkless and PySpark-migrating code can call these methods without attribute checks. Fixes #284.
- **#285 – DataFrame.createOrReplaceTempView() and temp view methods always exposed (PySpark parity)** — `df.createOrReplaceTempView(name)`, `createTempView`, `createGlobalTempView`, and `createOrReplaceGlobalTempView` are now always present on the Python DataFrame. When built with the `sql` feature they register views and `spark.table(name)` / `spark.sql("SELECT ... FROM name")` resolve them. When built without it they raise a clear `RuntimeError` instead of `AttributeError`. Fixes #285.
- **#286 – getActiveSession / session registry for aggregate functions (PySpark parity)** — The parameterless `SparkSession()` constructor now registers the session as the active/default session (same as `get_or_create()`), so `get_active_session()` and `get_default_session()` return it and aggregate functions (e.g. `df.agg(F.sum(col("x")))`) work without requiring a separate registration step. Fixes #286.
- **#287 – DataFrame.agg(*exprs) for global aggregation (PySpark parity)** — `DataFrame.agg(expr)` and `DataFrame.agg([expr1, expr2, ...])` (or a tuple of expressions) now perform global aggregation over the whole DataFrame and return a single-row DataFrame (e.g. `df.agg([sum(col("x")), avg(col("y"))])`). Fixes #287.
- **#288 – Window.partitionBy() / orderBy() accept column names (str) (PySpark parity)** — `Window.partitionBy("col1", "col2")` and `Window.orderBy("col")` now accept column names (strings) as well as Column expressions, so `Window.partitionBy("dept").orderBy("salary")` works without wrapping in `col()`. Fixes #288.
- **#289 – DataFrame.na.drop() / na.fill() with subset, how, thresh (PySpark parity)** — `df.na().drop(subset=..., how=..., thresh=...)` and `df.na().fill(value, subset=...)` now match PySpark: `drop` accepts `subset` (columns to consider), `how` (`"any"` drop if any null, `"all"` drop only if all null), and `thresh` (keep row if at least that many non-null in subset). `fill` accepts scalar or Column as `value` and optional `subset` (columns to fill). Direct `df.dropna(subset=..., how=..., thresh=...)` and `df.fillna(value, subset=...)` updated accordingly. Fixes #289.
- **#290 – DataFrame.fillna(value, subset=[...]) (PySpark parity)** — Direct `df.fillna(value, subset=[...])` is supported (same API as #289); fills only the listed columns. Fixes #290.
- **#291 – create_dataframe_from_rows: allow empty data with schema or empty schema (PySpark parity)** — `create_dataframe_from_rows([], schema)` returns an empty DataFrame with the given column names and types. `create_dataframe_from_rows([], [])` returns an empty DataFrame with no columns. Fixes #291.
- **#292 – union_by_name(allow_missing_columns=True) (PySpark parity)** — `df.union_by_name(other, allow_missing_columns=True)` (default True) fills columns missing in the other DataFrame with null. When False, errors if the other DataFrame is missing any column from this one. Fixes #292.
- **#319 – lag(col, offset) / lead(col, offset) as module-level (PySpark parity)** — `lag(column, offset=1)` and `lead(column, offset=1)` are now exposed at module level; use with `.over(partition_by)` (e.g. `rs.lag(rs.col("v"), 1).over(["dept"])`). Fixes #319.
- **#320 – dense_rank() window function (PySpark parity)** — `dense_rank(descending=False)` returns a window function; use with `.over(window)` (e.g. `rs.dense_rank().over(win)`). Fixes #320.
- **#313 – hour(col) (PySpark parity)** — Module-level `hour(column)` extracts the hour (0–23) from a date or timestamp column. Fixes #313.
- **#315 – last_day(col) (PySpark parity)** — Module-level `last_day(column)` returns the last day of the month for a date/timestamp column. Fixes #315.
- **#322 – to_date(col [, format]) (PySpark parity)** — Module-level `to_date(column, format=None)` casts or parses to date; with `format` parses string column using PySpark-style format. Fixes #322.
- **#317 – element_at(col, index) (PySpark parity)** — Module-level `element_at(column, index)` (1-based index) for array/map columns. Fixes #317.
- **#316 – array_remove(col, element) (PySpark parity)** — Module-level `array_remove(column, value)` removes all elements equal to value from the array. Fixes #316.
- **#307 – encode(col, charset) / decode(col, charset) (PySpark parity)** — Module-level `encode(column, charset)` and `decode(column, charset)` for string/binary (e.g. UTF-8, hex). Fixes #307.
- **#293 – first() / first_ignore_nulls() aggregate (PySpark parity)** — `first(column, ignorenulls=True)` aggregation for use in `groupBy.agg()` (e.g. `df.group_by("k").agg([rs.first(rs.col("v"))])`). Fixes #293.
- **#301 – any_value(expr [, isIgnoreNull]) (PySpark parity)** — Module-level `any_value(column, ignorenulls=True)` for use in `groupBy.agg()`. Fixes #301.
- **#302 – count_if(expr) (PySpark parity)** — Module-level `count_if(column)` (count where condition true) for use in `groupBy.agg()`. Fixes #302.
- **#297 – approx_count_distinct(column [, rsd]) (PySpark parity)** — Module-level `approx_count_distinct(column, rsd=None)` for use in `groupBy.agg()` (Polars uses exact n_unique; rsd reserved for API). Fixes #297.
- **#300 – approx_percentile(col, percentage [, accuracy]) (PySpark parity)** — Module-level `approx_percentile(column, percentage, accuracy=None)` for use in `groupBy.agg()` (maps to quantile; accuracy reserved for API). Fixes #300.
- **#303 – max_by(value_col, ord_col) / min_by(value_col, ord_col) (PySpark parity)** — Module-level `max_by(value_column, ord_column)` and `min_by(value_column, ord_column)` for use in `groupBy.agg()` (value at row where ord is max/min). Fixes #303.
- **#304 – try_sum(column) / try_avg(column) (PySpark parity)** — Module-level `try_sum(column)` and `try_avg(column)` for use in `groupBy.agg()` (maps to sum/mean; null-on-overflow reserved for API). Fixes #304.
- **#309 – collect_list(column) (PySpark parity)** — Module-level `collect_list(column)` for use in `groupBy.agg()` (collect values into list per group). Fixes #309.
- **#310 – collect_set(column) (PySpark parity)** — Module-level `collect_set(column)` for use in `groupBy.agg()` (collect distinct values into list per group). Fixes #310.
- **#314 – bool_and(column) / every(column) (PySpark parity)** — Module-level `bool_and(column)` and `every(column)` for use in `groupBy.agg()` (boolean AND across group). Fixes #314.
- **#311 – corr(col1, col2) (PySpark parity)** — Module-level `corr(column1, column2)` for use in `groupBy.agg()` (Pearson correlation). Fixes #311.
- **#312 – covar_pop(col1, col2) (PySpark parity)** — Module-level `covar_pop(column1, column2)` for use in `groupBy.agg()` (population covariance). Fixes #312.
- **#321 – skewness(col) / kurtosis(col) (PySpark parity)** — Module-level `skewness(column)` and `kurtosis(column)` for use in `groupBy.agg()`. Fixes #321.
- **#305 – explode_outer(column) (PySpark parity)** — Module-level `explode_outer(column)` for list/map columns; null/empty list yields one row with null in the exploded column (Polars default behavior). Fixes #305.
- **#306 – inline(column) / inline_outer(column) array of structs (PySpark parity)** — Module-level `inline(column)` and `inline_outer(column)` for array-of-structs columns; explode then use unnest to expand struct fields to columns. Fixes #306.
- **#318 – flatten(column) array-of-arrays (PySpark parity)** — Module-level `flatten(column)` for list-of-lists columns; flattens to one list per row. Fixes #318.

### Documentation

- **README and docs** — Rust quick start and QUICKSTART Basic Usage use `.into_expr()` so filter/col examples compile. Added runnable examples (`examples/demo.rs`, `examples/quickstart_from_polars.rs`) and real output blocks to QUICKSTART, USER_GUIDE, UDF_GUIDE, and PERSISTENCE_GUIDE.

## [0.8.5] - 2026-02-13

### Added

- **#272 – round() on string column strips whitespace (PySpark parity)** — `round()` on string columns now trims leading/trailing whitespace before parsing to double, so values like `"  10.6  "` and `"\t20.7"` round to 11.0 and 21.0 instead of returning null. Fixes #272.
- **#273 – to_timestamp() on string column (PySpark parity)** — `to_timestamp(col("ts_str"))` without format now parses common string timestamps (e.g. `"2024-01-01 10:00:00"`) using default format `%Y-%m-%d %H:%M:%S` instead of raising `RuntimeError`. With format, PySpark-style patterns with single-quoted literals (e.g. `"yyyy-MM-dd'T'HH:mm:ss"`) are supported: quoted segments like `'T'` are unquoted before conversion to strftime, so `"2024-01-01T10:00:00"` parses correctly. String values are trimmed before parsing. Fixes #273.
- **#274 – join key type coercion (PySpark parity)** — Join on columns with different types (e.g. str on left, int on right) now coerces both sides to a common type (via `find_common_type`) instead of raising `RuntimeError: datatypes of join keys don't match`. Fixes #274.
- **#275 – create_map() with no arguments (PySpark parity)** — `create_map()` and `create_map([])` now return a column of empty maps (one `{}` per row) instead of raising `TypeError: py_create_map() missing 1 required positional argument: 'cols'`. Python binding accepts `*cols` so zero arguments is valid. Fixes #275.
- **#276 – between() with string column and numeric bounds (PySpark parity)** — `col("col").between(1, 20)` when `col` is string now coerces (string parsed to number for comparison) instead of raising `RuntimeError: cannot compare string with numeric type`. Coercion is applied in `with_column` as well as in `filter`. Fixes #276.
- **#280 – posexplode() accepts column name (str) (PySpark parity)** — `posexplode("Values")` now works in addition to `posexplode(F.col("Values"))`; previously raised `TypeError: 'str' object cannot be converted to 'Column'`. Fixes #280.

## [0.8.4] - 2026-02-13

### Added

- **#262 – F.round() on string column (PySpark parity)** — `round()` now accepts string columns that contain numeric values; values are implicitly cast to double then rounded (PySpark behavior). Previously raised `RuntimeError: round can only be used on numeric types`. Fixes #262.
- **#263 – F.array() with no args (PySpark parity)** — `array()` with no arguments now returns a column of empty arrays (one `[]` per row). Previously raised `RuntimeError: array requires at least one column`. Fixes #263.
- **#264 – F.posexplode() and F.explode() in Python module (PySpark parity)** — Module-level `posexplode(column)` and `explode(column)` are now exposed; previously `F.posexplode` raised `AttributeError`. `posexplode` returns `(pos_column, value_column)`. Fixes #264.
- **#265 – date/datetime vs string comparison (PySpark parity)** — Comparing a date or datetime column to a string literal in `filter` (e.g. `df.filter(col("dt") == "2025-01-01")`) now implicitly casts the string to the column type instead of raising `RuntimeError: cannot compare 'date/datetime/time' to a string value`. Uses `try_cast` so invalid strings become null (non-matching). Fixes #265.
- **#266 – eq_null_safe type coercion (PySpark parity)** — `eq_null_safe` (eqNullSafe) now applies the same string–numeric coercion as regular comparisons: string column vs int literal is coerced via `try_to_number` so that e.g. `df.select(col("str_col").eq_null_safe(lit(123)))` returns True/False per row instead of raising `RuntimeError: cannot compare string with numeric type (i32)`. Fixes #266.

## [0.8.3] - 2026-02-13

### Added

- **#260 – lit(None).cast("double") / lit(None).cast("date") (PySpark parity)** — Casting a null literal to double or date is now supported for schema evolution and typed null columns. Fixes #260 (#261).

### Changed

- **Make: parallel check and check-full** — `make check` uses `fmt-check` and lists deps so `make -j5 check` runs format check, clippy, audit, deny, and test-rust in parallel. `make -j7 check-full` runs all 7 jobs (5 Rust + 2 Python) in parallel. README updated with `make fmt` and -j usage.

## [0.8.2] - 2026-02-12

### Added

- **#256 – create_dataframe_from_rows: schema "list" / "array" (PySpark parity)** — `create_dataframe_from_rows` now accepts column type `"list"` or `"array"` (default element type bigint), so DataFrames with array columns can be created without using `array<element_type>`. Enables posexplode/explode and APIs that need array columns. Fixes #256 (#259).

## [0.8.1] - 2026-02-12

### Added

- **#254 – F.split(column, pattern, limit) (PySpark parity)** — Optional third argument `limit` added to `split()`. When `limit > 0`, returns at most that many parts with the remainder in the last part (e.g. `F.split(col("s"), ",", 2)` on `"a,b,c"` yields `['a', 'b,c']`). Available as `F.split(col, delim, limit)` and `col.split(delim, limit)` in Python; plan/SQL and parity parser updated. Fixes #254 (#255).

### Fixed

- **Rustdoc warnings** — Fixed broken intra-doc links (`args[i]`, `columns[0]`/`columns[1]`) and unclosed HTML tag (`Vec<Option<JsonValue>>`) in doc comments so `cargo doc --document-private-items --no-deps` builds with zero warnings.

## [0.8.0] - 2026-02-12

### Added

- **#248 – Column.eq_null_safe() (PySpark parity)** — Python `Column` now exposes `.eq_null_safe(other)` for null-safe equality in filters (true when both null or both equal). Enables `df.filter(col("a").eq_null_safe(col("b")))`. Fixes #248 (#251).
- **#249 – soundex() in Python API** — Module-level `soundex(column)` is now exposed in Python; the Rust implementation already existed but was not wired through the bindings. Fixes #249 (#252).
- **#250 – Column.between(low, high) (PySpark parity)** — Python `Column` now exposes `.between(low, high)` for inclusive range filters, e.g. `df.filter(col("v").between(lit(20), lit(30)))`. Fixes #250 (#253).
- **Rust–Python parity: 14 expression bindings** — The following Rust Column/function APIs are now exposed in Python as both module-level functions (e.g. `F.length(col)`) and `Column` methods (e.g. `col.length()`), with matching stubs in `robin_sparkless.pyi`:
  - **String:** `length`, `trim`, `ltrim`, `rtrim`, `repeat`, `reverse`, `initcap`
  - **Regex:** `regexp_extract`, `regexp_replace`
  - **Math:** `floor`, `round`, `exp`
  - **Hash/other:** `levenshtein`, `crc32`, `xxhash64`
  New tests: `tests/python/test_missing_bindings_parity.py`. Parity cross-check doc: [docs/RUST_PYTHON_PARITY_CROSSCHECK.md](docs/RUST_PYTHON_PARITY_CROSSCHECK.md).

## [0.7.1] - 2026-02-12

### Added

- **#244 – Column.isin() (PySpark parity)** — Python `Column` now exposes `.isin(values)` so patterns like `df.filter(col("id").isin([]))` work without raising `AttributeError`. Supports empty lists (returns 0 rows) and lists of ints or strings, backed by existing Rust `isin_i64`/`isin_str` functions. New tests: `test_issue_244_column_isin.py`.
- **#245 – Column nulls ordering methods (PySpark parity)** — Python `Column` now exposes `.asc()`, `.asc_nulls_first()`, `.asc_nulls_last()`, `.desc()`, `.desc_nulls_first()`, `.desc_nulls_last()` returning `SortOrder` for use with `order_by_exprs`. Matches PySpark `col("value").desc_nulls_last()` / `asc_nulls_first()` semantics for null placement in sort order. New tests: `test_issue_245_column_nulls_ordering.py`.

### Fixed

- **width_bucket argument validation (panic → error)** — Python `width_bucket(col, min, max, num_bucket)` and the plan interpreter now validate that `num_bucket > 0`. Previously, `num_bucket <= 0` could trigger a panic inside the Rust implementation; now Python raises `ValueError("width_bucket: num_bucket must be positive")` and plan evaluation returns a clear `PlanExprError`.
- **elt/struct/stack empty-argument panics** — Python functions `elt(index, cols)`, `struct(cols)`, and `stack(cols)` now check that at least one column is provided. Empty input lists used to cause a panic in the underlying Rust functions; they now raise `ValueError` with descriptive messages (`elt() requires at least one column`, etc.).
- **named_struct stub/API alignment** — `robin_sparkless.pyi` now declares `named_struct(names: list[str], columns: list[Column]) -> Column` to match the actual Python implementation (`named_struct(names=[...], columns=[...])`) instead of a misleading `*name_column_pairs` vararg signature.
- **PyO3 deprecations and error types** — Replaced deprecated `PyAny::iter()` with `try_iter()` in the Python UDF vectorized path; switched `DataFrame.corr()` return values to `IntoPyObjectExt::into_py_any` instead of deprecated `into_py`. Column index out of range in the Python DataFrame API now raises `IndexError` instead of `RuntimeError`.

## [0.7.0] - 2026-02-11

### Added

- **#239 – datetime/date in row data (PySpark parity)** — `_create_dataframe_from_rows` and `execute_plan` now accept Python `datetime.datetime` and `datetime.date` in row values; they are serialized to ISO strings and parsed by the existing Rust date/timestamp handling. New tests: `test_issue_239_datetime_in_row.py`.
- **#237 – Window and row_number in Python API** — `row_number()` and `Window.partitionBy(...).orderBy(...)` are exposed; use `F.row_number().over(window)` for PySpark-style window expressions. New tests: `test_issue_237_window_row_number_api.py`.
- **#238 – concat and concat_ws in Python API** — Module-level `concat(columns)` and `concat_ws(separator, columns)` are exposed for string concatenation. New tests: `test_issue_238_concat_api.py`.
- **#202 – Supported plan operations API (Sparkless parity)** — New `supported_plan_operations()` returns a tuple of plan op names supported by `_execute_plan` (e.g. `filter`, `select`, `limit`, `orderBy`, …). Sparkless and other backends can query this instead of using a hardcoded list that omits `filter`, fixing `SparkUnsupportedOperationError: Operation 'Operations: filter' is not supported` when using the Robin backend. Documented in `docs/LOGICAL_PLAN_FORMAT.md`. New tests: `test_issue_202_supported_plan_operations.py`.

### Fixed

- **#236 – CaseWhen: Column.otherwise() missing (PySpark parity)** — `when(condition, value)` now returns a `WhenThen` with `.otherwise(default)` so `when(col("x") > 0, lit(1)).otherwise(lit(0))` works. Fixes `AttributeError: 'WhenThen' object has no attribute 'otherwise'`. New tests: `test_issue_236_case_when_otherwise.py`.
- **#235 – Type strictness: string vs numeric comparison** — Comparing a string column to a numeric literal in `filter` (e.g. `df.filter(col("str_col") == lit(123))` or `df.filter(lit(123) == col("str_col"))`) no longer raises `RuntimeError: cannot compare string with numeric type (i32)`. A central `coerce_for_pyspark_comparison` helper now rewrites comparison expressions so that string–numeric combinations route the string side through `try_to_number` (string→double) and compare numerically, with invalid numeric strings treated as null (non-matching). This applies consistently across DataFrame filters, plan interpreter comparisons, and Python Column operators. New tests: `test_issue_235_type_strictness_comparisons.py` and Rust unit tests in `type_coercion.rs` / `functions.rs`.
- **#199 – Other expression or result parity (Sparkless)** — Representative failures (astype/cast returns None, duplicate column names, expression/alias not found, division by zero, orderBy unsupported, etc.) are addressed by child issues #211, #213–#220. Regression tests added for the issue example: `test_issue_199_expression_result_parity.py` (cast to string/int in with_column, collect returns expected value).
- **#220 – orderBy not supported (Sparkless-side)** — robin-sparkless already supports `orderBy` in `_execute_plan` and lists it in `supported_plan_operations()`. Regression tests added: `test_issue_220_orderby_supported.py`. Sparkless should query `supported_plan_operations()` for the Robin backend to avoid `SparkUnsupportedOperationError: Operation 'Operations: orderBy' is not supported`.
- **#219 – TypeError NoneType in astype float/string conversions** — Collect after float/string or string/float cast with nulls returns nulls as Python None; iteration over collected rows and keys is safe. Regression tests added: `test_issue_219_astype_float_string_none.py`. Callers should use `value is None` before `x in value` when handling possibly-null values.
- **#218 – Division by zero returns null (Sparkless parity)** — Division by zero (e.g. `lit(1) / col("x")` when x=0, or `col("a") / lit(0)`) now returns null instead of inf. Spark/PySpark return null; Polars returns inf. `apply_pyspark_divide` now checks for divisor zero and yields null. New tests: `test_issue_218_division_by_zero.py`.
- **#217 – String-to-int cast: empty/invalid strings (Sparkless parity)** — Casting empty or invalid strings to int/long no longer raises `RuntimeError: conversion from \`str\` to \`i32\` failed`. Custom string→int parsing treats empty/whitespace/invalid as null (Spark/Sparkless parity). New `apply_string_to_int` in udfs; `cast`/`try_cast` use it for string→Int32/Int64. New tests: `test_issue_217_string_to_int_cast.py`.
- **#216 – Date cast from datetime string (Sparkless parity)** — Casting a string like `"2025-01-01 10:30:00"` to date no longer fails. The date parser now accepts both date-only (`YYYY-MM-DD`) and datetime (`YYYY-MM-DD HH:MM:SS`) strings and truncates to date (Spark parity). New `apply_string_to_date` in udfs; `cast`/`try_cast` use it for string→date. New tests: `test_issue_216_date_cast_datetime_string.py`.
- **#215 – Duplicate column names in select (Sparkless parity)** — Same fix as #213: select expressions that produce duplicate column names (e.g. `col("num").cast("string")`, `col("num").cast("int")`) no longer raise `RuntimeError: duplicate: the name 'num' is duplicate`. Regression tests added: `test_issue_215_duplicate_column_names.py`.
- **#213 – Duplicate column names in select (Sparkless parity)** — `select(col("num").cast("string"), col("num").cast("int"))` no longer raises `RuntimeError: duplicate: the name 'num' is duplicate`. `select_with_exprs` now uses `polars_plan::utils::expr_output_name` to detect duplicate output names and disambiguates with `_1`, `_2`, … (first keeps original name). New dependency: `polars-plan = "0.45"`. New tests: `test_issue_213_duplicate_column_names.py`.
- **#211 – astype/cast result None in collect (Sparkless parity)** — `collect()` could return `None` for cast/astype result cells when Polars produced an `AnyValue` variant that was not handled in `any_value_to_py`. All numeric variants (Int16, UInt8, UInt16), Binary/BinaryOwned, and a fallback for any other variant (Duration, Time, Categorical, etc.) are now handled so cast results are never dropped. New tests: `test_issue_211_astype_result_none.py` (with_column, select, and _execute_plan cast).
- **#201 – Type strictness (string vs numeric, coercion)** — PySpark coerces string to numeric in arithmetic; robin-sparkless now matches. Python Column operators (`col("a") + col("b")`, `-`, `*`, `/`, `%`) already used PySpark-style coercion; the plan interpreter now supports `add`, `subtract`, `multiply`, `divide`, `mod` (and aliases `+`, `-`, `*`, `/`, `%`) with the same string-to-numeric coercion in `withColumn`/`select` expressions. Invalid numeric strings yield null. New tests: `test_issue_201_type_strictness.py`, `plan_with_column_add_string_numeric`.
- **#214 – Expression/alias 'not found' in select (Sparkless parity)** — Same fix as #212: select with aliased/computed expressions (e.g. `when().then().otherwise().alias("result")`, window `.alias("rank")`) no longer raises `RuntimeError: not found: result` / `not found: rank`. Regression tests added: `test_issue_214_expression_alias_not_found.py`.
- **#212 – Expression/alias 'not found' in select (Sparkless parity)** — Select with aliased expressions (e.g. `when().then().otherwise().alias("result")`, `col("x").rank().over([...]).alias("rank")`) no longer raises `RuntimeError: not found: result` / `not found: rank`. The same `resolve_expr_column_names` logic from #200 skips resolving alias output names as input columns. Doc comment updated; new tests: `test_issue_212_expression_alias_not_found.py` (when/otherwise, window rank, plan select).
- **#200 – substr/substring with alias (Sparkless parity)** — `select(col("name").substr(1, 3).alias("partial"))` no longer raises `RuntimeError: not found: partial`. Column names that appear only as alias outputs in an expression are no longer resolved as input columns in `resolve_expr_column_names`, so expression-defined output names (e.g. `partial`) are not looked up in the DataFrame schema. New tests: `test_issue_200_substr_alias.py`.
- **#199 – String-to-boolean cast (partial)** — `cast` and `try_cast` now support string-to-boolean via custom parsing ("true"/"false"/"1"/"0" case-insensitive). Polars does not support Utf8→Boolean natively. Fixes test_column_astype string-to-boolean failures.
- **#198 – map(), array(), nested struct/row values** — `create_dataframe_from_rows` and `execute_plan` now support `array<>`, `struct<>`, and nested types. Python row values accept `dict` (struct/map) and `list` (array); `collect()` returns struct columns as Python dicts. New test: `test_create_dataframe_from_rows_struct_and_array`.
- **Type stubs and lint** — Added `Column.try_cast` to robin_sparkless.pyi for mypy; addressed clippy warnings in session.rs (is_none_or, needless_borrow).
- **#208 – String arithmetic tests adapted for robin-sparkless API** — `test_string_arithmetic_robin.py` now uses `with_column` (snake_case) instead of `withColumn`, explicit `cast`/`try_cast` for string-to-numeric coercion (robin requires explicit cast; invalid strings become null via `try_cast`), and `print_schema()` instead of `schema()` for result type assertions.
- **#196 – concat/concat_ws with literal separator or mixed literals in plan select** — When the plan `select` payload contains a string that looks like `concat(...)` or `concat_ws(...)` (e.g. `concat(first_name, , last_name)` with empty literal between columns), the plan interpreter now parses it as an expression and evaluates it instead of resolving it as a column name. Supports literal separators and mixed column/literal args. `concat` in the expression layer now accepts a single argument (PySpark parity). New test: `plan_select_concat_string`.
- **#195 – Column/expression resolution in plan interpreter** — `execute_plan` now resolves column names (case-insensitive) for `select`, `orderBy`, `drop`, `withColumnRenamed`, `groupBy`, and `join` so plans using column names that differ in case from the schema, or that reference computed columns by alias after a select-with-expressions step, work correctly. Select supports both a list of column name strings (resolved) and a list of `{name, expr}` objects (expressions resolved via `resolve_expr_column_names`). Aggregation columns in `groupBy`/`agg` are resolved. New test: `plan_column_resolution`.

### Changed

- **Error handling and docs** — Rust: `create_map` and `array` return `Result` instead of panicking for empty input; Python: `coalesce`, `format_string`, `printf`, and `named_struct` validate arity and raise `ValueError` for empty columns. Type-coercion tests use `Result` and `?`; new session test for empty-schema error. Docs: QUICKSTART, PYSPARK_DIFFERENCES, UDF_GUIDE, and `lib.rs` panic/error section updated for API accuracy and runnable examples (e.g. `_create_dataframe_from_rows` name, vectorized UDF single-column example).

## [0.6.0] - 2026-02-10

### Fixed

- **#194 – Column name case sensitivity (Sparkless parity)** — Column names are now resolved case-insensitively by default (PySpark `spark.sql.caseSensitive=false`), so queries and DataFrame API calls using lowercase or mixed-case column names (e.g. `col("name")`, `SELECT name FROM t`) work when the schema has different casing (e.g. `Name`). SQL: identifiers in SELECT, WHERE, GROUP BY, ORDER BY, HAVING, and aggregate arguments are resolved against the current DataFrame. DataFrame API: `filter`, `select_with_exprs`, `order_by_exprs`, and `with_column` now resolve column names in expressions via `resolve_expr_column_names()` before applying to Polars. New tests: `test_sql_case_insensitive_columns`, `test_case_insensitive_filter_select`.

### Changed

- **Documentation** — USER_GUIDE and QUICKSTART updated: fix `_create_dataframe_from_rows` API, PySpark-style camelCase for persistence methods, correct `when`/`then`/`otherwise` nested syntax, `na().fill(rs.lit(0))` usage, clarify `to_pandas()` returns list of dicts. Added `tests/python/test_doc_examples.py` to verify doc code runs.
- Version 0.6.0.

## [0.5.0] - 2026-02-09

### Added

- **UDFs (scalar, vectorized, grouped)** — Expanded user-defined function support across Rust and Python with a session-scoped registry.
  - **Rust**: `SparkSession::register_udf(name, closure)` and `call_udf(name, cols)` — UDFs run lazily via Polars `Expr::map` / `map_many`.
  - **Python scalar UDFs**: `spark.udf().register(name, f, return_type=None)` (default `StringType`); `call_udf(name, *cols)`; `my_udf(col("a"))` via returned `UserDefinedFunction`. Scalar Python UDFs run row-at-a-time (eager at the UDF boundary).
  - **Python vectorized UDFs (column-wise)**: `spark.udf().register(name, f, return_type=..., vectorized=True)` — UDF receives Python sequences (e.g. lists or pandas Series) and returns one value per input element. Supported in `with_column` / `select` / `call_udf` paths; batch size controlled by new session config `spark.robin.pythonUdf.batchSize`.
  - **Grouped vectorized Python UDFs (GROUPED_AGG)**: New `pandas_udf` helper for grouped aggregations: `@rs.pandas_udf("double", function_type="grouped_agg") def f(values): ...`. Supported in `group_by(...).agg([f(col("x")).alias("out"), ...])`, returning one value per group. Backed by `PythonUdfKind::GroupedVectorizedAgg` and a dedicated grouped execution path.
  - **Config knobs**: `SparkSession` reads `spark.robin.pythonUdf.batchSize` and `spark.robin.pythonUdf.maxConcurrentBatches` from its config map (Python: `SparkSession.builder().config(key, value)`); batch size is used for non-grouped vectorized UDFs; maxConcurrentBatches is reserved for future concurrency control.
  - **Plan interpreter**: Logical plan format extended with `python_grouped_udf` aggregation nodes in `groupBy` payloads; the interpreter currently rejects these with a clear `PlanError::InvalidPlan` until grouped UDF execution is wired through `execute_plan`.
  - **SQL**: Unknown functions in SELECT and WHERE continue to resolve to the UDF registry; built-ins `UPPER`/`LOWER` supported; `SelectItem::ExprWithAlias` for `SELECT expr AS alias`.
  - **Thread-local session**: `call_udf` resolves UDFs from the session set by `get_or_create()`.
  - **Docs**: [UDF_GUIDE.md](docs/UDF_GUIDE.md), [PYTHON_API.md](docs/PYTHON_API.md), [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md), [DEFERRED_SCOPE.md](docs/DEFERRED_SCOPE.md), [ROBIN_SPARKLESS_MISSING.md](docs/ROBIN_SPARKLESS_MISSING.md), [FULL_PARITY_ROADMAP.md](docs/FULL_PARITY_ROADMAP.md), and top-level READMEs updated to describe UDF support and grouped vectorized `pandas_udf(..., function_type="grouped_agg")`. `udtf` and non-aggregating `pandas_udf` variants remain deferred.

### Changed

- Version 0.5.0.

## [0.4.0] - 2026-02-09

### Added

- **#187 – Window API for row_number, rank, sum over window, lag, lead** — Column now exposes `.over(partition_by)` and window functions: `.row_number(descending)`, `.rank(descending)`, `.dense_rank(descending)`, `.lag(n)`, `.lead(n)`. Use with `.over(["dept"])` for partition. Aggregations like `sum(col("amount")).over(["id"])` are supported. Enables PySpark-style window expressions without a separate Window type.
- **#186 – lit(): extend to date/datetime types for PySpark parity** — `lit()` now accepts `datetime.date` and `datetime.datetime` in addition to `None`, int, float, bool, and str. Enables expressions like `col("dt") > lit(some_date)` and `with_column("ts", lit(datetime.datetime(...)))` with proper date/timestamp semantics.
- **#184 – Filter: support Column–Column comparisons** — `df.filter(col("a") > col("b"))` and `df.filter(col("a").gt(col("b")))` now work. The Python `Column` type implements rich comparison methods (`__gt__`, `__ge__`, `__lt__`, `__le__`, `__eq__`, `__ne__`), and the comparison methods (`gt`, `ge`, `lt`, `le`, `eq`, `ne`) accept either a Column or a scalar (int, float, bool, str, None), matching PySpark semantics.
- **#174 – Column Python operator overloads** — `col("age") > lit(30)` and `col("age") > 30` work (PySpark-style). Implementation was in 0.4.0 (#184). Tests added: operator vs method parity, `with_column` with operator expressions, combined `&`/`|`, float/string scalars, reflected comparison (e.g. `30 < col("age")`).
- **#175 – join(on=): accept str for single column (PySpark compatibility)** — `df.join(other, on="id", how="inner")` now works; previously only `on=["id"]` was accepted. `on` can be a single column name (str) or a list/tuple of column names.
- **In-memory saveAsTable and catalog (PySpark-aligned)** — Two namespaces: temp views and saved tables. `df.write().saveAsTable(name, mode="error"|"overwrite"|"append"|"ignore")` registers in the saved-tables catalog (session-scoped; no disk). `spark.table(name)` and `spark.read_delta(name_or_path)` resolve temp view first, then saved table. Catalog: `listTables(dbName=None)`, `tableExists(tableName, dbName=None)`, `dropTempView(name)`, `dropTable(tableName)`. `df.write_delta_table(name)` registers a DataFrame for `read_delta(name)` without the delta feature. Only unqualified table names; see [docs/PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).

### Changed

- **Python parity tests** — Lit date/datetime, column-vs-column, issue 176, and window parity tests now use only predetermined expected outputs (from prior PySpark runs); no PySpark or JVM at test runtime.

## [0.3.1] - 2026-02-09

### Fixed

- **#182 – select() now evaluates Column expressions instead of resolving by name** — When `select()` received Column objects (e.g. `lit(2) + col("x")` or `(col("a") * 2).alias("doubled")`), the Python bindings tried `extract::<String>()` before `extract::<PyColumn>()`. Because Column is convertible to string, expressions were wrongly treated as column names and raised `RuntimeError: not found: (2 + x)`. We now check for PyColumn first, then str, so expression-based `select()` and the Sparkless Robin backend match PySpark semantics.
- **Type-conversion functions no longer panic** — `to_char`, `to_varchar`, `to_number`, `try_to_number`, and `try_to_timestamp` now return `Result<Column, String>` (Rust) and `PyResult<PyColumn>` (Python) instead of using `.expect()`; invalid type names or unsupported column types produce a clear error instead of aborting. Plan interpreter and parity tests propagate these errors.

## [0.3.0] - 2026-02-06

### Added

- **#179 – with_column expression operators (PySpark parity)** — Column now supports Python operators `+`, `-`, `*`, `/`, `%` for building expressions. Enables PySpark-style `col("a") * 2`, `lit(2) + col("x")`, `(col("a") * 2).alias("doubled")` in `with_column` and `with_columns`. Also added Rust `add`, `subtract`, `divide`, `mod_` on Column.

### Changed

- Version bump to 0.3.0.

### Fixed

- **#178 – pytest-xdist / forked worker crashes** — Polars (used by robin-sparkless) is multithreaded and not fork-safe. Added `configure_for_multiprocessing()` to limit Polars to a single thread, reducing worker crashes ("node down: Not properly terminated") when using pytest-xdist (`pytest -n N`) or multiprocessing with fork. Call it early (e.g. in `conftest.py`) or set `ROBIN_SPARKLESS_MULTIPROCESSING=1` before running. See [docs/PYTHON_API.md](docs/PYTHON_API.md#multiprocessing-and-pytest-xdist).
- **Parity fixtures**: PySpark-expected values as source of truth; Robin matches behavior.
  - `current_date`/`curdate` mock returns Date type (not timestamp).
  - `octet_length` parity handler and Int32 return type.
  - `with_unix_micros` UTC timezone alignment.
  - `with_rand_seed` skipped (Spark XORShiftRandom vs Robin StdRng differ).
  - `current_date_timestamp` and `with_curdate_now` expected values aligned with mock dates.

## [0.2.1]

### Added

- **Gap closure (bitmap, datetime/interval, misc, DataFrame)** — plan Phases 1–4
  - **Phase 1**: Bitmap (5): `bitmap_bit_position`, `bitmap_bucket_number`, `bitmap_construct_agg`, `bitmap_count`, `bitmap_or_agg`. Datetime/interval: `make_dt_interval`, `make_ym_interval`, `to_timestamp_ltz`, `to_timestamp_ntz`.
  - **Phase 2**: `sequence`, `shuffle`, `inline`, `inline_outer`; regression aggregates `regr_avgx`, `regr_avgy`, `regr_count`, `regr_intercept`, `regr_r2`, `regr_slope`, `regr_sxx`, `regr_sxy`, `regr_syy`. Stubs: `call_function`, UDF/UDTF, window/session_window, HLL/sketch aggregates, etc. (see PYSPARK_DIFFERENCES).
  - **Phase 3**: `DataFrame::cube()`, `DataFrame::rollup()` with `.agg()`; generic `DataFrame::write()` → `.mode()`, `.format()`, `.save(path)` (parquet/csv/json, overwrite/append). DataFrame stubs: `data` (same as collect), `toLocalIterator` (same as collect), `persist`/`unpersist` (no-op); `rdd`, `foreach`, `foreachPartition`, `mapInPandas`, `mapPartitions` (raise NotImplementedError); `storageLevel` (returns None), `isStreaming` (False), `withWatermark` (no-op). Python: `PyCubeRollupData`, `PyDataFrameWriter`, `cube()`, `rollup()`, `write()`.
  - **Phase 4**: XML/XPath/sentences documented as optional/deferred in [ROBIN_SPARKLESS_MISSING.md](docs/ROBIN_SPARKLESS_MISSING.md) and [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).

- **Signature alignment (optional params & two-arg when)** ✅ **COMPLETED**
  - **Optional parameters**: `assert_true(col, errMsg)`; `like`/`ilike(str, pattern, escapeChar)`; `months_between(date1, date2, roundOff)`; `parse_url(..., key)`; `make_timestamp(..., timezone)`; `position(substr, str, start)`; `to_char`/`to_varchar`/`to_timestamp`/`try_to_timestamp(col, format)` (PySpark-style format mapping for datetime); `to_number`/`try_to_number` accept `format` (reserved for future format-based parsing).
  - **Two-arg when**: `when(condition, value)` returns value where condition is true, null otherwise (single-branch when).
  - **Parity fixtures** (149 → 159): `position_start`, `assert_true_err_msg`, `like_escape_char`, `ilike_escape_char`, `months_between_round_off`, `parse_url_key`, `make_timestamp_timezone`, `to_timestamp_format`, `to_char_format`, `when_two_arg`.
  - **PyO3**: Signature attributes aligned to PySpark param names (snake_case in `#[pyo3(signature = ...)]`); deprecated `into_py` replaced with `into_py_any` for PyColumn/PyWhenBuilder. Section 3 (param count/shape) documented in [SIGNATURE_ALIGNMENT_TASKS.md](docs/SIGNATURE_ALIGNMENT_TASKS.md).
  - **Code quality**: `cargo fmt`, Clippy clean with `-D warnings`.

- **Phase 25 – Readiness for post-refactor merge** ✅ **COMPLETED**
  - **Plan interpreter**: `execute_plan(session, data, schema, plan)` in Rust (`src/plan/`); Python `robin_sparkless.execute_plan(data, schema, plan_json)` returning a DataFrame (call `.collect()` for list of dicts).
  - **Logical plan schema**: [docs/LOGICAL_PLAN_FORMAT.md](docs/LOGICAL_PLAN_FORMAT.md) defines op list, payload shapes (filter, select, withColumn, join, union, orderBy, limit, groupBy+aggs, etc.), and expression tree format.
  - **Expression interpreter**: `src/plan/expr.rs` converts serialized expression trees to Polars `Expr`. **Extended to all scalar functions**: col, lit, comparison/logical ops, eq_null_safe, and **all scalar functions** valid in filter/select/withColumn (string, math, datetime, type/conditional, binary/bit, array/list, map/struct, misc; two-arg when). Single source of truth: `expr_from_fn` and `expr_from_fn_rest` delegate to `crate::functions` / `Column`; literal and arg helpers in expr.rs.
  - **Plan fixtures and tests**: `tests/fixtures/plans/filter_select_limit.json`, `join_simple.json`, `with_column_functions.json`; `plan_parity_fixtures` test in `tests/parity.rs`; unit tests in `src/plan/expr.rs` for length, substring, year, cast, when, concat, greatest, array_size, element_at, coalesce.
  - **create_dataframe_from_rows**: Rust `SparkSession::create_dataframe_from_rows(rows, schema)` for arbitrary schema and row data; Python `SparkSession.create_dataframe_from_rows(data, schema)` (data: list of dicts or list of lists). See [READINESS_FOR_SPARKLESS_PLAN.md](docs/READINESS_FOR_SPARKLESS_PLAN.md) and [ROADMAP.md](docs/ROADMAP.md).

- **Missing PySpark features (plan Phases 1–6)** ✅ **COMPLETED**
  - **Phase 1**: GroupedData `covar_pop`, `covar_samp`, `corr`, `kurtosis`, `skewness`; `approx_percentile`, `percentile_approx`; `df.corr()` correlation matrix; parity agg parser and `covar_pop_expr`/`corr_expr`/`kurtosis`/`skewness` in functions.
  - **Phase 2**: AES `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt` (AES-128-GCM); `encode`, `decode`, `to_binary`, `try_to_binary` (UTF-8, hex); `octet_length`, `char_length`, `character_length`. Polars `moment` feature for kurtosis/skew.
  - **Phase 3**: `aggregate` (array fold: zero + sum), `cardinality` (alias for size); `json_object_keys`, `json_tuple`; `from_csv`, `to_csv`, `schema_of_csv`, `schema_of_json` (minimal/stub).
  - **Phase 4**: `grouping`, `grouping_id` (stub: return 0).
  - **Phase 5**: `dtypes()` (column name + dtype string list); `repartition_by_range`, `sort_within_partitions` (no-op); `create_global_temp_view`, `create_or_replace_global_temp_view` (stub: same catalog as temp view).
  - **Phase 6**: Aliases `sign`→signum, `std`→stddev, `mean`→avg, `date_trunc`→trunc, `regexp`→rlike.
  - **Docs**: [ROBIN_SPARKLESS_MISSING.md](docs/ROBIN_SPARKLESS_MISSING.md) and [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md) updated.

- **Phase 24 – Full parity 5: bit, control, JVM stubs, random, crypto** ✅ **COMPLETED** (bit, control, JVM stubs, rand/randn, AES crypto implemented)
  - **Bit**: `bit_and`, `bit_or`, `bit_xor`, `bit_count`, `bit_get`; `bitwise_not` / `bitwiseNOT`. Parity fixture `with_bit_ops` added.
  - **Control**: `assert_true`, `raise_error` (expression-level; assert_true fails when any value is false; raise_error always fails when evaluated).
  - **JVM stubs**: `broadcast` (no-op), `spark_partition_id` (constant 0), `input_file_name` (empty string), `monotonically_increasing_id` (constant 0), `current_catalog`, `current_database`, `current_schema`, `current_user`, `user` (constant placeholders). Semantics documented in [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).
  - **Random**: `rand(seed)`, `randn(seed)` use a real RNG with optional seed; when added via `with_column` or `with_columns`, one distinct value per row (PySpark-like). Semantics documented in [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).
  - **Crypto**: `aes_encrypt`, `aes_decrypt`, `try_aes_decrypt` **implemented** (Phase 2; AES-128-GCM). See [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md).
  - **PyO3**: All new functions exposed in the `robin_sparkless` Python module (bit ops, control, JVM stubs, rand/randn, broadcast).

- **Phase 23 – Full parity 4: JSON, CSV, URL, misc** ✅ **COMPLETED**
  - **URL**: `url_decode`, `url_encode` (percent-encoding).
  - **JSON**: `json_array_length`, `parse_url`.
  - **Misc**: `isin`, `isin_i64`, `isin_str`; `equal_null`; `hash` (xxHash64); `shift_left`, `shift_right`, `shift_right_unsigned`; `version`; `stack` (alias for struct_).
  - **Parity fixtures**: `with_isin`, `with_url_decode`, `with_url_encode`, `json_array_length_test`, `with_hash`, `with_shift_left` (142→148).
  - **PyO3**: All new functions.

- **Phase 22 – Full parity 3: datetime extensions** ✅ **COMPLETED**
  - **Aliases**: `curdate`, `now`, `localtimestamp`; `date_diff`, `dateadd`, `datepart`.
  - **Extract / unix**: `extract`, `date_part`, `unix_micros`, `unix_millis`, `unix_seconds`.
  - **UDF-based**: `dayname`, `weekday` (PySpark weekday: Mon=0..Sun=6).
  - **Constructors**: `make_timestamp`, `make_timestamp_ntz`; `to_timestamp` (strict cast).
  - **Interval**: `make_interval`, `timestampadd`, `timestampdiff`; `days`, `hours`, `minutes`, `months`, `years`.
  - **Timezone**: `from_utc_timestamp`, `to_utc_timestamp`, `convert_timezone`, `current_timezone` (chrono-tz).
  - **Parity fixtures**: `with_dayname`, `with_weekday`, `with_extract`, `with_unix_micros`, `make_timestamp_test`, `timestampadd_test`, `from_utc_timestamp_test` (136 → 142; `with_curdate_now` skipped).
  - **PyO3**: All new functions.

- **Phase 21 – Full parity 2: string, binary, type, array/map/struct** ✅ **COMPLETED**
  - **String**: `btrim`, `locate`, `conv` (base conversion).
  - **Binary**: `hex`, `unhex`, `bin`, `getbit`.
  - **Type/cast**: `to_char`, `to_varchar`, `to_number`, `try_to_number`, `try_to_timestamp`.
  - **Map**: `str_to_map` (parse "k1:v1,k2:v2" into map).
  - **Array**: `arrays_overlap`, `arrays_zip`, `explode_outer`, `posexplode_outer`, `array_agg`.
  - **Struct**: `transform_keys`, `transform_values` (Rust only; require Expr).
  - **Parity fixtures**: `with_btrim`, `with_hex`, `with_conv`, `with_str_to_map`, `arrays_overlap`, `arrays_zip` (130 → 136).
  - **PyO3**: All new functions except transform_keys/transform_values.

- **Phase 20 – Full parity 1: ordering, aggregates, numeric** ✅ **COMPLETED**
  - **Ordering**: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`; `DataFrame::order_by_exprs` with per-column null placement.
  - **Aggregates**: `median`, `mode`, `stddev_pop`, `stddev_samp`, `var_pop`, `var_samp`, `try_sum`, `try_avg`.
  - **Numeric**: `bround` (banker's rounding), `negate`, `negative`, `positive`, `cot`, `csc`, `sec`, `e`, `pi`.
  - **Parity fixtures**: `groupby_median`, `with_bround`; OrderBy supports optional `nulls_first`.
  - **PyO3**: PySortOrder, order_by_exprs, all new functions.

- **Phase 19 – Remaining gaps 4: aggregates, try_*, misc** ✅ **COMPLETED**
  - **Aggregates**: `any_value`, `bool_and`, `bool_or`, `every`/`some`, `count_if`, `max_by`, `min_by`, `percentile`, `product`, `collect_list`, `collect_set` (GroupedData + parity agg parser).
  - **Try_***: `try_divide` (when/otherwise), `try_add`, `try_subtract`, `try_multiply` (UDFs with checked arithmetic), `try_element_at` (alias of element_at).
  - **Misc**: `width_bucket`, `elt`, `bit_length`, `typeof` (Polars expr or UDF).
  - **Parity fixtures**: `groupby_any_value`, `groupby_product`, `try_divide`, `width_bucket` (124 → 128).
  - **PyO3**: All new GroupedData methods; try_divide, try_add, try_subtract, try_multiply, width_bucket, elt, bit_length, typeof as module/Column methods.

- **Phase 18 – Remaining gaps 3: array, map, struct** ✅ **COMPLETED**
  - **Array**: `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union` (UDFs in udfs.rs + Column/functions + PyO3 + parity parser).
  - **Map**: `map_concat`, `map_from_entries`, `map_contains_key`, `get` (map element); `create_map` added to parity parser.
  - **Struct**: `struct`, `named_struct` (Polars as_struct expr).
  - **Parity fixtures**: `array_append`, `array_prepend`, `array_insert`, `array_except`, `array_intersect`, `array_union`, `map_contains_key`, `get_map`, `struct_test`, `named_struct_test`, `map_concat`, **`map_filter`**, **`zip_with`**, **`map_zip_with`** (103 → 124 fixtures).
  - **Parity harness**: Added struct/map JSON conversion so struct columns and map (List(Struct{key,value})) columns compare correctly in fixtures.
  - **Phase 18 deferred (now implemented)**: `map_filter`, `map_zip_with`, `zip_with` — implemented via UDF + list.eval with Expr-based predicates/merge. PyO3: `map_filter_value_gt`, `zip_with_coalesce`, `map_zip_with_coalesce`.

- **Phase 17 – Remaining gaps 2: datetime/unix and math** ✅ **COMPLETED**
  - **Datetime/unix**: `unix_timestamp`, `to_unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date` (Rust + PyO3 + parity parser).
  - **Math**: `pmod`, `factorial` (positive modulus; n! for n in 0..=20).
  - **Implementation**: UDFs in udfs.rs for parsing/formatting (chrono), epoch conversion via Polars cast/mul for timestamp_*, date↔days via Polars Date/Int32 cast.
  - **Parity fixtures**: `unix_timestamp`, `from_unixtime`, `make_date`, `timestamp_seconds`, `timestamp_millis`, `timestamp_micros`, `unix_date`, `date_from_unix_date`, `pmod`, `factorial` (93 → 103 fixtures).

- **Phase 16 – Remaining gaps 1: string/regex** ✅ **COMPLETED**
  - **String/regex**: `regexp_count`, `regexp_instr`, `regexp_substr`, `split_part`, `find_in_set`, `format_string`, `printf` (Rust + PyO3 + parity parser).
  - **Implementation**: `regexp_count` via Polars `str().count_matches()`, `regexp_substr` as alias of `regexp_extract(0)`, `regexp_instr` and `find_in_set` via UDFs in udfs.rs, `format_string`/`printf` with printf-style %s/%d/%f/%g parsing, `split_part` via split + list.get.
  - **Parity fixtures**: `regexp_count`, `regexp_substr`, `regexp_instr`, `split_part`, `find_in_set`, `format_string` (88 → 94 fixtures).

- **Phase 15 – Functions batch 3** ✅ **COMPLETED**
  - **Batch 1 (aliases/simple)**: `nvl`, `ifnull`, `nvl2`, `substr`, `power`, `ln`, `ceiling`, `lcase`, `ucase`, `dayofmonth`, `to_degrees`, `to_radians`, `isnull`, `isnotnull` (Rust + PyO3 + parity parser). Fixture `phase15_aliases_nvl_isnull`.
  - **Batch 2 (string)**: `left`, `right`, `replace` (literal), `startswith`, `endswith`, `contains`, `like` (SQL LIKE → regex), `ilike`, `rlike`/`regexp`. Fixture `string_left_right_replace`.
  - **Batch 3 (math)**: `cosh`, `sinh`, `tanh`, `acosh`, `asinh`, `atanh`, `cbrt`, `expm1`, `log1p`, `log10`, `log2`, `rint`, `hypot` (UDFs in udfs.rs + Column/functions + PyO3). Fixture `math_cosh_cbrt`.
  - **Batch 4 (array)**: `array_distinct`. Fixture `array_distinct`.
  - Parity fixtures: 84 → 88. Remaining gaps addressed in Phases 16–19; then Phases 20–24 (full parity), Phase 25 (readiness for post-refactor merge), Phase 26 (publish crate), Phase 27 (Sparkless integration). Gap list: [docs/PHASE15_GAP_LIST.md](docs/PHASE15_GAP_LIST.md), [docs/GAP_ANALYSIS_SPARKLESS_3.28.md](docs/GAP_ANALYSIS_SPARKLESS_3.28.md).

- **Phase 1 – Foundation** ✅
  - Structural alignment: split `dataframe.rs` into `transformations.rs`, `aggregations.rs`, `joins.rs`.
  - Case sensitivity: `spark.sql.caseSensitive` (default false), centralized column resolution for filter, select, withColumn, join; fixture `case_insensitive_columns`.
  - Fixture converter: `tests/convert_sparkless_fixtures.py` maps Sparkless `expected_outputs` → robin-sparkless format; operation mapping for filter, groupby, join, window, withColumn, union, distinct, drop, dropna, fillna, limit, withColumnRenamed.

- **Phase 2 – High-Value Functions** (partial) ✅
  - String: `length`, `trim`, `ltrim`, `rtrim`, `regexp_extract`, `regexp_replace`, `split`, `initcap`; parity fixture `string_length_trim`.
  - Datetime: `to_date()`, `date_format(format)` (chrono strftime), `year`, `month`, `day` in Rust API (no date/datetime fixture yet; harness does not build date columns from JSON).
  - Additional parity: `regexp_like`, `regexp_extract_all` (fixtures + parser support).

- **Phase 3 – DataFrame Methods** ✅
  - `union` / `unionAll`, `unionByName`, `distinct` / `dropDuplicates`, `drop`, `dropna`, `fillna`, `limit`, `withColumnRenamed`.
  - Parity fixtures: `union_all`, `union_by_name`, `distinct`, `drop_columns`, `dropna`, `fillna`, `limit`, `with_column_renamed`.

- **Phase 4 – PyO3 Bridge** ✅
  - Optional Python bindings when built with `--features pyo3`.
  - Python module `robin_sparkless` with PySpark-like API: `SparkSession`, `SparkSessionBuilder`, `DataFrame`, `Column`, `GroupedData`, `WhenBuilder`, `ThenBuilder`.
  - Session: `builder()`, `get_or_create()`, `create_dataframe`, `read_csv`, `read_parquet`, `read_json`, `is_case_sensitive()`.
  - DataFrame: `filter`, `select`, `with_column`, `order_by`, `group_by`, `join`, `union`, `union_by_name`, `distinct`, `drop`, `dropna`, `fillna`, `limit`, `with_column_renamed`, `count`, `show`, `collect` (list of dicts).
  - Column/expressions: `col`, `lit`, `when().then().otherwise()`, `coalesce`, `sum`, `avg`, `min`, `max`, `count`; methods `gt`, `ge`, `lt`, `le`, `eq`, `ne`, `and_`, `or_`, `alias`, `is_null`, `is_not_null`, `upper`, `lower`, `substr`.
  - GroupedData: `count()`, `sum(column)`, `avg(column)`, `min(column)`, `max(column)`, `agg(exprs)`.
  - Build: `maturin develop --features pyo3`; API contract in [docs/PYTHON_API.md](docs/PYTHON_API.md). Python smoke tests in `tests/python/`; `make test` runs Rust + Python tests.

- **Phase 5 – Test Conversion** ✅
  - Parity test discovers `tests/fixtures/*.json` and `tests/fixtures/converted/*.json`; optional `skip: true` / `skip_reason`.
  - `make sparkless-parity`: when `SPARKLESS_EXPECTED_OUTPUTS` is set, runs converter then `cargo test pyspark_parity_fixtures`.
  - 58 hand-written fixtures at Phase 5 completion; target 50+ met. See [CONVERTER_STATUS.md](docs/CONVERTER_STATUS.md), [SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md).

- **Phase 6 – Broad Function Parity** (partial) ✅
  - **Joins**: `DataFrame::join()` with `JoinType` (Inner, Left, Right, Outer); parity fixtures `inner_join`, `left_join`, `right_join`, `outer_join`; `right_input` and `Operation::Join` in harness.
  - **Multi-aggregation**: `GroupedData::agg()` with multiple aggregations; fixture `groupby_multi_agg`.
  - **Window**: `Column::rank()`, `row_number()`, `dense_rank()`, `lag()`, `lead()` with `.over(partition_by)`; `first_value`, `last_value`, `percent_rank`; fixtures `row_number_window`, `rank_window`, `lag_lead_window`, `first_value_window`, `last_value_window`, `percent_rank_window`. API for `cume_dist`, `ntile`, `nth_value` (partition_by).
  - **Array**: `array_size`/`size`, `array_contains`, `element_at`, `explode`, `array_sort`, `array_join`, `array_slice`; **implemented** (Polars list.eval): `array_position`, `array_remove`, `posexplode`; fixtures `array_contains`, `element_at`, `array_size`.
  - **String**: `regexp_extract_all`, `regexp_like`; PyColumn exposure for `size`, `element_at`, `explode`, `first_value`, `last_value`, `percent_rank`, `regexp_like`.
  - **String (basics)**: `upper`, `lower`, `substring` (1-based), `concat`, `concat_ws`; fixtures `string_upper_lower`, `string_substring`, `string_concat`.

- **Phase 7 – SQL & Advanced** ✅
  - **SQL** (optional `sql` feature): `SparkSession::sql(query)` with temp views (`create_or_replace_temp_view`, `table`). Single SELECT, FROM/JOIN, WHERE, GROUP BY, ORDER BY, LIMIT → DataFrame ops. Python: `spark.sql()`, `spark.create_or_replace_temp_view()`, `spark.table()`.
  - **Delta Lake** (optional `delta` feature): `read_delta(path)`, `read_delta_with_version(path, version)` (time travel), `write_delta(path, overwrite)`. Python bindings for read_delta, read_delta_version, write_delta.
  - **Performance**: Criterion benchmarks `cargo bench` (filter/select/groupBy robin vs Polars); target within ~2x.
  - **Robustness**: Clearer error messages (column names, hints); Troubleshooting in [QUICKSTART.md](docs/QUICKSTART.md).

- **Phase 9 – High-Value Functions & DataFrame Methods** ✅
  - Datetime: `current_date`, `current_timestamp`, `date_add`, `date_sub`, `hour`, `minute`, `second`, `datediff`, `last_day`, `trunc`.
  - String: `repeat`, `reverse`, `instr`, `lpad`, `rpad`; fixtures `string_repeat_reverse`, `string_lpad_rpad`.
  - Math: `sqrt`, `pow`, `exp`, `log`; fixture `math_sqrt_pow`.
  - Conditional: `nvl`/`ifnull`, `nullif`, `nanvl`.
  - GroupedData: `first`, `last`, `approx_count_distinct`; fixture `groupby_first_last`.
  - DataFrame: `replace`, `cross_join`, `describe`, `cache`/`persist`/`unpersist`, `subtract`, `intersect`; fixtures `replace`, `cross_join`, `describe`, `subtract`, `intersect`.

- **Phase 10 – Complex types & window parity** (February 2026) ✅
  - **Window parity**: Fixtures `percent_rank_window`, `cume_dist_window`, `ntile_window`, `nth_value_window` now covered (multi-step workaround in harness); no longer skipped.
  - **String 6.4**: `mask`, `translate`, `substring_index` implemented; fixtures `string_mask`, `string_translate`, `string_substring_index`. **Phase 8**: `soundex`, `levenshtein`, `crc32`, `xxhash64` now **implemented** via map UDFs (strsim, crc32fast, twox-hash, soundex crates).
  - **Array extensions**: `array_exists`, `array_forall`, `array_filter`, `array_transform`, `array_sum`, `array_mean` (Polars `list_any_all`, `list_eval`); fixture `array_sum`. **Phase 8**: `array_flatten` and `array_repeat` now **implemented** via map UDFs.
  - **Map functions**: **Phase 8** – `create_map`, `map_keys`, `map_values`, `map_entries`, `map_from_arrays` now **implemented** (Map as `List(Struct{key, value})`; create_map via as_struct/concat_list; map_keys/map_values via list.eval + struct.field; map_from_arrays via UDF).
  - **JSON**: `get_json_object`, `from_json`, `to_json` (Polars `extract_jsonpath`, `dtype-struct`); fixture `json_get_json_object`.
  - **Parity**: 73 fixtures passing (was 68); ~120+ functions; **no remaining Phase 8 stubs** for array_repeat, array_flatten, map, or string 6.4 (soundex/levenshtein/crc32/xxhash64).

- **Phase 8 – Remaining parity completed** (February 2026) ✅
  - **array_repeat**: Implemented via `Expr::map` UDF (list `try_apply_amortized` + extend).
  - **array_flatten**: Implemented via `Expr::map` UDF (list-of-lists flatten per row).
  - **Map**: `create_map` (as_struct + concat_list), `map_keys`/`map_values` (list.eval + struct.field_by_name), `map_entries` (identity), `map_from_arrays` (zip UDF with list builder).
  - **String 6.4**: `soundex` (soundex crate), `levenshtein` (strsim), `crc32` (crc32fast), `xxhash64` (twox-hash) via `Expr::map` / `Expr::map_many` UDFs.
  - New module `src/udfs.rs` for execution-time UDFs used by these expressions.

- **Documentation and roadmap**:
  - [PYSPARK_DIFFERENCES.md](docs/PYSPARK_DIFFERENCES.md): Known divergences (window, SQL, Delta); Phase 8 stubs removed (all implemented). Linked from README and docs index.
  - FULL_BACKEND_ROADMAP Phase 8 marked completed; PARITY_STATUS, ROADMAP, FULL_BACKEND_ROADMAP, IMPLEMENTATION_STATUS, PYSPARK_DIFFERENCES, README, docs/README updated for Phase 8 completion and ~120+ functions.

- **Phase 11 – Parity scale and test conversion** ✅
  - **Parity harness**: Date, timestamp, and boolean column support in fixture input; `dtype_to_string` and `collect_to_simple_format` for Date, Datetime, Int8; `types_compatible` for date/timestamp/Int8.
  - **New fixtures** (73 → 80): `date_add_sub`, `datediff`, `datetime_hour_minute`, `string_soundex`, `string_levenshtein`, `string_crc32`, `string_xxhash64`.
  - **Expression parser**: soundex, levenshtein, crc32, xxhash64 in withColumn expressions.
  - **Converter**: Date/timestamp type mapping in [tests/convert_sparkless_fixtures.py](tests/convert_sparkless_fixtures.py).
  - **CI**: [.github/workflows/ci.yml](.github/workflows/ci.yml) runs format, clippy, audit, deny, and all tests (including `pyspark_parity_fixtures`); separate job for Python (PyO3) tests.
  - **Docs**: [TEST_CREATION_GUIDE.md](docs/TEST_CREATION_GUIDE.md) date/timestamp format; [SPARKLESS_PARITY_STATUS.md](docs/SPARKLESS_PARITY_STATUS.md) CI note; ROADMAP, FULL_BACKEND_ROADMAP, PARITY_STATUS updated.

- **Phase 12 – DataFrame methods parity** ✅
  - **Rust**: Implemented `freq_items`, `approx_quantile`, `crosstab`, `melt` (full implementations); `sample_by` (stratified sampling); Spark no-ops: `hint`, `is_local`, `input_files`, `same_semantics`, `semantic_hash`, `observe`, `with_watermark`. DataFrame methods count ~35 → ~55+.
  - **PyO3**: Exposed `random_split`, `summary`, `to_df`, `select_expr`, `col_regex`, `with_columns`, `with_columns_renamed`, `stat()` (returns `DataFrameStat` with `cov`/`corr`), `na()` (returns `DataFrameNa` with `fill`/`drop`), `to_pandas` (same as collect; for use with `pandas.DataFrame.from_records`). Registered `PyDataFrameStat` and `PyDataFrameNa` in the module.
  - **Parity**: Fixtures `first_row`, `head_n`, `offset_n` for first/head/offset operations.
  - **Docs**: [PYTHON_API.md](docs/PYTHON_API.md), [PARITY_STATUS.md](docs/PARITY_STATUS.md), [IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md), [ROADMAP.md](docs/ROADMAP.md), [FULL_BACKEND_ROADMAP.md](docs/FULL_BACKEND_ROADMAP.md), README, and docs index updated for Phase 12.

- **Phase 13 – Functions batch 1 (string, binary, collection)** ✅ (partial)
  - **Rust**: String — `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`; Base64 — `base64`, `unbase64` (base64 crate); Binary — `sha1`, `sha2(bit_length)`, `md5` (sha1, sha2, md5 crates; string in → hex out); Collection — `array_compact`. UDFs in `udfs.rs` for ascii, format_number, char, base64, unbase64, sha1, sha2, md5.
  - **PyO3**: Module-level `ascii`, `format_number`, `overlay`, `position`, `char`, `chr`, `base64`, `unbase64`, `sha1`, `sha2`, `md5`, `array_compact`; Column methods `ascii_`, `format_number`, `overlay`, `char_`, `chr_`, `base64_`, `unbase64_`, `sha1_`, `sha2_`, `md5_`, `array_compact`.
  - **Parity**: `parse_with_column_expr` extended for all new functions; fixtures `string_ascii`, `string_format_number` (82 fixtures total).
  - **Docs**: PARITY_STATUS, IMPLEMENTATION_STATUS, PYTHON_API updated for Phase 13.

- **Phase 14 – Functions batch 2 (math, datetime, type/conditional)** ✅
  - **Math**: `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2(y, x)`, `degrees`, `radians`, `signum` (UDFs in `udfs.rs`; Polars has no trig on `Expr`).
  - **Datetime**: `quarter`, `weekofyear`/`week`, `dayofweek` (Sun=1..Sat=7), `dayofyear`; `add_months`, `months_between`, `next_day(day_of_week)` (chrono UDFs).
  - **Type/conditional**: `cast(column, type_name)` (strict), `try_cast(column, type_name)` (null on failure), `parse_type_name()`; `isnan(column)`; `greatest`/`least` over columns (UDFs for Float64/Int64/String).
  - **Parity**: Parser branches for all Phase 14 functions; fixtures `math_sin_cos`, `datetime_quarter_week` (84 fixtures total).
  - **PyO3**: Module-level and Column methods for sin, cos, tan, asin, acos, atan, atan2, degrees, radians, signum, quarter, weekofyear, dayofweek, dayofyear, add_months, months_between, next_day, cast, try_cast, isnan, greatest, least.
  - **Docs**: README, CHANGELOG, PARITY_STATUS, IMPLEMENTATION_STATUS, ROADMAP, PYTHON_API, FULL_BACKEND_ROADMAP, docs/README, QUICKSTART updated for Phase 14.

### Changed

- **Phase 14**: Math (sin, cos, tan, degrees, radians, signum, etc.), datetime (quarter, weekofyear, add_months, months_between, next_day), type/conditional (cast, try_cast, isnan, greatest, least); 84 parity fixtures; PyO3 bindings; all docs and README updated.
- **Phase 13**: String/binary/collection batch 1 (ascii, format_number, overlay, position, char, chr, base64, unbase64, sha1, sha2, md5, array_compact); 82 parity fixtures; PyO3 bindings; with_columns_renamed type fix in Python.
- **Phase 12**: DataFrame methods ~55+ (freq_items, approx_quantile, crosstab, melt, sample_by, no-ops); PyO3 stat/na/to_pandas, random_split, with_columns, etc.; parity fixtures first_row, head_n, offset_n; all docs and README updated.
- **Phase 11**: Parity fixtures 73 → 80; harness date/datetime/boolean support; CI workflow; converter date/timestamp mapping; docs updated.
- **Documentation**: README, ROADMAP, FULL_BACKEND_ROADMAP, MIGRATION_STATUS, COMPILATION_STATUS updated for Phase 8/10 completion; removed all "stubbed" references for array_repeat, array_flatten, Map, and string 6.4 (soundex, levenshtein, crc32, xxhash64).
- **Phase 8**: All four previously stubbed areas are now implemented: array_repeat, array_flatten, map functions (create_map, map_keys, map_values, map_entries, map_from_arrays), and string 6.4 (soundex, levenshtein, crc32, xxhash64). PYSPARK_DIFFERENCES no longer lists these as stubbed.
- **Phase 10**: Window fixtures (percent_rank, cume_dist, ntile, nth_value) documented as covered in PYSPARK_DIFFERENCES; `substring_index` fixed for negative count (no u32 underflow); `mask` uses `replace_all` for correct regex replacement.
- **PyO3 0.24**: Upgraded optional `pyo3` dependency from 0.22 to 0.24 (addresses RUSTSEC-2025-0020). Python bindings use non-deprecated APIs: `PyList::empty`, `PyDict::new`, `IntoPyObjectExt::into_bound_py_any` for collect.
- Parity harness now accepts optional `right_input` for multi-DataFrame fixtures
- Schema comparison allows Polars `_right` suffix for duplicate join column names
- `GroupedData::agg()` with multiple expressions now reorders columns to match PySpark (grouping cols first)

### Tooling

- Added `deny.toml` for cargo-deny (advisories, bans, sources; licenses need per-crate config)
- Updated Makefile with Rust targets: build, test, check, fmt, clippy, audit, outdated, deny, all

## [0.2.0] - 2026-02-06

### Added

- **check-full** now runs Python lint and type-check: `ruff format --check`, `ruff check`, `mypy` via new Makefile target `lint-python` (no Java/PySpark required for full check).
- **create_dataframe_from_rows** parity test uses predetermined PySpark-derived expected rows; tests run only robin-sparkless at runtime (#151).
- **Type stubs** (`robin_sparkless.pyi`): `DataFrame.drop(cols: list[str])`, `Column.multiply`, `DataFrame.pivot`; duplicate `month`/`year` definitions removed for mypy/ruff.
- **Docs**: [CLOSED_ISSUES_TEST_COVERAGE.md](docs/CLOSED_ISSUES_TEST_COVERAGE.md) (closed issues → fixtures/tests), [PORTED_TEST_EXPECTATIONS.md](docs/PORTED_TEST_EXPECTATIONS.md), [SPARKLESS_PYTHON_TEST_PORT.md](docs/SPARKLESS_PYTHON_TEST_PORT.md); SQL/session, string/binary, datetime/type, window, and deferred parity docs updated.
- **Rust/Python API**: `hash()` Murmur3 parity; `array_distinct` first-occurrence order; JSON write append; Python `from_csv`, `to_csv`, `schema_of_csv`, `schema_of_json`; SQL `HAVING` in GROUP BY; Python `get_json_object`, `json_tuple` (#91, #94); `year`/`month`/`nullif`, `log` with base, `astype`/`split`; left_semi and left_anti join; `Column` `__and__`/`__or__` for filter with `&`/`|`; `Column.multiply()`, `DataFrame.pivot` stub (#151, #156).
- **Sparkless parity**: Ported tests and expectations for issues #1–#21; many converted fixtures and parity fixes (split, join_simple, zip_with, array_intersect, etc.).

### Changed

- **Makefile**: `test-python` no longer installs pyspark (no Java needed for test run). `check-full` = check → lint-python → test-python.
- **Python parity test** `test_create_dataframe_from_rows_schema_pyspark_parity`: asserts against a fixed expected list derived from PySpark 3.5; no PySpark or JVM at test runtime.
- **CI**: Python extension built with `pyo3,sql,delta`; release workflow groups Rust and Python jobs.

### Fixed

- **Python row conversion**: `py_to_json_value` checks `bool` before `i64` so Python `True`/`False` stay booleans in `create_dataframe_from_rows` and collect (#151).
- **Python collect**: `any_value_to_py` handles `Date` and `Datetime`/`DatetimeOwned` so `collect()` returns date/datetime as strings for parity tests.
- **Python API**: `pivot()` signature aligned with Rust (`pivot_col`, `values`); `Column.multiply()` exposed; `DataFrame.drop(cols)` accepts list of column names.
- **Parity fixtures**: literal `split(|)`, `join_simple`, `zip_with`/`array_intersect`; `string_xxhash64`, `with_hash` un-skipped with expected values from current implementation; alloc/anes vendor patches for build.

### Tooling

- **Ruff**: format and check run in `lint-python`; test and stub files formatted.
- **Mypy**: `mypy .` in `lint-python`; stubs updated so tests type-check (drop list, multiply, pivot; no duplicate symbols).

## [0.1.0] - (Initial release)

### Added

- PySpark-like DataFrame API built on Polars
- `SparkSession`, `DataFrame`, `GroupedData`, `Column`
- Operations: filter, select, orderBy, groupBy, withColumn, read_csv, read_parquet, read_json
- Expression functions: col, lit_*, when/then/otherwise, coalesce
- GroupedData aggregates: count, sum, avg, min, max, agg
- Parity test harness with fixture-based PySpark comparison
