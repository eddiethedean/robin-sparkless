# Parity Functions: Test Expectations and PySpark Alignment

## Test expectations match PySpark

Expected outputs under `tests/expected_outputs/` are generated from **PySpark** via `tests/tools/generate_expected_outputs.py`. They reflect PySpark behavior and are the source of truth for parity.

## Engine fixes applied (parity/functions)

- **initcap**: Implemented as a UDF that title-cases each word (first letter uppercase, rest lowercase) so behavior matches PySpark. Polars 0.53 has no `to_titlecase`.
- **xxhash64**: Uses seed **42** in `apply_xxhash64` to align with Spark’s XXH64 seed; hash values now match PySpark for the same inputs.
- **json_tuple**: Key arguments are coerced to strings when possible (e.g. `o.extract::<String>().unwrap_or_else(|_| o.to_string())`) so keys from plans or other types don’t raise "keys must be strings".

## Fixes applied (literal replication, explode, log)

- **Literal replication:** When every expression in `select()` references no column from the frame, the engine now cross-joins a single key column with the literal-derived result so the row count matches the input (N rows). Fixes `get_json_object`, `math_exp` (and similar literal-only selects).
- **Split + explode:** When `withColumn(name, F.explode(F.col(name)))` replaces a list column with its exploded form, the engine now uses `LazyFrame.explode()` so other columns are replicated correctly. Fixes `test_split_with_limit_parity`, `test_split_without_limit_parity`, `test_split_with_limit_minus_one_parity`.
- **log(float base):** Python `log(col_or_base, base_or_col=None)` now accepts PySpark’s `log(base, column)` and `log(column, base)`; one argument may be a numeric base (int/float), the other a Column. Fixes `test_log_with_float_base_parity`, `test_log_with_different_bases_parity`.

## Remaining parity failures (engine gaps)

These tests still fail because of current engine behavior; expectations are correct (PySpark-aligned).

| Area | Failure | Cause |
|------|---------|--------|
| **Literal replication (mixed)** | `levenshtein`, `json_tuple` (and similar) | Expression references a column (e.g. `levenshtein(col("name"), lit(""))`) so the “no column refs” path does not apply; or schema/column shape differs (e.g. `json_tuple`). |
| **Struct field alias** | `test_struct_field_with_alias_*` | Selecting struct fields with aliases returns `None` for the extracted value; needs struct/alias handling to match PySpark. |
| **Window orderBy list** | `test_window_orderby_list_multiple_columns_parity` | `Window.orderBy([list of columns])` ordering differs from PySpark. |
| **array_contains with column** | `test_array_contains_join_*` | `list.contains` with a column argument (join) fails: type/plan handling for "column in list" in join/filter. |
| **Array/column dtype** | Various `test_array_*` | Wrong column used (e.g. list op on string "tags"), output naming, or row counts (explode/array_union). |

Regenerating expected outputs from PySpark will not fix these; they require engine/plan/API changes in the Rust/Python layers.
