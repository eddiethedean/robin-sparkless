# Test Failure Checklist

**Last run:** `pytest tests/python tests/upstream_sparkless -n 10`  
**Result:** 862 failed, 1944 passed, 25 skipped (2831 total)

This checklist tracks fixes needed to reduce test failures. Items are grouped by category and ordered by estimated impact.

---

## Latest run failure buckets (by error type)

| Bucket | Approx. count | Example error / cause |
|--------|----------------|------------------------|
| **Row / comparison: str vs int** | ~80+ | `TypeError: '<' not supported between instances of 'str' and 'int'` — Row or sort comparing mixed types (Row as tuple vs dict, or schema string vs int in Row) |
| **Collect/Row: expect string, got number** | ~50+ | `AssertionError: assert 100 == '100'`, `assert 123 == '123'`, `assert True == '1'` — tests expect string-typed column in Row; we return int/float/bool (schema-string preservation or cast-to-string) |
| **simpleString missing** | ~5+ | `AttributeError: 'IntegerType' object has no attribute 'simpleString'` — PySpark type API (simpleString) |
| **json_tuple API** | ~3+ | `TypeError: json_tuple keys must be strings` — json_tuple key type / signature |
| **SparklessError: duplicate column** | ~5+ | `duplicate: column with name 'count'` / `duplicate output name 'manager_id'` — plan producing duplicate names |
| **SparklessError: string vs numeric** | ~2+ | `cannot compare string with numeric type (i64)` — coercion still needed (e.g. eq_null_safe path) |
| **date_trunc / duration** | ~5+ | `expected leading integer in the duration string, found 'm'`; `round operation not supported for dtype str` — date_trunc unit/period handling |
| **replace() API** | ~5+ | `replace() missing 1 required positional argument: 'value'`; `PyColumn.replace() missing 1 required positional argument: 'replacement'` — DataFrame/Column replace signature |
| **regexp_extract_all + list** | ~5+ | `TypeError: unhashable type: 'list'` — select/expr with list (regexp_extract_all) |
| **head() return type** | ~2 | `AttributeError: 'list' object has no attribute 'collect'` — head() returns list, test expects DataFrame |
| **orderBy direction** | ~2+ | `assert [1,2,3] == [3,2,1]` — ascending=False / order direction |
| **DESCRIBE EXTENDED** | ~1 | `Table or view 'EXTENDED' not found` — DESCRIBE EXTENDED parsed as table name |
| **approx_count_distinct** | ~2 | `KeyError: 'approx_count_distinct(value)'` — agg result column naming |
| **Struct/Array/Map types** | ~10+ | withField null, elementtype, map/struct from JSON, ArrayType nullable |
| **first / ignorenulls** | ~2+ | `assert 'A' is None` — first() default vs ignorenulls |
| **Row format (tuple vs dict)** | ~10+ | `assert [(1,10,1,20)] == [{'id': 1, ...}]` — collect returns tuple-like; test expects dict |
| **DDL / createDataFrame** | ~10+ | `KeyError: 'name'`, `'list' object is not callable` — DDL schema parsing, create_data_frame API |
| **Misc (pow, union, spark context, etc.)** | ~20+ | pow/bitwise, union DataFrame-like, spark_context.version, substring_index, xxhash64, etc. |

---

## High Priority

### 1. CSV read options: bool vs string
- [x] Accept Python `bool` for options like `inferSchema`, `header` in read_csv
- [x] Convert bool to string ("true"/"false") before passing to Rust bindings
- **Affected:** ~60 tests in `test_inferschema_parity.py`
- **Error:** `TypeError: argument 'value': 'bool' object cannot be converted to 'PyString'`

### 2. `when()` API mismatch
- [x] Support `when(condition, value)` in addition to `when(condition).then(value)`
- **Affected:** ~15 tests in `test_casewhen_windowfunction_cast.py`, `test_withfield.py`, `test_create_map.py`
- **Error:** `TypeError: when() takes 1 positional arguments but 2 were given`

### 3. `orderBy` with Column expressions
- [x] Accept Column expressions in `orderBy()` (e.g. `F.col("x").desc_nulls_last()`)
- [x] Add overload or `sort()` that accepts Column objects
- **Affected:** ~25 tests in `test_column_ordering.py`, `test_column_substr.py`, `test_chained_arithmetic.py`
- **Error:** `TypeError: orderBy() expects column names as str or list/tuple[str]`

### 4. `cast()` type object support
- [x] Accept type objects (e.g. `IntegerType()`) in `cast()`, not just strings
- [x] Convert via `type_obj.simpleString()` or equivalent before passing to Rust
- **Affected:** ~15 tests in `test_issue_453_alias_cast_withcolumn.py`, `test_withfield.py`
- **Error:** `TypeError: argument 'type_name': 'IntegerType' object cannot be converted to 'PyString'`

---

## Medium Priority

### 5. `na.fill` vs `fillna` API
- [x] Add `df.na` property returning object with `fill()` method
- [x] Delegate `na.fill(...)` to `fillna(...)`
- **Affected:** ~25 tests in `test_na_fill.py`, `test_na_fill_robust.py`
- **Error:** `AttributeError: 'builtin_function_or_method' object has no attribute 'fill'`

### 6. `fillna` type preservation
- [x] Preserve numeric types when filling (e.g. fill with 0, not "0")
- [x] Cast fill value to column dtype before applying
- **Affected:** ~15 tests in `test_fillna_subset.py`
- **Error:** `AssertionError: assert '0' == 0`

### 7. `fillna` subset parameter
- [x] Accept `subset="col"` (single string) as well as `subset=["col"]`
- **Affected:** 2 tests
- **Error:** `TypeError: argument 'subset': Can't extract 'str' to 'Vec'`

### 8. Column `astype` method
- [x] Add `astype(dtype)` as alias for `cast(dtype)` on PyColumn
- **Affected:** ~20 tests in `test_column_astype.py`
- **Error:** `AttributeError: 'builtins.PyColumn' object has no attribute 'astype'`

### 9. Nulls-ordering methods on Column
- [x] Add `asc_nulls_first`
- [x] Add `asc_nulls_last`
- [x] Add `desc_nulls_first`
- **Note:** `desc_nulls_last` exists
- **Affected:** ~20 tests in `test_column_ordering.py`
- **Error:** `AttributeError: 'builtins.PyColumn' object has no attribute 'asc_nulls_first'`

---

## Lower Priority

### 10. Map column subscript
- [x] Implement `col["key"]` for map columns (PyColumn `__getitem__`); get_item_camel accepts PyColumn key
- **Affected:** ~5 tests in `test_issue_441_map_column_subscript.py`, `test_issue_440_create_map_list.py`
- **Note:** Some failures remain (backend map repr as struct / UDF output type).

### 11. Pivot API methods
- [x] Add `count_distinct`, `collect_list`, `collect_set`, `first`, `last`, `stddev`, `variance`, `mean`, `agg` to PyPivotedGroupedData
- **Affected:** ~10 tests in `test_pivot_grouped_data.py` — all 16 pivot tests pass.

### 12. Join `left_on` / `right_on`
- [x] Support join with `left_on` and `right_on` (different column names); `join(other, on=None, how=..., left_on=None, right_on=None)`
- **Affected:** ~2 tests in `test_join_type_coercion.py`
- **Note:** Condition form `join(other, left_col == right_col)` still unsupported.

### 13. Struct type support in createDataFrame
- [x] Support nested struct types in `create_dataframe_from_rows`
- [x] Fix `json_values_to_series: unsupported type 'struct<...'` (bracket-aware `parse_struct_fields` for nested `struct<...>`)
- **Affected:** ~15 tests in `test_withfield.py`, `test_array_type_robust.py`
- **Note:** Some withfield tests still fail: schema reporting (StructType vs StringType after withColumn) and null struct as `None` vs `{field: None, ...}`.

### 14. `array()` with literals
- [x] Support `array(1, 2, 3)` and `array([1, 2, 3])` (literals, not just Columns)
- **Affected:** ~8 tests in `test_array_parameter_formats.py`
- **Note:** A few failures remain from mixed-type array elements being stringified on collect (row serialization), not the array() API.

### 15. `create_map` type preservation
- [x] Preserve numeric types in map values (not stringify) — map value strings parsed as JSON on collect when dtype was unified to String
- **Affected:** ~3 tests in `test_create_map.py` — all 43 create_map tests pass.

### 16. BinaryType
- [x] Add `BinaryType` to spark_types and sql/types; infer bytes as binary in createDataFrame; schema exposes BinaryType
- **Affected:** 1 test — `test_create_dataframe_with_bytes` passes.

---

## Missing Functions (Phase 4)

Export or implement in `sparkless.sql.functions`:

- [x] `approx_count_distinct` (~8 tests) — Phase 4: exposed in F + native
- [x] `date_trunc` (~4 tests) — Phase 4: exposed (alias for trunc)
- [x] `first` (aggregate) (~20 tests) — Phase 4: exposed for groupBy().agg()
- [x] `translate` (~1 test) — Phase 4: exposed
- [x] `substring_index` (~1 test) — Phase 4: exposed
- [x] `crc32` (~1 test) — Phase 4: exposed
- [x] `xxhash64` (~1 test) — Phase 4: exposed
- [x] `get_json_object` (~1 test) — Phase 4: exposed
- [x] `json_tuple` (~1 test) — Phase 4: exposed (F.json_tuple(col, *keys))
- [x] `size` (~2 tests) — Phase 4: exposed (alias for array_size)
- [x] `array_contains` (~1 test) — Phase 4: exposed; value can be column or literal
- [x] `explode` (~1 test) — Phase 4: exposed (`posexplode` already existed)

---

## Known Limitations (Consider Skipping)

### UDF
- [ ] UDF not implemented — consider skipping `test_udf_comprehensive.py` (~20 tests)
- **Error:** `NotImplementedError: udf is not yet implemented in robin-sparkless`

### SQL / DML
- [ ] SQL join types: only INNER, LEFT, RIGHT, FULL, LEFT SEMI, LEFT ANTI, CROSS supported
- [x] UPDATE and DELETE supported (single table; modify table in session catalog; return empty DataFrame)
- **Affected:** `test_sql_update.py` — passes with robin backend

---

## Validation / DID NOT RAISE

Tests expecting errors that are not raised:

- [ ] `test_datetime_functions_require_session` — expect RuntimeError
- [ ] `test_to_date_requires_string` — expect TypeError
- [ ] `test_mixed_int_float_raises_error` — expect TypeError
- [ ] `test_create_dataframe_with_all_null_column` — expect ValueError
- [ ] `test_create_dataframe_type_promotion_int_to_float` — expect TypeError
- [ ] `test_tuple_data_empty_schema` — expect SparklessError

---

## Other / Edge Cases (Phase 7)

- [x] `substr(1, 0)` with null: returns `''` instead of `None` (test_substr) — Phase 7.1: preserve null in substr when length < 1
- [x] `soundex` null/empty: returns `'0000'` instead of `''` (test_issue_189_string_functions_robust) — Phase 7.2: empty string -> ''
- [x] ArrayType `element_type` equality: `StringType()` vs `StringType()` instance comparison (test_array_type_keywords) — Phase 7.3: DataType.__eq__
- [ ] ArrayType `nullable` / `containsNull` handling (test_array_type_robust; other array-type gaps remain)
- [x] `test_array_type_issue_247_example`: `float parse: invalid float literal` — Phase 7.4: use schema field order for createDataFrame dict rows (column_order from explicit schema)
- [x] `test_create_table_as_select`: returns 0 rows instead of 1 — Phase 7.5: CTAS run query and register result
- [x] `test_issue_270_tuple_dataframe`: AttributeError `data`, `StructType` schema handling — Phase 6 (tuple+empty schema raises)
- [x] `test_issue_355`: UnionByName type handling — passes with Robin backend
- [x] `test_first_method`: `first()` returns DataFrame/Column instead of row or None — Phase 7.8: first() returns Option[Row] (None for empty)
- [x] `test_column_case_variations`: 32 pass, 2 fail; remaining: groupBy().agg(F.count("*").alias("count")) triggers Polars "duplicate: column with name 'count'" (plan/sink path)

---

## Progress Tracking

| Category              | Total | Fixed | Remaining |
|-----------------------|-------|-------|-----------|
| CSV options           | ~60   | ~60   | 0         |
| when() API            | ~15   | ~15   | 0         |
| orderBy               | ~25   | ~25   | 0         |
| cast() type           | ~15   | ~15   | 0         |
| na.fill               | ~25   | ~25   | 0         |
| fillna types          | ~15   | ~15   | 0         |
| astype               | ~20   | ~20   | 0         |
| nulls-ordering       | ~20   | ~20   | 0         |
| Map subscript        | ~5    | 0     | ~5        |
| Pivot methods        | ~10   | 0     | ~10       |
| Missing functions    | ~50   | 0     | ~50       |
| UDF                   | ~20   | 0     | ~20       |
| Other                 | ~200+ | 0     | ~200+     |

---

*Generated from test failure analysis. Update as fixes are applied.*
