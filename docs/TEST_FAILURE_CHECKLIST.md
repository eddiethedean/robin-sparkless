# Test Failure Checklist

**Last run:** pytest -n 10  
**Result:** 2105 failed, 1423 passed, 66 skipped

This checklist tracks fixes needed to reduce test failures. Items are grouped by category and ordered by estimated impact.

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
- [ ] UPDATE and DELETE not supported
- **Affected:** ~5 tests in `test_sql_cte_robust.py`, `test_sql_update.py`

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

## Other / Edge Cases

- [ ] `substr(1, 0)` with null: returns `''` instead of `None` (test_substr)
- [ ] `soundex` null/empty: returns `'0000'` instead of `''` (test_issue_189_string_functions_robust)
- [ ] ArrayType `element_type` equality: `StringType()` vs `StringType()` instance comparison (test_array_type_keywords)
- [ ] ArrayType `nullable` / `containsNull` handling (test_array_type_robust)
- [ ] `test_array_type_issue_247_example`: `float parse: invalid float literal`
- [ ] `test_create_table_as_select`: returns 0 rows instead of 1
- [ ] `test_issue_270_tuple_dataframe`: AttributeError `data`, `StructType` schema handling
- [ ] `test_issue_355`: UnionByName type handling
- [ ] `test_first_method`: `first()` returns DataFrame/Column instead of row or None
- [ ] `test_column_case_variations`: various case-sensitivity and API gaps

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
