# Test failure categories

Summary of **~605** failing tests (from `pytest tests/ -n 10` with package installed via maturin in `.venv`), grouped by cause/area.

---

## 1. Missing native API / bindings (≈33+ failures)

**Error pattern:** `AttributeError: module 'sparkless._native' has no attribute '...'`

| Missing API | Affected area | Test files (examples) |
|-------------|----------------|------------------------|
| `arrays_overlap` | Array contains / join | `test_array_contains_join_parity.py`, `test_issue_331_array_contains_join.py` |

**Action:** Implement and expose `arrays_overlap` (and any other missing array/join helpers) in the Rust extension and Python bindings.

---

## 2. SQL: SHOW / DESCRIBE not supported (≈7 failures)

**Error pattern:** `SQL: only SELECT, CREATE SCHEMA/DATABASE, DROP TABLE/VIEW/SCHEMA/DATABASE, and DESCRIBE are supported`

| Unsupported | Tests |
|-------------|--------|
| `SHOW DATABASES` | `test_show_describe.py::test_show_databases` |
| `SHOW TABLES` | `test_show_tables`, `test_show_tables_in_database` |
| `DESCRIBE EXTENDED` (parsed as table name) | `test_describe_extended` |

**Action:** Either extend SQL parser/execution to support these SHOW/DESCRIBE forms, or mark/skip these tests for robin backend.

---

## 3. Schema / createDataFrame / DDL (≈80+ failures)

**Error patterns:**  
`TypeError: 'list' object is not callable`, `KeyError: 'name'`, `KeyError: 'a'`,  
`AssertionError: expected TypeError`, schema type mismatches (IntegerType vs LongType),  
`TypeError: row 0: expected dict, list, or tuple`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| DDL schema parsing | DDL string parsing or schema object construction differs | `test_issue_372_create_data_frame.py`, `test_issue_418_nested_ddl.py` |
| Infer schema parity | Infer schema result (column types/order) differs from expected | `test_inferschema_parity.py` (44 failures) |
| Single-type createDataFrame | Row type / schema handling for single-type DataFrames | `test_issue_213_createDataFrame_with_single_type.py` |
| Pandas/schema type | IntegerType vs LongType when creating from pandas with schema | `test_issues_225_231.py::TestIssue229PandasDataFrameSupport` |

**Action:** Align DDL parsing, `createDataFrame` (list/dict/Row, schema, pandas) and inferred schema with PySpark/upstream expectations; unify IntegerType vs LongType where required.

---

## 4. Type / cast API and semantics (≈20+ failures)

**Error patterns:**  
`AttributeError: 'IntegerType' object has no attribute 'simpleString'`,  
`_native.SparklessError: conversion from str to i32/i64 failed`,  
assertions on cast result (e.g. null vs error for invalid string)

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| DataType.simpleString | Missing or different on IntegerType, LongType, StringType, etc. | `test_issue_394_cast_data_type.py` |
| String→int cast | Strict conversion (error instead of null for bad/invalid strings) | `test_issue_217_string_to_int_cast.py`, upstream cast tests |
| Cast/alias select | Result value or column name after cast | `test_cast_alias_select_parity.py` |

**Action:** Add or fix `simpleString()` on all DataType subclasses; decide and implement string→int semantics (null vs error) to match PySpark/upstream.

---

## 5. Join semantics and API (≈30+ failures)

**Error patterns:**  
`AssertionError: DataFrames are not equivalent`,  
`AssertionError: assert [(1, 10, 1, 20)] == [{'id': 1, 'v': 10, 'w': 20}]` (row shape/keys),  
column name / duplicate name errors

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| Outer / semi / anti / cross | Join type semantics or result schema/rows | `test_join.py::test_outer_join`, `test_semi_join`, `test_anti_join`, `test_cross_join` |
| Join on Column | Accept column expression in join condition; result columns | `test_issue_353_join_on_accept_column.py` |
| Join column names / aliases | Duplicate or mismatched names after join | `test_issue_374_join_aliased_columns.py`, `test_issue_421_join_column_names.py` |
| Left semi | LeftSemi join behavior | `test_issue_438_leftsemi_join.py` |

**Action:** Align join types (outer/semi/anti/cross) and “join on Column” behavior with PySpark; fix column naming/aliasing in join plans.

---

## 6. NA / replace API and semantics (≈20+ failures)

**Error patterns:**  
`TypeError: replace() missing 1 required positional argument: 'value'`,  
`TypeError: PyColumn.replace() missing 1 required positional argument: 'replacement'`,  
`_native.SparklessError: cannot compare string with numeric type`,  
assertions on fill/replace result (e.g. `assert '1' == 1`)

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| DataFrameNaFunctions.replace | Signature or behavior (to_replace + value) | `test_issue_360_na_replace.py`, `test_issue_287_na_replace.py`, `test_issue_379_column_replace_dict_list.py` |
| eqNullSafe / null comparison | Type coercion or comparison with null/numeric | `test_issue_248_column_eq_null_safe.py`, `test_issue_260_eq_null_safe.py` |
| Fill/NA doc example | Default or fill value (e.g. `assert None == 0`) | `test_doc_examples.py::test_user_guide_na_fill_drop` |

**Action:** Implement or fix `DataFrameNaFunctions.replace` and Column `replace` signature/behavior; align eqNullSafe and NA fill with expected types and values.

---

## 7. Order by / sort (≈15+ failures)

**Error patterns:**  
`assert [1, 2, 3] == [3, 2, 1]`,  
`TypeError: orderBy() expects column names as str or Column/SortOrder expressions`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| ascending=False | Single-column or list sort with `ascending=False` not applied | `test_issue_378_order_by_ascending_bool.py`, `test_issue_327_orderby_ascending.py` |
| orderBy(list) | orderBy with list of columns | `test_issue_335_window_orderby_list.py`, `test_issue_415_orderby_list.py`, parity window tests |

**Action:** Ensure orderBy accepts Column/SortOrder and boolean `ascending`; support list of columns and parity with PySpark order.

---

## 8. Aggregates / groupby (≈25+ failures)

**Error patterns:**  
`KeyError: 'avg(Value)'`, `KeyError: 'approx_count_distinct(value)'`,  
`_native.SparklessError: duplicate: column with name 'count' has more than one occurrence`,  
`AssertionError: assert ('max(salary)' in ... or 'max_salary' in ...)`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| Aggregate column naming | Alias for agg (e.g. `max(salary)` vs `max_salary`) | `test_first_method.py::test_first_after_groupby_agg`, `test_issue_397_groupby_alias.py` |
| approx_count_distinct | Missing or different API/semantics | `test_approx_count_distinct_rsd.py`, parity |
| Duplicate column names | Multiple aggs producing same default name | `test_column_case_variations.py`, `test_issue_286_aggregate_function_arithmetic.py` |
| sum/mean on string column | Error vs null/behavior | `test_issue_393_sum_string_column.py`, `test_issue_437_mean_string_column.py` |

**Action:** Align aggregate expression naming and aliasing; implement or fix `approx_count_distinct`; avoid duplicate output names in groupby/agg.

---

## 9. String / array / JSON functions (≈50+ failures)

**Error patterns:**  
`AssertionError: DataFrames are not equivalent` (string/array results),  
`AssertionError: assert 'a' == ''`, `assert 7148569436472236994 == 8557436188178888239` (xxhash64),  
`_native.SparklessError: user error: list.eval operation not supported for dtype str`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| Levenshtein / xxhash64 / get_json_object / json_tuple | Result value or null handling | `test_string.py` (parity), `test_issue_189_string_functions_robust.py` |
| substring_index / regexp_extract | Edge cases or multiple matches | `test_issue_189_string_functions_robust.py` |
| split limit | Split with limit parameter | `test_issue_328_split_limit.py`, split_limit_parity |
| Array contains / explode / posexplode | Alias, lengths, or result schema | `test_issue_293_explode_withcolumn.py`, `test_issue_366_alias_posexplode.py`, `test_issue_429_posexplode_no_alias.py`, `test_issue_430_posexplode_alias_execution.py` |
| Array type / map type | Unsupported map/struct in array or JSON | `test_array_type_robust.py`, `test_issue_339_column_subscript.py`, `test_issue_441_map_column_subscript.py` |

**Action:** Align string/array/JSON function results and null handling with PySpark; fix split limit, explode/posexplode aliasing and length handling; extend support for map/struct in arrays where needed.

---

## 10. Struct / withField / getField (≈25+ failures)

**Error patterns:**  
`_native.SparklessError: field not found: E1`, `field not found: k`, `field not found: Value`,  
`AssertionError: assert None == 1`, `Expected struct for nested access 'StructValue'`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| Struct field alias | Alias after struct field selection | `test_issue_330_struct_field_alias.py`, `test_struct_field_alias_parity.py` |
| withField | Struct update / withField semantics | `test_withfield.py`, `test_issue_398_withfield_window.py` |
| getField / subscript | Nested field access on struct/column | `test_issue_358_getfield.py`, `test_issue_339_column_subscript.py` |

**Action:** Align struct field resolution (names/case), alias propagation, and withField/getField behavior with PySpark.

---

## 11. Window functions (≈25+ failures)

**Error patterns:**  
`assert 100 == 90`, `_native.SparklessError: duplicate: ... output name`,  
`_native.SparklessError: expected leading integer in the duration string, found 'm'` (date_trunc)

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| Window orderBy list | orderBy as list in window spec | `test_window_orderby_list_parity.py`, `test_issue_335_window_orderby_list.py` |
| date_trunc | Duration string format (e.g. '1 month' vs 'm') | `test_date_trunc_robust.py`, `test_date_trunc_polars_backend.py` |
| Row number / rank over descending | Order or frame | `test_issue_414_row_number_over_descending.py` |
| Window + arithmetic / withField | Combined window and struct/arithmetic | `test_issue_398_withfield_window.py`, `test_window_arithmetic.py` |

**Action:** Support window orderBy as list; align date_trunc duration parsing; fix window frame/ordering and duplicate column names in window plans.

---

## 12. Pow / coalesce / head / SparkContext / union(DataFrame-like) (≈15+ failures)

**Error patterns:**  
`TypeError: unsupported operand type(s) for ** or pow(): 'builtins.PyColumn' and 'int'`,  
`AttributeError: collect`, `AttributeError: 'list' object has no attribute 'collect'`,  
`TypeError: 'str' object is not callable`,  
`TypeError: argument 'other': '...Wrapper' object cannot be converted to 'PyDataFrame'`

| Subcategory | Cause | Test files (examples) |
|-------------|--------|------------------------|
| pow / ** | Column ** int not implemented or delegated to wrong path | `test_issue_405_pow_bitwise.py` |
| coalesce | Return type (e.g. int vs string) or variadic behavior | `test_issue_407_coalesce_variadic.py` |
| head() | DataFrame.head() returns list instead of DataFrame or missing .collect() | `test_issue_413_head.py` |
| SparkContext.version | API (property vs callable) | `test_issue_387_spark_context.py`, `test_sparkcontext_validation.py` |
| union(DataFrame-like) | Accept duck-typed “DataFrame-like” in union | `test_issue_385_union_dataframe_like.py` |

**Action:** Implement Column ** int (or route to correct native pow); fix coalesce result types; implement head() and SparkContext.version; consider accepting DataFrame-like in union or document limitation.

---

## 13. Error message wording (≈4 failures)

**Error pattern:** Tests expect substring `'cannot resolve'` in error message; robin emits `'not found: column ...'`.

| Tests | Expectation |
|-------|-------------|
| `test_issue_158_dropped_column_error.py` | Message contains `'cannot resolve'` |

**Action:** Either add `'cannot resolve'` (or equivalent) to error text for “column not found” cases, or relax test to accept current message.

---

## 14. Misc (single-file or few failures each)

- **Format string / null:** `test_format_string_parity.py` (null formatting).
- **DML / INSERT:** `test_dml.py` (if INSERT not supported).
- **CTE / self-join:** Duplicate output name in CTE (`test_sql_cte_robust.py`).
- **Delta / schema evolution:** `test_delta_lake_schema_evolution.py`.
- **Notebooks:** `test_notebooks.py`.
- **Substr alias:** `test_issue_200_substr_alias.py` (list not callable).
- **Array literal/collect:** `test_issue_256_create_dataframe_array_column.py` (e.g. `'[1,2]'` vs `[1, 2]`).
- **unionByName diamond:** `test_issue_355.py` (column type/doubled value).
- **First/ignore nulls:** `test_first_ignorenulls.py`.
- **Pandas column order:** `test_issue_372_pandas_column_order.py`.
- **Select with list/tuple:** Schema or column order after `select([...])` (`test_issue_202_select_with_list.py`).

---

## Counts by test location (top 15)

| Count | Path (prefix) |
|-------|----------------|
| 44 | `tests/upstream_sparkless/tests/unit/dataframe/test_inferschema_parity.py` |
| 30 | `tests/upstream_sparkless/tests/test_issue_331_array_contains_join.py` |
| 29 | `tests/upstream_sparkless/tests/test_issue_339_column_subscript.py` |
| 28 | `tests/upstream_sparkless/tests/test_issue_293_explode_withcolumn.py` |
| 26 | `tests/upstream_sparkless/tests/test_issue_295_withColumnRenamed_nonexistent.py` |
| 17 | `tests/upstream_sparkless/tests/test_issue_434_ltrim_rtrim_in_expr.py` |
| 15 | `tests/upstream_sparkless/tests/test_issue_330_struct_field_alias.py` |
| 13 | `tests/upstream_sparkless/tests/test_issue_413_union_createDataFrame.py` |
| 13 | `tests/upstream_sparkless/tests/test_issue_328_split_limit.py` |
| 11 | `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py` |
| 11 | `tests/upstream_sparkless/tests/test_issue_437_mean_string_column.py` |
| 10 | `tests/upstream_sparkless/tests/test_issue_397_groupby_alias.py` |
| 10 | `tests/upstream_sparkless/tests/test_issue_393_sum_string_column.py` |
| 10 | `tests/upstream_sparkless/tests/test_issue_366_alias_posexplode.py` |
| 10 | `tests/upstream_sparkless/tests/parity/functions/test_array.py` |

---

## Suggested priority

1. **High impact / many failures:** infer schema parity (44), array_contains_join / arrays_overlap (30+), column subscript (29), explode/withColumn (28), withColumnRenamed (26).
2. **API gaps:** SHOW/DESCRIBE, replace/NA, orderBy(ascending=False), head(), pow(Column, int), approx_count_distinct, DataFrame-like union.
3. **Type/schema:** DataType.simpleString, IntegerType vs LongType, createDataFrame DDL and row types.
4. **Join/aggregate naming:** Join result shape/names, aggregate aliases, duplicate column names.
5. **String/array/struct:** Levenshtein, xxhash64, split limit, struct field alias, withField, date_trunc.
