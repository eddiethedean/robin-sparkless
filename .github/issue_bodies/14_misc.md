## Category: Misc (single-file or few failures each)

Various failures: format string null, DML/INSERT, CTE self-join duplicate name, Delta schema evolution, notebooks, substr alias, array literal collect, unionByName diamond, first/ignore nulls, pandas column order, select with list/tuple.

### Reproduction examples

- Format string with null: format_string("%s-%s", col("a"), col("b")) when b is null -> expect "null-456" style.
- Substr alias: df.columns or schema list not callable (test_issue_200_substr_alias).
- Array literal: createDataFrame with array column; collect returns "[1,2]" string vs [1, 2] list (test_issue_256_create_dataframe_array_column).
- unionByName diamond: column type or doubled value (test_issue_355).
- First/ignore nulls: first() with ignorenulls (test_first_ignorenulls).
- Select with list: df.select([col("a"), col("b")]) schema/order (test_issue_202_select_with_list).
- describe extended parsed as table name (test_describe_extended).
- String concat cache (test_issue_188_string_concat_cache).
- to_timestamp returns None (test_issue_153_to_timestamp_returns_none).
- List rows with None (test_issue_200_list_rows_with_column_schema).
- DML, CTE, Delta, notebooks: test_dml, test_sql_cte_robust, test_delta_lake_schema_evolution, test_notebooks.

### Tests to pass or fix (representative)

- tests/upstream_sparkless/tests/parity/functions/test_format_string_parity.py::test_format_string_with_null_parity
- tests/upstream_sparkless/tests/parity/sql/test_dml.py
- tests/upstream_sparkless/tests/unit/session/test_sql_cte_robust.py::test_cte_with_self_join
- tests/python/test_issue_200_substr_alias.py
- tests/python/test_issue_256_create_dataframe_array_column.py
- tests/upstream_sparkless/tests/unit/test_issue_355.py
- tests/upstream_sparkless/tests/unit/functions/test_first_ignorenulls.py
- tests/upstream_sparkless/tests/test_issue_202_select_with_list.py
- tests/upstream_sparkless/tests/test_issue_153_to_timestamp_returns_none.py
- tests/upstream_sparkless/tests/test_issue_200_list_rows_with_column_schema.py
- tests/upstream_sparkless/tests/test_issue_188_string_concat_cache.py
- tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py
- tests/upstream_sparkless/tests/test_notebooks.py

**Action:** Fix or skip per subcategory; see docs/test_failure_categories.md for full list.
