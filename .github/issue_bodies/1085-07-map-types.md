**Supersedes:** #1085 (logical group 7)

**Theme:** `create_map` from arrays/lists and map column subscript `map_col[key]` (issues 440, 441).

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_440_create_map_list.py::test_create_map_list_exact_issue_440`
- `tests/upstream_sparkless/tests/test_issue_440_create_map_list.py::test_create_map_list_map_lookup_key_not_found`
- `tests/upstream_sparkless/tests/test_issue_440_create_map_list.py::test_create_map_list_six_pairs`
- `tests/upstream_sparkless/tests/test_issue_440_create_map_list.py::test_create_map_list_then_filter`
- `tests/upstream_sparkless/tests/test_issue_440_create_map_list.py::test_create_map_list_with_null_value_in_map`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_chained_with_columns`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_coalesce_default`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_create_map_with_column_key`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_in_select`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_key_not_found`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_multiple_in_select`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_null_key_returns_null`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_orderby_result`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_then_filter`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_when_otherwise`
- `tests/upstream_sparkless/tests/test_issue_441_map_column_subscript.py::test_map_column_subscript_with_column_key_exact_issue_441`
