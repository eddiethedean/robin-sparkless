**Supersedes:** #1085 (logical group 6)

**Theme:** `alias`/`cast` in `select` and `withColumn` (issues 435, 453, 436). Concat cast string.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_column_with_underscore`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_double_type`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_long_type`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_mixed_with_plain_select`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_multiple_columns`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_select_exact_issue_435`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_string_type`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_then_filter`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_then_select_subset`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_alias_cast_with_nulls`
- `tests/upstream_sparkless/tests/test_issue_435_alias_cast_select.py::test_cast_without_alias_still_works`
- `tests/upstream_sparkless/tests/test_issue_453_alias_cast_withcolumn.py::test_alias_cast_select_still_works`
- `tests/upstream_sparkless/tests/test_issue_436_concat_cast_string.py::test_concat_literal_cast_string_exact_issue_436`
