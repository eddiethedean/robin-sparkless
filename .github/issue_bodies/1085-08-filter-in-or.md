**Supersedes:** #1085 (logical group 8)

**Theme:** `filter` with `Column.isin(...)` combined with OR; filter isinstance ordering (parity).

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_419_filter_in_or_parse_exception.py::TestIssue419FilterInOrParseException::test_filter_in_and_condition`
- `tests/upstream_sparkless/tests/test_issue_419_filter_in_or_parse_exception.py::TestIssue419FilterInOrParseException::test_filter_in_multiple_values_or`
- `tests/upstream_sparkless/tests/test_issue_419_filter_in_or_parse_exception.py::TestIssue419FilterInOrParseException::test_filter_in_or_empty_result`
- `tests/upstream_sparkless/tests/test_issue_419_filter_in_or_parse_exception.py::TestIssue419FilterInOrParseException::test_filter_in_or_string_literal_type_coercion_exact_issue`
- `tests/upstream_sparkless/tests/test_issue_419_filter_in_or_parse_exception.py::TestIssue419FilterInOrParseException::test_filter_in_or_with_show`
- `tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_logical_operations_in_filters`
