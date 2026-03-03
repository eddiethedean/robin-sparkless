**Supersedes:** #1085 (logical group 14)

**Theme:** get_json_object, regexp_extract_all, substring_index, xxhash64 edge cases (issue 189 robust).

## Tests to fix

- `tests/upstream_sparkless/tests/unit/functions/test_issue_189_string_functions_robust.py::TestIssue189StringFunctionsRobust::test_get_json_object_missing_path_and_invalid_json`
- `tests/upstream_sparkless/tests/unit/functions/test_issue_189_string_functions_robust.py::TestIssue189StringFunctionsRobust::test_regexp_extract_all_multiple_matches_and_nulls`
- `tests/upstream_sparkless/tests/unit/functions/test_issue_189_string_functions_robust.py::TestIssue189StringFunctionsRobust::test_substring_index_edge_cases`
- `tests/upstream_sparkless/tests/unit/functions/test_issue_189_string_functions_robust.py::TestIssue189StringFunctionsRobust::test_xxhash64_known_values_and_null`
