**Supersedes:** #1085 (logical group 2)

**Theme:** Join then groupby column resolution; join result select with different column name casing; compound conditions; table-prefixed SQL.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_280_join_groupby_ambiguity.py::TestJoinThenGroupByNoAmbiguity::test_outer_join_then_groupby`
- `tests/upstream_sparkless/tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferentCaseSelect::test_chained_operations_after_select`
- `tests/upstream_sparkless/tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferentCaseSelect::test_different_join_types`
- `tests/upstream_sparkless/tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferentCaseSelect::test_drop_after_select`
- `tests/upstream_sparkless/tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferentCaseSelect::test_join_different_case_select_third_case`
- `tests/upstream_sparkless/tests/test_issue_297_join_different_case_select.py::TestIssue297JoinDifferentCaseSelect::test_multiple_matches_uses_requested_name`
- `tests/upstream_sparkless/tests/test_issues_376_382_robust.py::test_robust_join_compound_condition`
- `tests/upstream_sparkless/tests/test_issues_376_382_robust.py::test_robust_sql_group_by_table_prefixed`
- `tests/upstream_sparkless/tests/test_issues_376_382_robust.py::test_robust_sql_three_joins_select_third_table`
- `tests/upstream_sparkless/tests/test_issues_376_382_robust.py::test_robust_sql_where_table_prefixed`
- `tests/upstream_sparkless/tests/unit/dataframe/test_join_type_coercion.py::TestJoinTypeCoercion::test_join_with_left_on_right_on`
- `tests/upstream_sparkless/tests/unit/dataframe/test_join_type_coercion.py::TestJoinTypeCoercionParity::test_pyspark_parity_left_on_right_on_different_names`
