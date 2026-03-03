**Supersedes:** #1085 (logical group 9)

**Theme:** Window functions with comparison/null handling; orderBy as list; cast in window expressions.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctionComparison::test_window_function_comparison_with_dense_rank`
- `tests/upstream_sparkless/tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctionComparison::test_window_function_comparison_with_isnotnull`
- `tests/upstream_sparkless/tests/test_issue_336_window_function_comparison.py::TestIssue336WindowFunctionComparison::test_window_function_comparison_with_null_values`
- `tests/upstream_sparkless/tests/test_issue_335_window_orderby_list.py::TestIssue335WindowOrderByList::test_window_orderby_list_with_stddev_variance`
- `tests/upstream_sparkless/tests/unit/dataframe/test_casewhen_windowfunction_cast.py::TestWindowFunctionCast::test_window_function_cast_in_select`
- `tests/upstream_sparkless/tests/unit/dataframe/test_casewhen_windowfunction_cast.py::TestWindowFunctionCast::test_window_function_cast_to_long`
- `tests/upstream_sparkless/tests/unit/dataframe/test_casewhen_windowfunction_cast.py::TestWindowFunctionCast::test_window_function_cast_with_datatype_object`
- `tests/upstream_sparkless/tests/unit/dataframe/test_casewhen_windowfunction_cast.py::TestWindowFunctionCast::test_window_function_cast_with_partition`
