**Supersedes:** #1085 (logical group 12)

**Theme:** Isin negation, between on string column, log with float constant, date vs datetime comparison, string arithmetic.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_369_isin_negation.py::TestIssue369IsinNegation::test_negation_isin_show`
- `tests/upstream_sparkless/tests/test_issue_369_isin_negation.py::TestIssue369IsinNegation::test_negation_isin_string_column_int_list`
- `tests/upstream_sparkless/tests/test_issue_369_isin_negation.py::TestIssue369IsinNegation::test_negation_isin_string_to_string`
- `tests/upstream_sparkless/tests/test_issue_445_between_string_column_numeric_bounds.py::test_between_string_column_not_between`
- `tests/upstream_sparkless/tests/test_issue_329_log_float_constant.py::TestIssue329LogFloatConstant::test_log_with_column_base`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_date_datetime_with_and`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_date_eq_datetime`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_date_less_than_datetime`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_date_lte_datetime`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_date_ne_datetime`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_datetime_less_than_date`
- `tests/upstream_sparkless/tests/test_issue_431_date_datetime_comparison.py::test_exact_scenario_from_issue_431`
- `tests/upstream_sparkless/tests/unit/dataframe/test_string_arithmetic.py::TestStringArithmetic::test_string_arithmetic_chained_operations`
- `tests/upstream_sparkless/tests/unit/dataframe/test_string_arithmetic.py::TestStringArithmetic::test_string_arithmetic_complex_expression`
- `tests/upstream_sparkless/tests/unit/dataframe/test_string_arithmetic.py::TestStringArithmetic::test_string_arithmetic_with_string_column`
