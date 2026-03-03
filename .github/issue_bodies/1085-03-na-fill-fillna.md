**Supersedes:** #1085 (logical group 3)

**Theme:** `na.fill` (dict, subset), `fillna` (float, subset, type mismatch), `na.drop` subset. Match PySpark behavior.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_359_na_drop.py::TestIssue359NADrop::test_na_drop_subset_as_string`
- `tests/upstream_sparkless/tests/test_issue_422_fillna_float.py::TestIssue422FillnaFloat::test_fillna_float_chained_fillna`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill.py::TestNaFill::test_na_fill_dict`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill.py::TestNaFill::test_na_fill_nonexistent_column`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_dict_ignores_subset`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_large_dict`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_partial_dict`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_preserves_non_null_values`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_schema_preservation`
- `tests/upstream_sparkless/tests/unit/dataframe/test_na_fill_robust.py::TestNaFillRobust::test_na_fill_type_mismatch_silently_ignored`
- `tests/upstream_sparkless/tests/unit/dataframe/test_fillna_subset.py::TestFillnaSubset::test_fillna_subset_type_mismatch_boolean_column_string_fill`
- `tests/upstream_sparkless/tests/unit/dataframe/test_fillna_subset.py::TestFillnaSubset::test_fillna_subset_type_mismatch_string_column_int_fill`
