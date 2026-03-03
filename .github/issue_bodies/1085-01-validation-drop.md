**Supersedes:** #1085 (logical group 1)

**Theme:** Correct behavior and validation when columns are dropped or renamed, then used in filters/transforms.

**Example (fails):** `df.drop("a").filter(F.col("b") > 0)` may return 0 rows vs expected.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_136_column_rename_validation.py::TestIssue136ColumnRenameValidation::test_column_rename_and_transform_with_filter`
- `tests/upstream_sparkless/tests/test_issue_136_column_rename_validation.py::TestIssue136ColumnRenameValidation::test_rename_then_add_column_then_filter`
- `tests/upstream_sparkless/tests/test_issue_138_column_drop_reference.py::TestIssue138ColumnDropReference::test_drop_column_after_transform`
- `tests/upstream_sparkless/tests/test_issue_168_validation_after_drop.py::TestIssue168ValidationAfterDrop::test_validation_after_drop_columns`
- `tests/upstream_sparkless/tests/test_issue_168_validation_after_drop.py::TestIssue168ValidationAfterDrop::test_validation_after_drop_with_complex_filter`
- `tests/upstream_sparkless/tests/test_issue_168_validation_after_drop.py::TestIssue168ValidationAfterDrop::test_validation_after_drop_with_nested_operations`
- `tests/upstream_sparkless/tests/test_issue_169_to_timestamp_drop_error.py::TestIssue169ToTimestampDropError::test_to_timestamp_drop_materialize_basic`
- `tests/upstream_sparkless/tests/test_issue_169_to_timestamp_drop_error.py::TestIssue169ToTimestampDropError::test_to_timestamp_drop_multiple_columns`
- `tests/upstream_sparkless/tests/test_issue_160_nested_operations.py::test_nested_operations_with_drop`
