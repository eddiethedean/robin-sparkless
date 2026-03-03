**Supersedes:** #1085 (logical group 13)

**Theme:** Pivot with aggregates/alias; createDataFrame with Row kwargs.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_215_row_kwargs_init.py::test_row_kwargs_with_createDataFrame`
- `tests/upstream_sparkless/tests/unit/dataframe/test_pivot_grouped_data.py::TestPivotGroupedData::test_pivot_multiple_aggregates`
- `tests/upstream_sparkless/tests/unit/dataframe/test_pivot_grouped_data.py::TestPivotGroupedData::test_pivot_single_aggregate_with_alias`
