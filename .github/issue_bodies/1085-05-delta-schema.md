**Supersedes:** #1085 (logical group 5)

**Theme:** Delta merge schema (append, bidirectional) and null literal casting to various types.

## Tests to fix

- `tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test_merge_schema_append`
- `tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test_merge_schema_bidirectional`
- `tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test_null_literal_casting_to_various_types`
