# PySpark mode test failures

Recorded from running the full test suite with `MOCK_SPARK_TEST_BACKEND=pyspark` (PySpark as the reference backend instead of sparkless).

**Command:**
```bash
env MOCK_SPARK_TEST_BACKEND=pyspark .venv/bin/python -m pytest tests/python tests/upstream_sparkless -n 10 -v --tb=short
```

**Note:** The run was captured from terminal output while in progress (~77%); the list below includes all failures observed up to that point. Total test count: 2837 items. If the run completed later, the final count may differ.

---

## Failure count: 79 (observed)

---

## Failures (pytest node ids)

```
tests/upstream_sparkless/test_issue_395_catalog_set_current_database_read_csv.py::test_read_csv
tests/upstream_sparkless/test_issue_375_flat_map.py::test_flat_map_empty
tests/upstream_sparkless/test_issue_404_select_star.py::test_select_star
tests/upstream_sparkless/test_issue_404_select_star.py::test_select_star_plus_column
tests/upstream_sparkless/test_issue_405_pow_bitwise.py::test_bitwise_not
tests/upstream_sparkless/tests/test_issue_202_select_with_list.py::TestIssue202SelectWithList::test_select_star_with_list_does_not_unpack
tests/upstream_sparkless/tests/test_issue_288_casewhen_operators.py::TestIssue288CaseWhenOperators::test_casewhen_bitwise_not
tests/upstream_sparkless/tests/parity/sql/test_show_describe.py::TestSQLShowDescribeParity::test_show_databases
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_coexists_with_standard_equality
tests/upstream_sparkless/tests/parity/functions/test_array_contains_join_parity.py::TestArrayContainsJoinParity::test_array_contains_join_multiple_matches_parity
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_integer_types
tests/upstream_sparkless/tests/test_issue_329_log_float_constant.py::TestIssue329LogFloatConstant::test_log_with_column_base
tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_outer_join
tests/upstream_sparkless/tests/parity/functions/test_array_contains_join_parity.py::TestArrayContainsJoinParity::test_array_contains_join_left_parity
tests/upstream_sparkless/tests/test_issue_330_struct_field_alias.py::TestIssue330StructFieldAlias::test_struct_field_without_alias_still_works
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_float_types
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_example_from_issue_260
tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_cross_join
tests/upstream_sparkless/tests/parity/functions/test_cast_alias_select_parity.py::TestCastAliasSelectParity::test_cast_alias_select_basic_parity
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_date_types
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_literal_semantics[None-None-True]
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_datetime_types
tests/upstream_sparkless/tests/parity/functions/test_cast_alias_select_parity.py::TestCastAliasSelectParity::test_cast_alias_select_multiple_aggregations_parity
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_literal_semantics[None-x-False]
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_column_vs_literal
tests/upstream_sparkless/tests/parity/functions/test_cast_alias_select_parity.py::TestCastAliasSelectParity::test_cast_alias_select_with_filter_parity
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_literal_semantics[x-None-False]
tests/upstream_sparkless/tests/test_issue_335_window_orderby_list.py::TestIssue335WindowOrderByList::test_window_orderby_list_with_stddev_variance
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_integer_literal
tests/upstream_sparkless/tests/examples/test_unified_infrastructure_example.py::TestUnifiedInfrastructure::test_aggregation_comparison
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_literal_semantics[x-x-True]
tests/upstream_sparkless/tests/test_issue_173_validation_during_materialization.py::TestIssue173ValidationDuringMaterialization::test_validation_during_materialization_with_dropped_columns
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_in_select_expression
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_literal_semantics[x-y-False]
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_in_join_condition
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_type_coercion
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_multiple_conditions
tests/upstream_sparkless/tests/parity/functions/test_window_orderby_list_parity.py::TestWindowOrderByListParity::test_window_orderby_list_basic_parity
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_empty_dataframe
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_in_groupby_aggregation
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_chained_with_other_operations
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_all_null_columns
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_no_matching_nulls
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafe::test_eqnullsafe_with_mixed_types_and_nulls
tests/upstream_sparkless/tests/test_issue_260_eq_null_safe.py::TestIssue260EqNullSafeParity::test_eqnullsafe_parity_with_pyspark
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_example_from_issue_261
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_inclusive_lower_bound
tests/upstream_sparkless/tests/test_issue_374_join_aliased_columns.py::TestIssue374JoinAliasedColumns::test_join_aliased_self_join
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_inclusive_upper_bound
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_float_values
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_null_values
tests/upstream_sparkless/tests/parity/internal/test_session.py::TestSessionParity::test_createDataFrame_with_explicit_schema
tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test_merge_schema_append
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_string_values
tests/upstream_sparkless/tests/parity/internal/test_session.py::TestSessionParity::test_createDataFrame_empty
tests/upstream_sparkless/tests/test_delta_lake_schema_evolution.py::TestDeltaLakeSchemaEvolution::test_merge_schema_bidirectional
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_literal_bounds
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_in_select_expression
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_per_row_column_bounds
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_date_values
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_with_reversed_bounds
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_in_when_otherwise_expression
tests/upstream_sparkless/tests/test_issue_366_alias_posexplode.py::TestIssue366AliasPosexplode::test_posexplode_empty_array
tests/upstream_sparkless/tests/test_issue_261_between.py::TestIssue261Between::test_between_pyspark_parity
tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_filter_on_table_with_comparison_operations
tests/upstream_sparkless/tests/test_issue_262_arraytype_positional.py::TestIssue262ArrayTypePositional::test_arraytype_all_initialization_patterns
tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_arithmetic_operations_in_filters
tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_complex_nested_operations_in_filters
tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_string_operations_in_filters
tests/upstream_sparkless/tests/parity/dataframe/test_filter_isinstance_ordering.py::TestIsInstanceOrdering::test_logical_operations_in_filters
tests/upstream_sparkless/tests/test_issue_429_posexplode_no_alias.py::test_posexplode_without_alias_no_type_error
tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_inner_join
tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_left_join
tests/upstream_sparkless/tests/parity/dataframe/test_join.py::TestJoinParity::test_right_join
tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_expr_requires_active_session
tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_when_requires_active_session
tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_window_functions_require_session
tests/upstream_sparkless/tests/test_type_strictness.py::TestTypeStrictness::test_to_date_requires_string
```

---

## Summary by area

| Area | Count | Notes |
|------|--------|------|
| **eq_null_safe (eqNullSafe)** | 24 | All `test_issue_260_eq_null_safe` / eqNullSafe semantics |
| **between** | 12 | All `test_issue_261_between` |
| **filter / isinstance ordering** | 5 | `test_filter_isinstance_ordering` (logical/arithmetic/string in filters) |
| **join parity** | 6 | outer, cross, inner, left, right |
| **session / createDataFrame** | 2 | `test_session.py` createDataFrame_empty, with_explicit_schema |
| **Delta Lake** | 2 | merge_schema_append, merge_schema_bidirectional |
| **posexplode** | 2 | empty array, no alias |
| **cast/alias select parity** | 3 | cast_alias_select_parity |
| **array contains join parity** | 2 | multiple_matches, left |
| **window orderBy list** | 2 | stddev_variance, basic_parity |
| **Session validation** | 3 | expr/when/window require active session |
| **Misc (single failures)** | 10 | read_csv, flat_map_empty, select_star, bitwise_not, select_star_with_list, casewhen_bitwise_not, show_databases, log_with_column_base, struct_field_alias, validation_during_materialization, unified_infrastructure aggregation, join_aliased_self_join, arraytype_all_initialization, to_date_requires_string |

---

*Generated from terminal output; run date 2026-03-03.*
