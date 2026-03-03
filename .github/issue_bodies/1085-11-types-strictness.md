**Supersedes:** #1085 (logical group 11)

**Theme:** Array construction (empty, mixed types, nulls), column astype, to_date string requirement. Match PySpark types/strictness.

## Tests to fix

- `tests/upstream_sparkless/tests/test_issue_367_array_empty.py::TestIssue367ArrayEmpty::test_array_empty_tuple_raises_like_pyspark`
- `tests/upstream_sparkless/tests/test_type_strictness.py::TestTypeStrictness::test_to_date_requires_string`
- `tests/upstream_sparkless/tests/unit/dataframe/test_column_astype.py::TestColumnAstype::test_astype_preserves_column_name`
- `tests/upstream_sparkless/tests/unit/dataframe/test_column_astype.py::TestColumnAstype::test_astype_string_to_boolean`
- `tests/upstream_sparkless/tests/unit/spark_types/test_array_type_robust.py::TestArrayTypeRobust::test_array_type_elementtype_with_filter_operation`
- `tests/upstream_sparkless/tests/unit/spark_types/test_array_type_robust.py::TestArrayTypeRobust::test_array_type_elementtype_with_map_type`
- `tests/upstream_sparkless/tests/unit/spark_types/test_array_type_robust.py::TestArrayTypeRobust::test_array_type_elementtype_with_non_nullable_array`
- `tests/upstream_sparkless/tests/unit/spark_types/test_array_type_robust.py::TestArrayTypeRobust::test_array_type_elementtype_with_struct_type`
- `tests/upstream_sparkless/tests/unit/test_array_parameter_formats.py::TestArrayParameterFormats::test_array_all_formats_with_mixed_types`
- `tests/upstream_sparkless/tests/unit/test_array_parameter_formats.py::TestArrayParameterFormats::test_array_with_mixed_types`
- `tests/upstream_sparkless/tests/unit/test_array_parameter_formats.py::TestArrayParameterFormats::test_array_with_mixed_types_comprehensive`
- `tests/upstream_sparkless/tests/unit/test_array_parameter_formats.py::TestArrayParameterFormats::test_array_with_null_values`
