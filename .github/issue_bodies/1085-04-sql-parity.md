**Supersedes:** #1085 (logical group 4)

**Theme:** SQL engine behavior: LIMIT, GROUP BY, CTEs. Match PySpark SQL parity.

## Tests to fix

- `tests/upstream_sparkless/tests/parity/sql/test_advanced.py::TestSQLAdvancedParity::test_sql_with_limit`
- `tests/upstream_sparkless/tests/parity/sql/test_queries.py::TestSQLQueriesParity::test_group_by`
- `tests/upstream_sparkless/tests/unit/session/test_sql_cte_robust.py::test_cte_with_aggregation_after_join`
- `tests/upstream_sparkless/tests/unit/session/test_sql_cte_robust.py::test_cte_with_multiple_joins`
- `tests/upstream_sparkless/tests/unit/session/test_sql_cte_robust.py::test_cte_with_where_clause`
