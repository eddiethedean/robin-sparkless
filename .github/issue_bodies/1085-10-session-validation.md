**Supersedes:** #1085 (logical group 10)

**Theme:** Require active session for expr, when, window, aggregates; createDataFrame with explicit schema (session parity).

## Tests to fix

- `tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_aggregate_functions_require_session`
- `tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_expr_requires_active_session`
- `tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_when_requires_active_session`
- `tests/upstream_sparkless/tests/test_sparkcontext_validation.py::TestSessionValidation::test_window_functions_require_session`
- `tests/upstream_sparkless/tests/parity/internal/test_session.py::TestSessionParity::test_createDataFrame_with_explicit_schema`
