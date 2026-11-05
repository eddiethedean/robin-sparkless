# Python testing API

Install `sparkless[testing]==4.19.0` for local tests or
`sparkless-testing==4.19.0` if you manage the engine installation separately.
Use `pytest_plugins = ["sparkless_testing.plugin"]` in the suite's root
`conftest.py`. The package resolves the selected engine when called.

| API | Inputs | Returns and failure behavior |
| --- | --- | --- |
| `get_mode()` | Reads `SPARKLESS_TEST_MODE` | `Mode.SPARKLESS` by default or `Mode.PYSPARK` |
| `create_session(app_name="test", mode=None, enable_delta=False, enable_hive=False, **config)` | Uses the current mode if omitted. Delta and Hive switches apply to PySpark. | A session for the selected backend. Missing engine dependencies or startup errors fail session creation. See [session lifecycle](#session-lifecycle). |
| `get_imports(mode=None)` | Optional backend selector | `SparkImports` with backend-specific functions, session class, rows, windows, and types |
| `compare_dataframes(actual, expected, *, tolerance=1e-6, check_schema=True, check_order=False)` | Actual DataFrame and an expected DataFrame or dictionary with `data` and optional `schema` | `ComparisonResult` with `equivalent` and `errors`. See [comparison semantics](#comparison-semantics) for schema limits. |
| `assert_dataframes_equal(actual, expected, **options)` | Same comparison options | Returns `None` on equality and raises `AssertionError` on differences |
| `assert_rows_equal(actual, expected, *, order_matters=True, tolerance=1e-6)` | Lists of collected rows | Returns `None` on equality and raises `AssertionError` on differences |

## Session lifecycle

In Sparkless mode, `create_session()` uses `get_or_create()` and can return a
compatible active session from the current thread. In PySpark mode, it can
stop an active PySpark session in the process before starting the test session.
Use the `spark` fixture for pytest-managed setup and teardown. For direct use
alongside an application that already uses Spark, run the helper in a separate
test process. Stop a session only when you control its lifecycle.

For fixture scope, markers, and examples, see the [testing guide](TESTING_GUIDE.md).
For Spark Connect session ownership, see the [server launcher API](SERVER_PYTHON_API.md).

## Comparison semantics

`assert_dataframes_equal` and `compare_dataframes` collect the input DataFrames.
In the published `sparkless-testing==4.19.0`, `check_schema=True` compares field
order, field names, and top-level type classes when both schemas are available.
It can miss differences in nullability, metadata, decimal precision and scale,
and nested types. Assert those properties separately if they matter to your test.

Use an expected DataFrame for schema comparisons in 4.19.0. For value-only
comparisons, you can pass a dictionary with a `data` list of row dictionaries.
The expanded schema checks, including dictionary-form schemas, are unreleased
and tracked in [#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746).

```python
from sparkless_testing import assert_dataframes_equal, assert_rows_equal

assert_dataframes_equal(actual_df, expected_df, check_schema=True)
assert_rows_equal(actual_df.collect(), expected_df.collect(), order_matters=False)
```

Use `order_matters=False` only when result ordering is outside the contract.
`check_schema=False` skips schema equality while still comparing collected
values and columns. These helpers are intended for bounded test data.
