# Python testing API

Install `sparkless[testing]==4.19.0` for local tests or
`sparkless-testing==4.19.0` if you manage the engine installation separately.
Use `pytest_plugins = ["sparkless_testing.plugin"]` in the suite's root
`conftest.py`. The package resolves the selected engine when called.

| API | Inputs | Returns and failure behavior |
| --- | --- | --- |
| `get_mode()` | Reads `SPARKLESS_TEST_MODE` | `Mode.SPARKLESS` by default or `Mode.PYSPARK` |
| `create_session(app_name="test", mode=None, enable_delta=False, enable_hive=False, **config)` | Uses the current mode if omitted. Delta and Hive switches apply to PySpark. | Session owned by the caller. Missing engine dependencies or startup errors fail session creation. Call `stop()` in `finally`. |
| `get_imports(mode=None)` | Optional backend selector | `SparkImports` with backend-specific functions, session class, rows, windows, and types |
| `compare_dataframes(actual, expected, *, tolerance=1e-6, check_schema=True, check_order=False)` | Actual DataFrame and an expected DataFrame or mapping with `data` and optional `schema` | `ComparisonResult` with `equivalent` and `errors`. This collects DataFrame rows. |
| `assert_dataframes_equal(actual, expected, **options)` | Same comparison options | Returns `None` on equality and raises `AssertionError` on differences |
| `assert_rows_equal(actual, expected, *, order_matters=True, tolerance=1e-6)` | Lists of collected rows | Returns `None` on equality and raises `AssertionError` on differences |

The pytest plugin owns sessions returned by its fixtures. For fixture scope,
markers, and the full comparison example, see the [testing guide](TESTING_GUIDE.md).
For Spark Connect session ownership, see the [server launcher API](SERVER_PYTHON_API.md).

## Comparison semantics

`assert_dataframes_equal` and `compare_dataframes` collect both DataFrames.
With `check_schema=True`, they compare field order, names, primitive and nested
types, decimal precision and scale, nullability, and field metadata. Schema
checking applies to two DataFrames or to a mapping whose `schema` contains
`fields`. A mapping may declare fields with `name`, `type`, and optional
`nullable` and `metadata` values. Type names accept Spark spellings such as
`long` or `bigint`.

```python
from sparkless_testing import assert_dataframes_equal, assert_rows_equal

assert_dataframes_equal(actual_df, expected_df, check_schema=True)
assert_rows_equal(actual_df.collect(), expected_df.collect(), order_matters=False)
```

Use `order_matters=False` only when result ordering is outside the contract.
`check_schema=False` skips schema equality while still comparing collected
values and columns. These helpers are intended for bounded test data.
