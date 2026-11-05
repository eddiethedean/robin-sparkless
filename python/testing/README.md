# sparkless-testing

Backend-independent pytest fixtures and helpers for test suites that run against
Sparkless or PySpark. Install `sparkless[testing]` when testing a Sparkless
application, or install `sparkless-testing` directly for a suite that selects its
backend independently.

Add this to your suite's root `conftest.py` to load the fixtures and backend
markers:

```python
pytest_plugins = ["sparkless_testing.plugin"]
```

The package does not import Sparkless, PySpark, a native engine, or a JVM until a
backend-dependent helper is called. Follow the
[testing guide](https://robin-sparkless.readthedocs.io/en/latest/TESTING_GUIDE/)
to install an engine and run the same test on both backends.

Use `compare_dataframes()` for structured row, ordered-column, and schema
results, or `assert_dataframes_equal()` to fail a test on any difference. Schema
checks cover names/order, types, nullability, declared metadata, decimals, and
nested struct/array/map fields. An expected mapping can declare `schema` and an
empty `data` list. See the
[comparison API contract](https://robin-sparkless.readthedocs.io/en/latest/TESTING_PYTHON_API/#comparison-semantics).

The published 4.19.0 package still has the schema comparison defect in
[#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746); use the
4.20 source candidate or the corrected package once it is released.
