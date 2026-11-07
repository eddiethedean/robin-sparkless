# sparkless-testing

Backend-independent pytest fixtures and helpers for test suites that run against
Sparkless or PySpark. Install `sparkless[testing]` when testing a Sparkless
application, or install `sparkless-testing` directly for a suite that selects its
backend independently.

```bash
python -m pip install "sparkless[testing]==4.20.0"
```

Add this to your suite's root `conftest.py` to load the fixtures and backend
markers:

```python
pytest_plugins = ["sparkless_testing.plugin"]
```

Use the `spark` fixture in your tests. It selects Sparkless by default. To
run the same tests on a local PySpark installation, set `SPARKLESS_TEST_MODE=pyspark`.
The [testing guide](https://robin-sparkless.readthedocs.io/en/latest/TESTING_GUIDE/)
covers PySpark and Java setup and the compatibility profiles.

Use `compare_dataframes()` for structured row, ordered-column, and schema
results, or `assert_dataframes_equal()` to fail a test on any difference. Schema
checks cover names/order, types, nullability, declared metadata, decimals, and
nested struct/array/map fields. An expected mapping can declare `schema` and an
empty `data` list. See the
[comparison API contract](https://robin-sparkless.readthedocs.io/en/latest/TESTING_PYTHON_API/#comparison-semantics).

The published 4.19.0 package had the schema comparison defect in
[#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746). Version
4.20.0 fixes schema comparison for empty results and nested types.
