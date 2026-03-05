## Integration Test Guide (Python)

This guide describes how to write and maintain **Python integration tests** under `tests/integration` so they:

- Use the **shared test harness** (fixtures + backend abstraction).
- Treat **PySpark behavior as the source of truth**.
- Work against both the **PySpark** backend and the **Robin** backend without changing test code.

---

### 1. Use the shared harness

- **Do NOT import PySpark or sparkless directly** in tests.
- Import through the shared helpers:

  ```python
  from tests.fixtures.spark_imports import get_spark_imports
  ```

- Use the `spark` fixture from `tests/conftest.py` for the default session (backend chosen via `SPARKLESS_TEST_BACKEND` / `MOCK_SPARK_TEST_BACKEND` or `@pytest.mark.backend(...)`).
- **Do not** construct `SparkSession` manually (e.g. `SparkSession("TestApp")` or `SparkSession.builder...getOrCreate()`) except via fixtures; never call `spark.stop()` in tests.

---

### 2. Sessions with custom config

For tests that need a session with specific config (e.g. `spark.sql.caseSensitive=true`), use a dedicated fixture instead of building a session inside the test:

- **Default session**: use the `spark` fixture (case-insensitive by default on both backends).
- **Case-sensitive session**: use the `spark_case_sensitive` fixture provided in `tests/integration/test_case_sensitivity.py` (or a similar fixture in conftest). It creates a session with `spark.sql.caseSensitive=true`, yields it, and stops it after the test.

Example:

```python
def test_case_sensitive_mode(self, spark_case_sensitive):
    spark = spark_case_sensitive
    df = spark.createDataFrame([{"Name": "Alice"}])
    with pytest.raises(Exception):
        df.select("name").collect()
```

---

### 3. Backend-agnostic Row and conf

- **Row field names / values**: Do not rely on backend-specific Row internals (e.g. `row.__dict__["_data_dict"]` or `row._schema.fields`). Use a helper that works on both backends:
  - `row.asDict()` (PySpark) or `dict(row)` / `_row_to_dict(row)` (see `tests/integration/test_case_sensitivity.py`).
  - For column names only: `list(_row_to_dict(row).keys())` or a `_row_keys(row)` helper.
- **Case sensitivity config**: PySpark’s `RuntimeConfig` may not have `is_case_sensitive()`. Use a helper that checks `conf.get("spark.sql.caseSensitive", "false").strip().lower() == "true"` when the method is missing (see `_is_case_sensitive(spark)` in the case sensitivity tests).

---

### 4. Backend-specific behavior

When behavior differs by backend (e.g. PySpark raises `AMBIGUOUS_REFERENCE` for ambiguous column names, while mock may return a first match), write the test so both outcomes are accepted:

- Use try/except: assert the “lenient” outcome when the operation succeeds, and allow the known exception (e.g. message containing `"ambiguous"`) when the backend is strict.
- Or use `@pytest.mark.backend("pyspark")` / `get_backend_type()` and skip or xfail on the other backend, and document the divergence.

---

### 5. Running integration tests

- **PySpark backend** (parity / expectation updates):

  ```bash
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/integration -n 10 -v
  ```

- **Robin backend** (default):

  ```bash
  pytest tests/integration -n 10 -v
  ```

Use a 15-minute timeout for full runs if needed.

---

### 6. References

- Harness: `tests/conftest.py`, `tests/fixtures/spark_backend.py`, `tests/fixtures/spark_imports.py`
- Example: `tests/integration/test_case_sensitivity.py` (fixtures `spark`, `spark_case_sensitive`; helpers `_row_to_dict`, `_row_keys`, `_is_case_sensitive`)
