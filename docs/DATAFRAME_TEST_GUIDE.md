## DataFrame Test Guide (Python)

This guide describes how to write and maintain **Python dataframe tests** under `tests/dataframe` so they:

- Use the **shared test harness** (fixtures + backend abstraction).
- Treat **PySpark behavior as the source of truth**.
- Work against both the **PySpark** backend and the **Robin** backend without changing test code.

---

### 1. Use the shared harness, not direct PySpark/sparkless

- **Do NOT import PySpark or sparkless directly** in tests.
- Always import through the shared helpers:

  ```python
  from tests.fixtures.spark_imports import get_spark_imports
  ```

- Let `tests/conftest.py` provide the `spark` fixture and backend selection based on:
  - `SPARKLESS_TEST_BACKEND` / `MOCK_SPARK_TEST_BACKEND`
  - `@pytest.mark.backend("pyspark" | "robin" | "both")` markers

---

### 2. Canonical test pattern

For new or refactored tests in `tests/dataframe`, follow this structure:

```python
from tests.fixtures.spark_imports import get_spark_imports


class TestSomeBehavior:
    def test_example(self, spark):
        imports = get_spark_imports()
        F = imports.F

        df = spark.createDataFrame(
            [
                {"id": 1, "value": 10},
                {"id": 2, "value": 20},
            ]
        )

        result = df.withColumn("double", F.col("value") * 2)
        rows = result.collect()

        assert len(rows) == 2
        assert rows[0]["double"] == 20
```

**Key rules:**

- Take `spark` as a **fixture argument**; never construct `SparkSession` manually.
- Get all functions/types (`F`, `StructType`, `StringType`, `Window`, …) from `get_spark_imports()`.
- Never call `spark.stop()` in tests; the fixture handles lifecycle.

---

### 3. Backend‑aware / PySpark‑only tests

Some tests are specifically about **PySpark behavior** (parity or API shape). For these:

- Still use `spark` + `get_spark_imports()`; do not import PySpark directly.
- Mark tests that must only run on PySpark with a backend marker, e.g.:

```python
import pytest
from tests.fixtures.spark_backend import BackendType, get_backend_type


@pytest.mark.backend("pyspark")
def test_pyspark_only_behavior(spark):
    imports = get_spark_imports()
    F = imports.F
    ...
```

or use a helper such as `get_backend_type()` in a skip condition **only when necessary**. Prefer markers when possible so selection is obvious in `-m` / CI filters.

---

### 4. Expectations must match real PySpark 3.x

When writing or updating expectations:

- **Run the test in PySpark mode** to see the real behavior:

  ```bash
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/dataframe/test_some_file.py -q
  ```

- Assert on whatever PySpark actually does:
  - Datetime parsing and time zone behavior (e.g. `spark.sql.legacy.timeParserPolicy`).
  - Window function semantics (no window expressions allowed directly in `WHERE`; use `withColumn` + filter, or assert the specific `AnalysisException`).
  - Exact error types/messages only when they are stable and important (e.g. API misuse).

Examples from existing tests:

- `test_issue_170_to_date_timestamp_type.py`:
  - Uses `spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")` so timestamp parsing matches Spark 3.x expectations for ISO strings with microseconds.
- `test_to_timestamp_compatibility.py`:
  - Asserts the accepted input types for `to_timestamp` and adapts tests where PySpark no longer raises a `TypeError` for certain implicit casts.

---

### 5. Common patterns to avoid

- **Avoid creating sessions manually**:

  ```python
  # Avoid
  from tests.fixtures.spark_imports import get_spark_imports
  SparkSession = get_spark_imports().SparkSession
  spark = SparkSession.builder.appName("test").getOrCreate()
  ```

  Always use the `spark` fixture instead.

- **Avoid reading environment variables directly** in tests to choose backends:
  - Do not re‑implement `_is_pyspark_mode()` by inspecting `SPARKLESS_TEST_BACKEND` / `MOCK_SPARK_TEST_BACKEND`.
  - Let `tests.fixtures.spark_backend` handle that and use markers or `get_backend_type()` if you truly need conditional logic.

---

### 6. Running dataframe tests

- **PySpark backend** (parity / expectation updates):

  ```bash
  SPARKLESS_TEST_BACKEND=pyspark pytest tests/dataframe -n 10
  ```

- **Robin backend** (default):

  ```bash
  pytest tests/dataframe -n 10
  ```

For focused work on a single module:

```bash
SPARKLESS_TEST_BACKEND=pyspark pytest tests/dataframe/test_issue_170_to_date_timestamp_type.py
```

---

### 7. Harness verification (sparkless vs robin mode)

Tests use the **proper harness** when they:

- **Session:** Use the `spark` fixture (e.g. `def test_foo(self, spark):`) or, when the fixture cannot be used, `get_spark(...)` / `get_session(...)` from `tests.utils`. Never construct a session with `SparkSession.builder.appName(...).getOrCreate()`.
- **Imports:** Use `get_spark_imports()` from `tests.fixtures.spark_imports` for `F`, types, `Window`, etc., or the legacy `get_functions()` / `get_window_cls()` from `tests.utils`. Never `import pyspark` or `import sparkless` in test files.

To find tests that still create sessions manually:

```bash
rg 'SparkSession\.builder\.appName|SparkSession\.builder\.getOrCreate' tests --glob 'test_*.py' -l
```

Fix them by taking `spark` as a fixture argument and removing manual session creation (and any `spark.stop()` calls).

---

### 8. Where to look for examples

- Harness & fixtures:
  - `tests/conftest.py`
  - `tests/fixtures/spark_backend.py`
  - `tests/fixtures/spark_imports.py`
- Canonical unit‑style tests:
  - `tests/unit/test_issue_270_tuple_dataframe.py`
  - `tests/unit/dataframe/test_na_fill_robust.py`
- Dataframe tests using the new pattern:
  - `tests/dataframe/test_issue_170_to_date_timestamp_type.py`
  - `tests/dataframe/test_function_api_compatibility.py`
  - `tests/dataframe/test_to_timestamp_compatibility.py`

