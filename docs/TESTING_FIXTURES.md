# Testing fixtures and backend selection

Start with the [five-minute testing guide](TESTING_GUIDE.md#quick-start).
Register `sparkless_testing.plugin` in the root conftest before using these
fixtures or markers. These modes choose local Sparkless or JVM PySpark;
[Connect sessions](SPARK_CONNECT.md) have a separate lifecycle.

## Environment Variable

The test backend is controlled by the `SPARKLESS_TEST_MODE` environment variable:

| Value | Backend | Use Case |
|-------|---------|----------|
| `sparkless` (default) | Sparkless (Rust/Polars) | Fast local tests, CI |
| `pyspark` | PySpark (JVM) | Parity validation, integration tests |

```bash
# Fast local tests
pytest tests/

# Validate against PySpark
SPARKLESS_TEST_MODE=pyspark pytest tests/

# Explicit sparkless mode
SPARKLESS_TEST_MODE=sparkless pytest tests/
```

---

## Fixtures

### `spark`

The main fixture providing a `SparkSession` for the current mode.

```python
def test_create_dataframe(spark):
    df = spark.createDataFrame([{"x": 1}, {"x": 2}])
    assert df.count() == 2
```

### `spark_mode`

Returns the current `Mode` enum (`Mode.SPARKLESS` or `Mode.PYSPARK`).

```python
from sparkless_testing import Mode

def test_mode_specific_behavior(spark, spark_mode):
    df = spark.createDataFrame([{"id": 1}])

    if spark_mode == Mode.PYSPARK:
        # PySpark-specific assertion
        assert hasattr(df, "_jdf")
    else:
        # Sparkless-specific assertion
        pass
```

### `spark_imports`

Provides mode-appropriate imports (SparkSession, functions, types).

```python
def test_with_imports(spark, spark_imports):
    F = spark_imports.F
    df = spark.createDataFrame([{"name": "alice"}])
    result = df.select(F.upper("name")).collect()
    assert result[0][0] == "ALICE"
```

### `isolated_session`

Use `isolated_session` in place of `spark` for a test that modifies session
state. It bypasses the plugin's shared-session cache. Run in a separate test
process if another application session must remain active. See the
[session lifecycle](TESTING_PYTHON_API.md#session-lifecycle).

```python
def test_isolated(isolated_session):
    spark = isolated_session
    spark.conf.set("my.custom.config", "value")
```

### `table_prefix`

Provides a unique prefix for table names (useful when sharing sessions).

```python
def test_with_table(spark, table_prefix):
    df = spark.createDataFrame([{"id": 1}])
    table_name = f"{table_prefix}_my_table"
    df.write.saveAsTable(table_name)
    # Table name is unique per test
```

---

## Markers

### `@pytest.mark.sparkless_only`

Skip test when running in PySpark mode.

```python
@pytest.mark.sparkless_only
def test_sparkless_specific_feature(spark):
    # This test only runs in sparkless mode
    pass
```

### `@pytest.mark.pyspark_only`

Skip test when running in sparkless mode.

```python
@pytest.mark.pyspark_only
def test_pyspark_specific_feature(spark):
    # This test only runs in PySpark mode
    pass
```

### Compatibility-profile markers

`@pytest.mark.pyspark3_only` and `@pytest.mark.pyspark4_only` select tests by
`SPARKLESS_PYSPARK_COMPAT` (`3.5` by default, or `4.0`). They do not install or
switch the PySpark runtime. Use a matching environment for the reference run.
The plugin does not implement a `backend` marker that overrides the selected
backend; use `SPARKLESS_TEST_MODE` and the supported skip markers instead.

See [compatibility profiles](PYSPARK_COMPAT_PROFILES.md) for semantic settings.

---

## Direct API Usage

You can also use `sparkless_testing` directly without pytest fixtures.

### Mode Detection

```python
from sparkless_testing import Mode, get_mode, is_pyspark_mode, is_sparkless_mode

mode = get_mode()  # Mode.SPARKLESS or Mode.PYSPARK

if is_pyspark_mode():
    print("Running with PySpark")
elif is_sparkless_mode():
    print("Running with sparkless")
```

### Session Creation

Use direct session creation in a test process where you control the session
lifecycle. Sparkless may reuse an active session, and PySpark mode can stop an
existing one. See the [session lifecycle](TESTING_PYTHON_API.md#session-lifecycle).

```python
from sparkless_testing import create_session

spark = create_session(app_name="my_test")
try:
    assert spark.range(3).count() == 3
finally:
    spark.stop()
```

Pass `mode=Mode.SPARKLESS` or `mode=Mode.PYSPARK` to select a backend explicitly.
Import `Mode` from `sparkless_testing`.

### Unified Imports

```python
from sparkless_testing import get_imports

imports = get_imports()

# Access Spark classes and functions
SparkSession = imports.SparkSession
F = imports.F  # functions module
Window = imports.Window
Row = imports.Row

# Data types
StructType = imports.StructType
StructField = imports.StructField
StringType = imports.StringType
IntegerType = imports.IntegerType
# ... and more
```

---

## Troubleshooting

### PySpark session creation fails

Use a Java version compatible with the PySpark version installed in your test
environment, and set `JAVA_HOME` to that JDK. The repository's manual CI smoke
job configures Java 11; the PySpark 4 oracle workflow configures Java 17.

```bash
java -version
python -m pip show pyspark
```

Check the selected runtime and environment before comparing results across
engines. The standalone Connect client does not require a local Spark JVM;
see [Spark Connect](SPARK_CONNECT.md).

### Import errors for sparkless_testing

Install `sparkless-testing` in the same environment as pytest:

```bash
python -m pip install "sparkless-testing==4.20.0"
```

For repository development, use the [contributor setup](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup) and rebuild the native extension after Rust changes.

### Tests pass in sparkless but fail in PySpark

First confirm the selected backend, installed PySpark version, compatibility
profile, and input/schema are the ones you intended. Then reduce the difference
to a small example and check [known differences](PYSPARK_DIFFERENCES.md). Report
remaining mismatches with both versions, actual backend identity, input schema,
and expected and observed results.

### Session reuse

`SPARKLESS_SHARED_SESSION=1` enables shared sessions only for local Sparkless
runs outside pytest-xdist workers and outside Delta tests. The fixture disables
sharing for PySpark. Use `isolated_session` to bypass the shared-session cache,
and `table_prefix` to avoid catalog-name collisions.

---

Next: [comparison reference](TESTING_PYTHON_API.md#comparison-semantics), [comparison examples](TESTING_COMPARISONS.md), or [contributor CI](TESTING_CI.md).
