# Test with Sparkless and PySpark

Use `sparkless_testing` to run the same pytest assertions with the in-process
Sparkless engine and a local JVM-backed PySpark reference. Compare results
and schemas on the inputs your application depends on.

For tests that use the real PySpark client with a native server, follow
the separate [Spark Connect guide](SPARK_CONNECT.md). The fixture modes on
this page select either local engine.

## Quick Start

In a Python 3.11 [virtual environment](INSTALLATION.md), install
`sparkless[testing]` to get the local engine, the separate
`sparkless-testing` package, and pytest.
For 4.18, follow the [guide at the published tag](https://github.com/eddiethedean/robin-sparkless/blob/v4.18.1/docs/TESTING_GUIDE.md).

```bash
python -m pip install "sparkless[testing]==4.20.0"
```

### 1. Add the pytest plugin to your `conftest.py`

```python
# conftest.py
pytest_plugins = ["sparkless_testing.plugin"]
```

Use the `.plugin` entry point to register both fixtures (`spark`, `spark_mode`,
`spark_imports`, etc.) and marker hooks. If you copied the snippet from the
4.19.0 PyPI description, replace `"sparkless_testing"` with
`"sparkless_testing.plugin"` so pytest applies backend and compatibility markers.

### 2. Save a test as `test_example.py`

```python
def test_filter(spark):
    df = spark.createDataFrame([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])
    result = df.filter(df.id > 1).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Bob"
```

### 3. Run tests

```bash
# In the directory containing conftest.py and test_example.py
python -m pytest test_example.py
```

Expect one passing test. Sparkless is the default when `SPARKLESS_TEST_MODE`
is unset.

### Run the same test with PySpark

Use a separate virtual environment and set `JAVA_HOME` to a JDK 17 installation.
The project's docs checks use JDK 17 for both pinned PySpark versions. Choose
one of these installs and run from the same test directory.

```bash
# PySpark 3.5.8
python -m pip install "sparkless[testing]==4.20.0" "pyspark==3.5.8"
SPARKLESS_TEST_MODE=pyspark SPARKLESS_PYSPARK_COMPAT=3.5 python -m pytest test_example.py

# PySpark 4.0.5
python -m pip install "sparkless[testing]==4.20.0" "pyspark==4.0.5"
SPARKLESS_TEST_MODE=pyspark SPARKLESS_PYSPARK_COMPAT=4.0 python -m pytest test_example.py
```

The selector chooses a backend. The compatibility profile does not install an
oracle or certify every Spark behavior. Check `python -m pip show pyspark` and
run the [backend identity example](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_dual_mode_example.py)
before using a suite as parity evidence.

---

In PowerShell, set `$env:SPARKLESS_TEST_MODE = "pyspark"` and
`$env:SPARKLESS_PYSPARK_COMPAT = "4.0"` on separate lines before running pytest.
Use `"3.5"` for PySpark 3.5.8.

## What to read next

| Task | Guide |
| --- | --- |
| Choose fixture scope, backend imports and markers | [Fixture reference](TESTING_FIXTURES.md) |
| Compare values, order and nested schemas | [Comparison examples](TESTING_COMPARISONS.md) and [API contract](TESTING_PYTHON_API.md) |
| Build a real read/join/aggregate/write pipeline | [File pipeline tutorial](PIPELINE_TUTORIAL.md) |
| Set up application CI or repository checks | [CI guide](TESTING_CI.md) |
| Keep the PySpark client with a native server | [Spark Connect](SPARK_CONNECT.md) |
| Diagnose a backend or schema difference | [Troubleshooting](TROUBLESHOOTING_PYTHON.md) |

## Existing section links

The sections below preserve older inbound links. The reference material now has
its own pages so the first-test path stays short.

### Fixtures

See [fixtures](TESTING_FIXTURES.md#fixtures).

### Markers

See [markers](TESTING_FIXTURES.md#markers).

### Direct API Usage

See [direct use](TESTING_FIXTURES.md#direct-api-usage).

### DataFrame Comparison

See [comparison examples](TESTING_COMPARISONS.md#dataframe-comparison).

### CI Configuration

See [application CI](TESTING_CI.md#ci-configuration).

### API Reference

See [testing API](TESTING_PYTHON_API.md).

### Best Practices

See [portable assertions](TESTING_COMPARISONS.md#portable-assertions).

### Troubleshooting

See [fixture troubleshooting](TESTING_FIXTURES.md#troubleshooting).

### Maintainer checks and CI

See [repository CI](TESTING_CI.md#maintainer-checks-and-ci).

### Expected skips

See [skip categories](TESTING_CI.md#expected-skips).

### Session reuse

See [session reuse](TESTING_FIXTURES.md#session-reuse).
