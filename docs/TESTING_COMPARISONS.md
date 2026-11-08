# Compare DataFrames in tests

The examples use an existing session or the fixtures configured in the
[testing quickstart](TESTING_GUIDE.md#quick-start). Keep fixtures small: these
helpers collect their inputs. [Exact comparison semantics](TESTING_PYTHON_API.md#comparison-semantics)
cover columns, schemas, nullability, metadata, nested types and tolerance.

## DataFrame Comparison

The module provides utilities for comparing DataFrames, which is essential for parity testing.

The 4.19.0 `sparkless-testing` artifact had the schema comparison bug tracked
in [#1746](https://github.com/eddiethedean/robin-sparkless/issues/1746). The
4.20.0 package compares nullability, metadata, decimal parameters, nested types,
and declared mapping schemas for empty expected results. See the
[comparison API contract](TESTING_PYTHON_API.md#comparison-semantics).

### `assert_dataframes_equal`

Assert two DataFrames are equivalent.

```python
from sparkless_testing import assert_dataframes_equal

def test_transform(spark):
    input_df = spark.createDataFrame([{"x": 1}, {"x": 2}])

    result = input_df.select((input_df.x * 2).alias("doubled"))
    expected = spark.createDataFrame([{"doubled": 2}, {"doubled": 4}])

    assert_dataframes_equal(result, expected)
```

### Options

```python
assert_dataframes_equal(
    actual_df,
    expected_df,
    tolerance=1e-6,       # Float comparison tolerance
    check_schema=True,    # Compare schemas
    check_order=False,    # Ignore row order
)
```

### `compare_dataframes`

Get detailed comparison results without raising an exception.

```python
from sparkless_testing import compare_dataframes

def test_compare(spark):
    actual_df = spark.createDataFrame([(1, "Ada")], ["id", "name"])
    expected_df = spark.createDataFrame([(1, "Ada")], ["id", "name"])
    result = compare_dataframes(actual_df, expected_df)

    if result.equivalent:
        print("DataFrames match!")
    else:
        print("Differences found:")
        for error in result.errors:
            print(f"  - {error}")
```

### `assert_rows_equal`

Compare row collections directly.

```python
from sparkless_testing import assert_rows_equal

actual = [{"id": 2, "name": "Bob"}, {"id": 1, "name": "Ada"}]
expected = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}]

assert_rows_equal(actual, expected, order_matters=False)
```

---

## Portable assertions

Use `spark_imports` for functions/types when the test runs on both engines. Add
explicit schemas when inference is not under test. Compare rows without order
only when ordering is outside the application contract. Validate schemas as well
as values, including null and empty input. A passing count alone misses wrong
values. Use `table_prefix` for names and `isolated_session` for stateful tests.
