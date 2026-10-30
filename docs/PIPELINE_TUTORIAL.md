# Build and test a small file pipeline

Goal: read CSV orders, normalize types, discard refunds, join customer regions,
aggregate revenue, and write Parquet. Budget: about ten minutes after installing
Sparkless and pytest. No Java is needed for the Sparkless run.

The complete executable source is
[`test_pipeline_example.py`](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_pipeline_example.py).
Download it and [the plugin conftest](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/conftest.py)
into one directory, or use `examples/python` in a checkout. It creates input in a
temporary directory and cleans up through pytest, so no sample dataset is needed.

## Transformation

```python
def summarize(orders, customers, F):
    clean = orders.withColumn("amount", F.col("amount").cast("double"))
    return (clean.filter(F.col("amount") > 0)
            .join(customers, on="customer_id", how="inner")
            .groupBy("region").agg(F.sum("amount").alias("revenue")))
```

The test writes four orders (12.5, 7.5, 5.0, -1.0). It reads CSV with headers,
casts `customer_id` to long before joining, and creates customers 1/East and
2/West. CSV parsing is not a schema contract: normalize types explicitly before
business logic. For money, choose and validate an appropriate decimal policy;
double here keeps this introductory example small.

The join retains only matching customer IDs. A missing customer therefore drops
an order; switch to a left join and define an unknown-region policy if your
application needs it. Null/invalid values need a separate rejection policy.

## Run and inspect

```bash
python -m pip install pytest
SPARKLESS_TEST_MODE=sparkless python -m pytest examples/python/test_pipeline_example.py -v
```

Expected output after reading the written Parquet:

| region | revenue |
| --- | ---: |
| East | 20.0 |
| West | 5.0 |

The assertion compares region/value mappings, because grouped result ordering
is not guaranteed. `overwrite` replaces output at this test's temporary path;
choose save modes deliberately for real data.

## Check real Spark

In a separate environment with Java and `sparkless[pyspark]` installed:

```bash
SPARKLESS_TEST_MODE=pyspark python -m pytest examples/python/test_dual_mode_example.py examples/python/test_pipeline_example.py -v -s
```

The identity test asserts the actual session and imported SparkSession come from
PySpark. Repeat with `sparkless[pyspark4]` and Java17 when Spark4 is your target.
Both engines must pass the same transformation assertions. This small example
is not a performance benchmark or certification of the rest of your workload.

Next: [API contracts](PYTHON_API_REFERENCE.md), [schema/errors](TYPES_AND_ERRORS.md),
and [testing patterns](TESTING_GUIDE.md).
