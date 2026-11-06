# Build and test a small file pipeline

Goal: read CSV orders, normalize types, discard refunds, join customer regions,
aggregate revenue, and write Parquet. Budget: about ten minutes after installing
the Sparkless testing extra. No Java is needed for the Sparkless run.

The complete executable source is
[`test_pipeline_example.py`](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_pipeline_example.py).
Download it and [the plugin conftest](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/conftest.py)
into one directory, or change into `examples/python` in a checkout. It creates input in a
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
python -m pip install "sparkless[testing]==4.20.0"
SPARKLESS_TEST_MODE=sparkless python -m pytest test_pipeline_example.py -v
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

In a separate Python environment with Java 17, install the test plugin and
the reference runtime. Run from the same example directory.

```bash
python -m pip install "sparkless[testing,pyspark]==4.20.0"
SPARKLESS_TEST_MODE=pyspark python -m pytest test_pipeline_example.py -v
```

Expect one passing test in each environment. For Spark 4, use
`sparkless[testing,pyspark4]==4.20.0` in another environment and set
`SPARKLESS_PYSPARK_COMPAT=4.0` alongside `SPARKLESS_TEST_MODE=pyspark`.
The separate [backend identity example](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_dual_mode_example.py)
can also verify that the reference session has a live JVM.

Next: [API contracts](PYTHON_API_REFERENCE.md), [schema/errors](TYPES_AND_ERRORS.md),
and [testing patterns](TESTING_GUIDE.md).
