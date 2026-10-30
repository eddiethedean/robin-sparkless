# Migrate to Sparkless 4

Start with the [version/install guide](INSTALLATION.md) and [support matrix](SUPPORT_MATRIX.md).
Sparkless 4 uses a Rust/Polars native extension; Sparkless 3 used the Polars Python
package. The familiar namespace remains, but that does not guarantee identical
behavior for every old pipeline.

## From PySpark

For supported local transformations, change imports:

```python
# Existing application:
# from pyspark.sql import SparkSession, functions as F
from sparkless.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("migration").getOrCreate()
try:
    df = spark.createDataFrame([(1,), (2,)], ["id"])
    assert [r["id"] for r in df.filter(F.col("id") > 1).collect()] == [2]
finally:
    spark.stop()
```

`appName`/`getOrCreate` work in both engines. Sparkless additionally provides
`app_name`/`get_or_create`; those snake-case spellings are not PySpark builder
methods. Keep backend imports behind your application's adapter or use the
packaged testing fixtures; do not hardcode Sparkless imports in a purported
PySpark validation run.

## From Sparkless 3

1. Use a clean Python 3.11+ environment and install the exact intended 4.x release.
2. Keep public `sparkless.sql` imports; remove legacy checkout-only
   `robin_sparkless` imports and direct Polars-Python assumptions.
3. Use properties `spark.catalog`, `spark.read`, `df.write`; Python builder is
   `SparkSession.builder`, not `builder()`.
4. Check schemas, null/empty inputs, ordering, numeric widths, timestamps/decimals,
   file round trips, and error behavior. Replace unsupported options explicitly.
5. Run the same transformation assertions on real Spark before upgrading CI.

Local scalar `F.udf` works with an explicit DataType return object in documented
contexts. Public pandas/vectorized/grouped UDFs and remote Python execution are
unsupported. Local RDD helpers are bounded in-process operations; they provide
no distributed RDD semantics. Reader `schema(...)` is absent from the checked
installed surface. See [all current differences](PYSPARK_DIFFERENCES.md).

## Select a compatibility profile

Default local profile is 3.5. To request the documented Spark 4 behavior bundle,
set `SPARKLESS_PYSPARK_COMPAT=4.0` before session creation, or set
`spark.conf.set("sparkless.pyspark.compat", "4.0")`. This does not install or
select an oracle. See [profile keys and effects](PYSPARK_COMPAT_PROFILES.md).

Use separate environments for Spark 3.5 and Spark 4.0.5; install the matching
PySpark extra and Java (17 for Spark 4). Register
`pytest_plugins = ["sparkless.testing.plugin"]` in your `conftest.py`, then run:

```bash
SPARKLESS_TEST_MODE=sparkless python -m pytest tests -v
SPARKLESS_TEST_MODE=pyspark python -m pytest tests -v
```

The selector is `SPARKLESS_TEST_MODE`. Older `SPARKLESS_TEST_BACKEND` examples do
not select the backend. Run the [identity example](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_dual_mode_example.py)
which verifies the actual session/imports and live JVM. Use profile-specific
markers only for intentionally different semantics, not to hide regressions.

Next: [testing guide](TESTING_GUIDE.md), [pipeline tutorial](PIPELINE_TUTORIAL.md),
[API contracts](PYTHON_API_REFERENCE.md), and [troubleshooting](TROUBLESHOOTING_PYTHON.md).
