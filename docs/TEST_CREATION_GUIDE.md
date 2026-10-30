# Add a regression test

Use the [canonical contributor environment](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md#setup).
The current parity harness is **pytest**, not a `tests/parity.rs` Cargo target.
Rust unit/integration tests remain useful for engine behavior, but are a separate lane.

## Start with one observable behavior

Create `tests/dataframe/test_my_regression.py` with a unique name and an explicit
schema when inference is not the behavior under test:

```python
def test_filter_preserves_null_semantics(spark, spark_imports):
    F = spark_imports.F
    schema = spark_imports.StructType([spark_imports.StructField("id", spark_imports.IntegerType(), True)])
    df = spark.createDataFrame([(1,), (2,), (None,)], schema)
    assert [r["id"] for r in df.filter(F.col("id") > 1).collect()] == [2]
```

For cross-engine tests use `spark_imports.StructType/StructField/IntegerType`
instead of Sparkless-specific tuple schema shortcuts. Existing repository
`tests/conftest.py` supplies `spark` and `spark_imports`. In external projects,
register `pytest_plugins = ["sparkless.testing.plugin"]` in `conftest.py`.

```bash
SPARKLESS_TEST_MODE=sparkless .venv/bin/python -m pytest tests/dataframe/test_my_regression.py -v
SPARKLESS_TEST_MODE=pyspark .venv/bin/python -m pytest tests/dataframe/test_my_regression.py -v
```

Install the appropriate PySpark extra and Java first. Verify backend identity
using [the public dual-mode example](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/test_dual_mode_example.py).
Assertions should fail if the result is wrong; avoid assertion-free smoke tests
or catching exceptions without checking them. Test invalid/empty/null inputs and
schema, not just row counts. Sort or compare sets only when ordering is not part
of the contract. Mark genuinely backend-specific cases with documented markers.

## Existing expected-output parity cases

The executable pattern lives in
[`tests/parity/dataframe/test_select.py`](https://github.com/eddiethedean/robin-sparkless/blob/main/tests/parity/dataframe/test_select.py):

```python
from tests.tools.parity_base import ParityTestBase

class TestSelectParity(ParityTestBase):
    def test_basic_select(self, spark):
        expected = self.load_expected("dataframe_operations", "basic_select")
        df = spark.createDataFrame(expected["input_data"])
        self.assert_parity(df.select("id", "name", "age"), expected)
```

```bash
.venv/bin/python -m pytest tests/parity/dataframe/test_select.py::TestSelectParity::test_basic_select -v
.venv/bin/python -m pytest tests/parity --collect-only -q
.venv/bin/python -m pytest tests/parity -v
```

`tests/tools/parity_base.py` loads `tests/expected_outputs` via the output loader;
its default historical oracle label is **3.2**, not evidence for current Spark4.
Inspect fixture provenance before changing it. `tests/gen_pyspark_cases.py`
generates a different JSON corpus in `tests/fixtures`; do not treat that generator
as the pytest expected-output format. Record the exact oracle version and
command when adding or regenerating evidence. Never regenerate expected output
using Sparkless itself.

Run focused tests while iterating, then the required CI lanes. `make check-full`
does not replace Python pytest; `make check` also cleans Cargo artifacts. See
[Contributing](https://github.com/eddiethedean/robin-sparkless/blob/main/CONTRIBUTING.md)
and [documentation maintenance](DOCUMENTATION.md).
