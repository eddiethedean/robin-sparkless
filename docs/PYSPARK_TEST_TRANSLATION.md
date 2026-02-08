# PySpark Test Translation

This document describes the pipeline for extracting Apache PySpark SQL tests and producing robin-sparkless parity fixtures and pytest stubs.

## Architecture

```
Apache Spark Repo (python/pyspark/sql/tests/)
    │
    ▼
scripts/extract_pyspark_tests.py
    │
    ├──► tests/fixtures/pyspark_extracted/*.json  (behavioral fixtures)
    │         │
    │         └── regenerate_expected_from_pyspark.py --include-skipped
    │
    └──► tests/python/test_pyspark_port_extracted.py  (error/API pytest stubs)
```

## Extraction Pipeline

### 1. Extract

```bash
# Clone Spark and extract (default: v3.5.0)
make extract-pyspark-tests

# Or use existing Spark repo
SPARK_REPO_PATH=/path/to/spark make extract-pyspark-tests

# Dry-run to classify only
python scripts/extract_pyspark_tests.py --clone --branch v3.5.0 --dry-run
```

### 2. Classification Rules

The extractor AST-parses target files and classifies each `test_*` method:

| Classification        | Criteria                                             | Output                          |
|-----------------------|------------------------------------------------------|---------------------------------|
| **fixture-candidate** | createDataFrame/spark.range + df ops + collect + assert | JSON fixture stub               |
| **python-test-candidate** | assertRaises, API/metadata assertions             | pytest stub in test_pyspark_port_extracted.py |
| **skip**              | UDF, pandas, streaming, RDD (self.sc), excluded patterns | —                               |

**Target files:** test_functions.py, test_dataframe.py, test_column.py, test_group.py, test_readwriter.py, test_session.py, test_sql.py

**Excluded patterns:** test_udf*, test_pandas*, test_streaming*, test_connect*, test_arrow, test_plot, coercion, typing

### 3. Fixture Generation

Extracted fixtures are **minimal stubs** with placeholder `expected`. To populate expected from PySpark:

```bash
export JAVA_HOME=/path/to/jdk-17  # Required for PySpark
python tests/regenerate_expected_from_pyspark.py tests/fixtures/pyspark_extracted --include-skipped
```

Fixtures have `skip: true` until operations are filled and expected is regenerated. You can hand-edit operations to match the original PySpark test logic.

### 4. Parity Run

```bash
make pyspark-parity
# or
cargo test pyspark_parity_fixtures
```

Parity runs fixtures from:
- `tests/fixtures/*.json` (hand-written)
- `tests/fixtures/converted/*.json` (Sparkless converter)
- `tests/fixtures/pyspark_extracted/*.json` (PySpark extractor)

## Operation Expression Mapping

PySpark tests use `F.col("x")`, `df.select(F.cos(df.a))`, etc. The parity harness expects `expr` strings:

- `F.cos(df.a)` → `{ "op": "withColumn", "column": "cos_a", "expr": "cos(col('a'))" }`
- Filter expressions → `{ "op": "filter", "expr": "col('age') > 30" }`

The regenerator script (`regenerate_expected_from_pyspark.py`) uses `apply_operations` which evaluates these expressions. Extracted fixtures typically need hand-editing of operations before regeneration.

## Python Test Stubs

Pytest stubs in `tests/python/test_pyspark_port_extracted.py` are initially skipped. Implement with `robin_sparkless` and remove the skip when ready:

```python
def test_cov() -> None:
    """Ported from PySpark test_cov."""
    from robin_sparkless import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
    # ... assert equivalent behavior
```

## Coverage Matrix

| Source              | Fixtures | Pytest Stubs |
|---------------------|----------|--------------|
| Hand-written        | tests/fixtures/*.json | — |
| Sparkless converted | tests/fixtures/converted/*.json | — |
| PySpark extracted   | tests/fixtures/pyspark_extracted/*.json | test_pyspark_port_extracted.py |

## See Also

- [TEST_CREATION_GUIDE.md](TEST_CREATION_GUIDE.md) — Fixture format, generator scripts
- [SPARKLESS_PARITY_STATUS.md](SPARKLESS_PARITY_STATUS.md) — Pass/fail counts, failure reasons
- [regenerate_expected_from_pyspark.py](../tests/regenerate_expected_from_pyspark.py) — Fixture expected regeneration
