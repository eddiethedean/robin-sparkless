# robin-sparkless (Python)

**PySpark-style DataFrames in Python—no JVM.** Uses [Polars](https://www.pola.rs/) under the hood for fast execution.

## Install

```bash
pip install robin-sparkless
```

**Requirements:** Python 3.8+

## Quick start

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("demo").get_or_create()
df = spark.create_dataframe(
    [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
    ["id", "age", "name"],
)
filtered = df.filter(rs.col("age").gt(rs.lit(26)))
print(filtered.collect())
# [{"id": 2, "age": 30, "name": "Bob"}, {"id": 3, "age": 35, "name": "Charlie"}]
```

Read from files:

```python
df = spark.read_csv("data.csv")
df = spark.read_parquet("data.parquet")
df = spark.read_json("data.json")
```

Filter, select, group, join, and use window functions with a PySpark-like API. See the [full documentation](https://robin-sparkless.readthedocs.io/) for details.

## Optional features

Install from source to enable extra features (requires [Rust](https://rustup.rs/) and [maturin](https://www.maturin.rs/)):

```bash
pip install maturin
maturin install --features "pyo3,sql"    # spark.sql() and temp views
maturin install --features "pyo3,delta"   # read_delta / write_delta
maturin install --features "pyo3,sql,delta"
```

## Links

- **Documentation:** [robin-sparkless.readthedocs.io](https://robin-sparkless.readthedocs.io/)
- **Source:** [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless)
- **Rust crate:** [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless)

## License

MIT
