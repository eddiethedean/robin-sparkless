# robin-sparkless (Python)

[![CI](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/robin-sparkless/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/robin-sparkless.svg)](https://pypi.org/project/robin-sparkless/)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Documentation](https://readthedocs.org/projects/robin-sparkless/badge/?version=latest)](https://robin-sparkless.readthedocs.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**PySpark-style DataFrames in Python—no JVM.** Uses [Polars](https://www.pola.rs/) under the hood for fast, native execution. Lazy by default: transformations extend the plan; only actions (`collect`, `show`, `count`, `write`) trigger execution. 200+ operations validated against PySpark.

## Install

```bash
pip install robin-sparkless
```

**Requirements:** Python 3.8+

## Quick start

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("demo").get_or_create()
df = spark.createDataFrame(
    [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
    ["id", "age", "name"],
)
filtered = df.filter(rs.col("age") > rs.lit(26))  # or .gt(rs.lit(26))
print(filtered.collect())
```

Output:
```
[{'id': 2, 'age': 30, 'name': 'Bob'}, {'id': 3, 'age': 35, 'name': 'Charlie'}]
```

**Read from files:**

```python
df = spark.read_csv("data.csv")
df = spark.read_parquet("data.parquet")
df = spark.read_json("data.json")
```

Filter, select, group, join, and use window functions with a PySpark-like API. Use `spark.createDataFrame(data, schema=None)` for list of dicts (schema inferred), list of tuples with column names, DDL string (including nested `struct<>`, `array<>`, `map<>`), or explicit schema as list of `(name, dtype_str)`. See the [User Guide](docs/USER_GUIDE.md) and [full documentation](https://robin-sparkless.readthedocs.io/) for details.

## UDFs and pandas_udf (Python)

- **Scalar Python UDFs**: `spark.udf().register("name", f, return_type=...)` and `call_udf("name", col("x"))`, or use the returned `UserDefinedFunction` directly in `with_column` / `select`.
- **Vectorized Python UDFs**: `spark.udf().register("name", f, return_type=..., vectorized=True)` for column-wise batch UDFs (one output per input row) in `with_column` / `select`.
- **Grouped vectorized UDFs (GROUPED_AGG)**: `@rs.pandas_udf("double", function_type="grouped_agg")` for per-group aggregations in `group_by().agg([...])`, returning one value per group.

See `docs/UDF_GUIDE.md` (or the “UDF guide” section in the online docs) for full details, semantics, and limitations.

## Optional features (install from source)

Building from source requires [Rust](https://rustup.rs/) and [maturin](https://www.maturin.rs/). Clone the repo, then:

```bash
pip install maturin
maturin develop --features pyo3           # default: DataFrame API
maturin develop --features "pyo3,sql"      # spark.sql(), temp views, saveAsTable (in-memory tables), catalog.listTables/dropTable, read_delta(name)
maturin develop --features "pyo3,delta"   # read_delta / write_delta (path I/O)
maturin develop --features "pyo3,sql,delta" # all optional features
```

## Type checking

The package ships with PEP 561 type stubs (`robin_sparkless.pyi`). Use mypy, pyright, or another checker:

```bash
pip install robin-sparkless mypy
mypy your_script.py
```

For **Python 3.8** compatibility, use mypy &lt;1.10 (newer mypy drops support for `python_version = "3.8"` in config). The project’s `pyproject.toml` includes `[tool.mypy]` and `[tool.ruff]` with `target-version` / `python_version` set for 3.8.

## Development

From a clone of the repo:

```bash
# Full CI-like check (Rust + Python lint + Python tests)
make check-full

# Run all examples (Rust + Python doc examples with real output)
make run-examples
```

Or step by step:

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install maturin pytest
maturin develop --features "pyo3,sql,delta"
pytest tests/python/ -v
```

Python lint and type-check (run by `make check-full`):

```bash
pip install ruff 'mypy>=1.4,<1.10'
ruff format --check .
ruff check .
mypy .
```

CI uses the same tooling: ruff, mypy&lt;1.10 (Python 3.8), and pytest. PySpark is not required for tests (parity expectations are predetermined).

## Links

| Resource | URL |
|----------|-----|
| **Documentation** | [robin-sparkless.readthedocs.io](https://robin-sparkless.readthedocs.io/) |
| **User Guide** | [docs/USER_GUIDE.md](docs/USER_GUIDE.md) |
| **Python API** | [docs/PYTHON_API.md](docs/PYTHON_API.md) |
| **UDF Guide** | [docs/UDF_GUIDE.md](docs/UDF_GUIDE.md) |
| **Source** | [github.com/eddiethedean/robin-sparkless](https://github.com/eddiethedean/robin-sparkless) |
| **Rust crate** | [crates.io/crates/robin-sparkless](https://crates.io/crates/robin-sparkless) |

## License

MIT
