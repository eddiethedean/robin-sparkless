# Robin Sparkless User Guide

This guide shows you how to use Robin Sparkless for everyday data work. It assumes basic familiarity with DataFrame concepts (like PySpark or Pandas).

---

## What is Robin Sparkless?

Robin Sparkless is a **PySpark-style DataFrame library** that runs in Rust with [Polars](https://www.pola.rs/) as the engine—**no JVM**. You get:

- Familiar APIs: `SparkSession`, `DataFrame`, `Column`, `filter`, `select`, `group_by`, etc.
- Fast execution on Polars
- Rust-first with optional Python bindings (PyO3)

---

## Installation

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "0.9.2"
```

Optional features:

```toml
robin-sparkless = { version = "0.9.2", features = ["sql"] }   # spark.sql(), temp views
robin-sparkless = { version = "0.9.2", features = ["delta"] }  # Delta Lake read/write
```

### Python

Install from PyPI:

```bash
pip install robin-sparkless
```

Or build from source with SQL support:

```bash
pip install maturin
maturin develop --features "pyo3,sql"
```

---

## Getting Started

### Your First Session

**Rust**

```rust
use robin_sparkless::SparkSession;

let spark = SparkSession::builder()
    .app_name("my_app")
    .get_or_create();
```

**Python**

```python
import robin_sparkless as rs

spark = rs.SparkSession.builder().app_name("my_app").get_or_create()
```

### Creating a DataFrame

**From tuples (Rust, 3 columns: id, age, name)**

```rust
let df = spark.create_dataframe(
    vec![
        (1, 25, "Alice".to_string()),
        (2, 30, "Bob".to_string()),
        (3, 35, "Charlie".to_string()),
    ],
    vec!["id", "age", "name"],
)?;
```

**From tuples (Python)**

```python
df = spark.create_dataframe(
    [(1, 25, "Alice"), (2, 30, "Bob"), (3, 35, "Charlie")],
    ["id", "age", "name"],
)
```

**From rows with arbitrary schema**

Use `_create_dataframe_from_rows` for schemas other than the 3-tuple (id, age, name):

```python
schema = [("id", "bigint"), ("name", "string"), ("score", "double")]
rows = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.0},
]
df = spark._create_dataframe_from_rows(rows, schema)
```

**From files**

```rust
let df = spark.read_csv("data.csv")?;
let df = spark.read_parquet("data.parquet")?;
let df = spark.read_json("data.json")?;
```

---

## Core Operations

### Filter

Keep rows that satisfy a condition.

**Rust**

```rust
use robin_sparkless::{col, lit_i64};

let adults = df.filter(col("age").gt(lit_i64(25)))?;
```

**Python**

```python
adults = df.filter(rs.col("age") > rs.lit(25))
```

### Select

Choose columns (and optionally transform them).

```python
# Select specific columns
df2 = df.select(["id", "name"])

# Select with expressions
df2 = df.select([rs.upper(rs.col("name")).alias("name_upper"), rs.col("age")])
```

### With Column

Add or replace a column.

```python
df2 = df.with_column("age_next_year", rs.col("age") + 1)
df2 = df.with_column("name_upper", rs.upper(rs.col("name")))
```

### Order By

Sort by one or more columns.

```python
df2 = df.order_by(["age"], ascending=[False])  # oldest first
```

### Limit

Take the first N rows.

```python
df2 = df.limit(10)
```

---

## Joins

Join two DataFrames on common columns.

```python
joined = left.join(right, ["dept_id"], "inner")
joined = left.join(right, ["dept_id"], "left")
```

Join types: `"inner"`, `"left"`, `"right"`, `"outer"`.

---

## Aggregations

### Group By and Aggregate

```python
# Count per group
grouped = df.group_by(["dept"])
result = grouped.count()

# Custom aggregates
result = grouped.agg([
    rs.sum(rs.col("sales")).alias("total_sales"),
    rs.avg(rs.col("score")).alias("avg_score"),
])
```

### Common Aggregates

- `count`, `sum`, `avg`, `min`, `max`
- `count_distinct`, `approx_count_distinct`
- `stddev`, `stddev_pop`, `var_pop`, `var_samp`

---

## Reading and Writing Data

### Reading

```python
# CSV (infers schema)
df = spark.read_csv("data.csv")

# Parquet (schema from file)
df = spark.read_parquet("data.parquet")

# JSON (line-delimited)
df = spark.read_json("data.json")
```

With options:

```python
df = spark.read().option("header", "true").option("delimiter", ";").format("csv").load("data.csv")
```

### Writing

```python
df.write().mode("overwrite").format("parquet").save("output.parquet")
df.write().mode("append").format("csv").save("output.csv")
```

---

## SQL (Optional)

With the `sql` feature, you can run SQL against temp views.

```python
df.createOrReplaceTempView("people")
result = spark.sql("SELECT name, age FROM people WHERE age > 25 ORDER BY age")
```

Supports: `SELECT`, `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`. Built-in functions (e.g. `UPPER`, `LOWER`) and registered UDFs work in SQL.

---

## User-Defined Functions

Register custom functions and use them in DataFrames or SQL.

**Python UDF**

```python
def double(x):
    return x * 2 if x is not None else None

my_udf = spark.udf().register("double", double, return_type="int")
df2 = df.with_column("doubled", my_udf(rs.col("id")))
```

**SQL**

```python
spark.udf().register("double", double, return_type="int")
result = spark.sql("SELECT id, double(id) AS doubled FROM people")
```

See [UDF Guide](UDF_GUIDE.md) for full details.

---

## Persistence and Tables

- **Temp views**: `df.createOrReplaceTempView("my_table")` — in-session only
- **Global temp views**: `df.createOrReplaceGlobalTempView("global_table")` — visible across sessions
- **Saved tables**: `df.write().saveAsTable("my_table", mode="overwrite")` — disk-backed when `spark.sql.warehouse.dir` is set

See [Persistence Guide](PERSISTENCE_GUIDE.md) for more.

---

## Common Patterns

### Chaining Operations

```python
result = (
    df.filter(rs.col("age") > 18)
    .select([rs.col("name"), rs.col("age")])
    .order_by(["age"], ascending=[False])
    .limit(10)
)
```

### Conditional Logic (when/then/otherwise)

```python
# Nested when/then/otherwise for multiple conditions
df2 = df.with_column(
    "category",
    rs.when(rs.col("age") >= 65)
    .then(rs.lit("senior"))
    .otherwise(
        rs.when(rs.col("age") >= 18).then(rs.lit("adult")).otherwise(rs.lit("minor"))
    ),
)
```

### Handling Nulls

```python
df2 = df.with_column("age_filled", rs.coalesce(rs.col("age"), rs.lit(0)))
df2 = df.na().fill(rs.lit(0))   # Fill nulls in all columns with 0
df2 = df.na().drop(subset=["name"])   # Drop rows with null in "name"
```

---

## Collecting Results

**Rust**

```rust
let rows = df.collect_as_json_rows()?;  // Vec<HashMap<String, JsonValue>>
df.show(Some(20))?;                     // Print to stdout
```

**Python**

```python
rows = df.collect()           # List of dicts
df.show(20)                   # Print to stdout
# to_pandas() returns list of dicts; for a pandas DataFrame use:
# pandas.DataFrame.from_records(df.to_pandas())
```

Example `collect()` output for the quick-start DataFrame (id, age, name):

```
[{'id': 1, 'age': 25, 'name': 'Alice'}, {'id': 2, 'age': 30, 'name': 'Bob'}, {'id': 3, 'age': 35, 'name': 'Charlie'}]
```

---

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| Column 'X' not found | Typo or wrong case | Check column names with `df.columns()` |
| create_dataframe: expected 3 column names | `create_dataframe` needs exactly 3 columns | Use `_create_dataframe_from_rows` for other schemas |
| call_udf: no session | UDF used before session created | Use `SparkSession.builder().get_or_create()` first |
| SQL: unknown function | Function not built-in or UDF | Register with `spark.udf().register()` or use a built-in |

---

## Next Steps

- [Quickstart](QUICKSTART.md) — Build from source, more examples
- [Python API](PYTHON_API.md) — Full Python API reference
- [UDF Guide](UDF_GUIDE.md) — Custom functions in detail
- [Persistence Guide](PERSISTENCE_GUIDE.md) — Temp views, tables, warehouse
- [PySpark Differences](PYSPARK_DIFFERENCES.md) — How Robin differs from PySpark
