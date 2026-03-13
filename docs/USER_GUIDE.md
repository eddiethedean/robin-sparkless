# Robin Sparkless User Guide

This guide shows you how to use Robin Sparkless for everyday data work. It assumes basic familiarity with DataFrame concepts (like PySpark or Pandas).

---

## What is Robin Sparkless?

Robin Sparkless is a **PySpark-style DataFrame library** that runs in Rust with [Polars](https://www.pola.rs/) as the engine—**no JVM**. You get:

- Familiar APIs: `SparkSession`, `DataFrame`, `filter`, `select`, `group_by`, etc.
- **Lazy by default**: transformations extend the plan; only actions (`collect`, `show`, `count`, `write`) trigger execution—aligns with PySpark and enables Polars query optimization.
- Fast execution on Polars

### Two expression APIs

- **ExprIr (engine-agnostic):** Use `col`, `lit_i64`, `gt`, `when`, etc. from the **crate root**; they build an `ExprIr` tree. Use `filter_expr_ir`, `select_expr_ir`, `with_column_expr_ir`, `collect_rows`, and `GroupedData::agg_expr_ir`. Prefer this for new code and embeddings; errors are `EngineError`, and the public API does not expose Polars types.
- **Column / Expr (Polars):** Use the **prelude** or `robin_sparkless::functions` for `col`, `lit_i64`, etc., which return `Column`. Use `filter`, `with_column`, `select_exprs`, and the full set of string/window/aggregate functions. Use this when you need the full PySpark-like API or are porting existing Column-based code.

The rest of this guide shows the **Column API** (prelude) for maximum familiarity; you can substitute the ExprIr equivalents where noted.

---

## Installation

### Rust

Add to `Cargo.toml`:

```toml
[dependencies]
robin-sparkless = "4"
```

Optional features:

```toml
robin-sparkless = { version = "4", features = ["sql"] }        # spark.sql(), temp views
robin-sparkless = { version = "4", features = ["delta"] }      # Delta Lake read/write
robin-sparkless = { version = "4", features = ["jdbc"] }       # PostgreSQL read/write
robin-sparkless = { version = "4", features = ["sqlite"] }     # SQLite read/write
robin-sparkless = { version = "4", features = ["jdbc_mysql"] } # MySQL/MariaDB
```

---

## Getting Started

### Your First Session

```rust
use robin_sparkless::SparkSession;

let spark = SparkSession::builder()
    .app_name("my_app")
    .get_or_create();
```

### Creating a DataFrame

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

```rust
use robin_sparkless::{col, lit_i64};

let adults = df.filter(col("age").gt(lit_i64(25).into_expr()).into_expr())?;
```

### Select

Choose columns (and optionally transform them) using `select` and expressions.

### With Column

Add or replace a column with computed values using `with_column` or `with_column_expr`.

### Order By and Limit

Sort by one or more columns and take the first N rows using `order_by` and `limit`.

---

## Joins

Join two DataFrames on common columns using `DataFrame::join` with `JoinType` (`Inner`, `Left`, `Right`, `Outer`).

---

## Aggregations

Group and aggregate with `group_by` and `GroupedData` methods such as `count`, `sum`, `avg`, `min`, `max`, and more.

---

## Reading and Writing Data

Use `SparkSession::read_csv`, `read_parquet`, and `read_json` to read data, and `DataFrame::write` (writer API) to write Parquet/CSV/JSON.

### JDBC / External Databases

Read from and write to external databases using a PySpark-compatible JDBC API. Supported backends:

| Backend | Feature | URL Example |
|---------|---------|-------------|
| PostgreSQL | `jdbc` | `jdbc:postgresql://localhost:5432/mydb` |
| SQLite | `sqlite` | `jdbc:sqlite:/path/to/db.db` |
| MySQL | `jdbc_mysql` | `jdbc:mysql://localhost:3306/mydb` |
| MariaDB | `jdbc_mariadb` | `jdbc:mariadb://localhost:3307/mydb` |
| SQL Server | `jdbc_mssql` | `jdbc:sqlserver://localhost:1433;databaseName=mydb` |
| Oracle | `jdbc_oracle` | `jdbc:oracle:thin:@//localhost:1521/ORCL` |
| DB2 | `jdbc_db2` | `jdbc:db2://localhost:50000/mydb` |

Add the feature in your `Cargo.toml`:

```toml
robin-sparkless = { version = "4", features = ["jdbc"] }       # PostgreSQL
robin-sparkless = { version = "4", features = ["sqlite"] }     # SQLite (file-based)
robin-sparkless = { version = "4", features = ["jdbc_mysql"] } # MySQL
```

#### Python Example

```python
url = "jdbc:postgresql://localhost:5432/mydb"
props = {"user": "admin", "password": "secret"}

# Basic read
df = spark.read.jdbc(url=url, table="users", properties=props)

# Read with options (PySpark-compatible)
df = (spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", "users")
    .option("sessionInitStatement", "SET timezone='UTC'")
    .option("queryTimeout", "30")
    .options(props)
    .load("."))

# Write with batching and truncate
df.write.jdbc(
    url=url,
    table="users_backup",
    properties={"batchsize": "5000", "truncate": "true", **props},
    mode="overwrite"
)

# SQLite (no server required)
df = spark.read.jdbc(url="jdbc:sqlite:/tmp/test.db", table="my_table", properties={})
df.write.jdbc(url="jdbc:sqlite:/tmp/test.db", table="results", properties={}, mode="append")
```

#### Supported Options

| Option | Type | Description |
|--------|------|-------------|
| `sessionInitStatement` | String | SQL to execute after connection (e.g., `SET timezone='UTC'`) |
| `queryTimeout` | Integer | Query timeout in seconds |
| `prepareQuery` | String | SQL to execute before main query (for CTEs, temp tables) |
| `fetchsize` | Integer | Rows per fetch |
| `batchsize` | Integer | Rows per batch/transaction on write (default: 1000) |
| `truncate` | Boolean | Use `TRUNCATE` vs `DELETE` for Overwrite mode |
| `cascadeTruncate` | Boolean | Add CASCADE to TRUNCATE (PostgreSQL/Oracle) |

#### Save Modes

| Mode | Behavior |
|------|----------|
| `append` | Insert rows into existing table |
| `overwrite` | Truncate/delete existing data, then insert |
| `error` | Error if table has any existing rows |
| `ignore` | Do nothing if table has existing rows |

See [JDBC_TESTING.md](JDBC_TESTING.md) for setup, Docker Compose files, and CI configuration.

---

## SQL (Optional)

With the `sql` feature, you can run SQL against temp views.

```rust
spark.create_or_replace_temp_view("people", df.clone());
let result = spark.sql("SELECT name, age FROM people WHERE age > 25 ORDER BY age")?;
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
| create_dataframe: expected 3 column names | Rust `create_dataframe` needs exactly 3 columns | In Python use `createDataFrame(data, schema)` for any schema |
| call_udf: no session | UDF used before session created | Use `SparkSession.builder().get_or_create()` first |
| SQL: unknown function | Function not built-in or UDF | Register with `spark.udf().register()` or use a built-in |

---

## Next Steps

- [Quickstart](QUICKSTART.md) — Build from source, more examples
For end-to-end API details, see the Rust docs on docs.rs.
- [UDF Guide](UDF_GUIDE.md) — Custom functions in detail
- [Persistence Guide](PERSISTENCE_GUIDE.md) — Temp views, tables, warehouse
- [PySpark Differences](PYSPARK_DIFFERENCES.md) — How Robin differs from PySpark
