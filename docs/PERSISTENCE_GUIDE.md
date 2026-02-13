# Persistence Between Sessions

Robin Sparkless supports two optional persistence mechanisms that align with PySpark:

| Option | Scope | Use case |
|--------|-------|----------|
| **Global temp views** | In-memory, same process | Share DataFrames across sessions within one application |
| **Disk-backed saveAsTable** | Parquet on disk | Persist tables across restarts and processes |

Both require the `sql` feature. Warehouse persistence uses the config key `spark.sql.warehouse.dir`.

---

## Global temp views

Global temp views persist across sessions within the **same process**. They follow PySpark's `global_temp` database semantics.

### When to use

- Notebook-style workflows where multiple sessions share data
- Sharing intermediate results across functions that create their own sessions
- Any case where you need `table("global_temp.name")` to work after the creating session is gone

### Rust

```rust
use robin_sparkless::{SparkSession, SaveMode};

let spark1 = SparkSession::builder().app_name("session1").get_or_create();
let df = spark1
    .create_dataframe(
        vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
        vec!["id", "age", "name"],
    )
    .unwrap();

// Register as global temp view
spark1.create_or_replace_global_temp_view("people", df);

// Same session
let t1 = spark1.table("global_temp.people")?;
assert_eq!(t1.count()?, 2);

// New session — still visible
let spark2 = SparkSession::builder().app_name("session2").get_or_create();
let t2 = spark2.table("global_temp.people")?;
assert_eq!(t2.count()?, 2);

// Drop when done
spark2.drop_global_temp_view("people");
```

### Python

```python
import robin_sparkless as rs

spark1 = rs.SparkSession.builder().app_name("session1").get_or_create()
df = spark1.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
df.createOrReplaceGlobalTempView("people")

# New session can access it
spark2 = rs.SparkSession.builder().app_name("session2").get_or_create()
spark2.table("global_temp.people").show()   # prints 2 rows (Alice, Bob)
spark2.table("global_temp.people").collect() # [{'id': 1, 'age': 25, 'name': 'Alice'}, ...]

# List global temp views
spark2.catalog().listTables("global_temp")  # ["people"]

# Drop
spark2.catalog().dropGlobalTempView("people")
```

### SQL

```sql
-- Register via SparkSession, then:
SELECT * FROM global_temp.people WHERE age > 26;
```

### Resolution

- `table("global_temp.xyz")` looks only in the global temp catalog
- `table("xyz")` (plain name) uses: temp view → saved table → warehouse; **not** global temp

---

## Disk-backed saveAsTable (warehouse)

When `spark.sql.warehouse.dir` is set, `saveAsTable` writes Parquet to disk. New sessions (or restarted processes) can read those tables via `table(name)`.

### When to use

- Persist data across process restarts
- Share tables between separate runs or processes
- Simple “catalog” without a metastore

### Config

Set the warehouse directory **before** creating the session:

```rust
let spark = SparkSession::builder()
    .app_name("demo")
    .config("spark.sql.warehouse.dir", "/tmp/my_warehouse")
    .get_or_create();
```

```python
spark = rs.SparkSession.builder() \
    .app_name("demo") \
    .config("spark.sql.warehouse.dir", "/tmp/my_warehouse") \
    .get_or_create()
```

### Rust

```rust
use robin_sparkless::{SparkSession, SaveMode};

let warehouse = "/tmp/robin_warehouse";
let spark1 = SparkSession::builder()
    .app_name("w1")
    .config("spark.sql.warehouse.dir", warehouse)
    .get_or_create();

let df = spark1
    .create_dataframe(
        vec![(1, 25, "Alice".to_string()), (2, 30, "Bob".to_string())],
        vec!["id", "age", "name"],
    )
    .unwrap();

df.write()
    .save_as_table(&spark1, "users", SaveMode::ErrorIfExists)
    .unwrap();

// New session (or new process) reads from disk
let spark2 = SparkSession::builder()
    .app_name("w2")
    .config("spark.sql.warehouse.dir", warehouse)
    .get_or_create();

let users = spark2.table("users")?;
assert_eq!(users.count()?, 2);
```

### Python

```python
import robin_sparkless as rs

warehouse = "/tmp/robin_warehouse"
spark1 = rs.SparkSession.builder() \
    .app_name("w1") \
    .config("spark.sql.warehouse.dir", warehouse) \
    .get_or_create()

df = spark1.create_dataframe([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
df.write().saveAsTable("users", mode="error")

# New session reads from warehouse
spark2 = rs.SparkSession.builder() \
    .app_name("w2") \
    .config("spark.sql.warehouse.dir", warehouse) \
    .get_or_create()

users = spark2.table("users")
assert users.count() == 2
```

### Save modes

| Mode | Behavior |
|------|----------|
| `error` (default) | Fail if table already exists |
| `overwrite` | Replace existing table |
| `append` | Add rows; schemas must match |
| `ignore` | No-op if table exists |

### Storage layout

Tables are stored as:

```
{warehouse}/{table_name}/data.parquet
```

### Resolution order for `table(name)`

1. Temp view (session)
2. Saved table (session)
3. Warehouse (`{warehouse}/{name}/`)

---

## Comparison

| Feature | Global temp view | Warehouse |
|---------|------------------|-----------|
| Persists across sessions | ✅ (same process) | ✅ |
| Persists across restarts | ❌ | ✅ |
| Requires config | ❌ | `spark.sql.warehouse.dir` |
| Access syntax | `table("global_temp.xyz")` | `table("xyz")` |
| Storage | In-memory | Parquet on disk |

---

## See also

- [PERSISTENCE_BETWEEN_SESSIONS.md](PERSISTENCE_BETWEEN_SESSIONS.md) — Design notes
- [PYSPARK_DIFFERENCES.md](PYSPARK_DIFFERENCES.md) — Known divergences from PySpark
