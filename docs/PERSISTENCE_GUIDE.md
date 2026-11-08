# Persistence Between Sessions

The local library supports two optional persistence mechanisms. Server storage has
a separate [backend-specific lifetime](WRITE_RECOVERY.md).

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
import sparkless as rs

spark1 = rs.SparkSession.builder.app_name("session1").get_or_create()
df = spark1.createDataFrame([(1, 25, "Alice"), (2, 30, "Bob")], ["id", "age", "name"])
df.createOrReplaceGlobalTempView("people")

# New session can access it
spark2 = rs.SparkSession.builder.app_name("session2").get_or_create()
spark2.table("global_temp.people").show()   # prints 2 rows (Alice, Bob)
spark2.table("global_temp.people").collect() # [{'id': 1, 'age': 25, 'name': 'Alice'}, ...]

# List global temp views
spark2.catalog.listTables("global_temp")  # ["people"]

# Drop
spark2.catalog.dropGlobalTempView("people")
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
from sparkless.sql import SparkSession

spark = SparkSession.builder \
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

### Python: verify independent processes

Download [warehouse_roundtrip.py](https://github.com/eddiethedean/robin-sparkless/blob/main/examples/python/warehouse_roundtrip.py)
and run it in the environment where you installed Sparkless:

```bash
python warehouse_roundtrip.py
```

The parent creates a temporary warehouse, starts a writer process, waits for it
to stop, and starts a separate reader process. The reader checks both values and
the long `id` schema. Expect
`warehouse values and schema survived independent process restart`.
The temporary warehouse is removed only after both children exit.

For separate manual runs with your own destination:

```bash
python warehouse_roundtrip.py write ./example-warehouse
python warehouse_roundtrip.py read ./example-warehouse
```

The writer uses explicit `mode("error")`; rerunning it against the same table
fails instead of replacing data. Choose a fresh warehouse for another demo.
Both commands resolve the warehouse to an absolute path.

### Save modes

| Mode | Behavior |
|------|----------|
| `error` / `errorifexists` | Fail if table already exists |
| `overwrite` | Replace existing table |
| `append` | Add rows; schemas must match |
| `ignore` | No-op if table exists |

The Python writer initially uses overwrite, including `saveAsTable`; explicitly
choose a mode. The Rust method receives its `SaveMode` argument explicitly.

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

## Storage and concurrency contract

- Temp views are session-local; global temp views are in-memory and process-global.
  Neither is durable. Stop sessions you own after use.
- A warehouse stores table data as Parquet and registers local saved tables. This
  is not a Hive metastore or a transaction coordinator across processes.
- The implementation stages warehouse replacements, but there is no documented
  multi-process writer lock or crash-recovery journal. Use one writer process per
  warehouse/table and serialize write access outside the library. Readers should
  reopen after a completed write; an existing lazy plan is not a freshness guarantee.
- Append validates the existing table schema; `mergeSchema` and `overwriteSchema`
  affect supported saveAsTable paths. Validate nullability and logical type metadata
  across restarts; do not treat ordinary Parquet inference as application validation.
- `catalog.dropTable` removes the saved-table registration. It is not a dependable
  filesystem deletion API: warehouse data can remain discoverable. Stop users of
  the warehouse and manage its files explicitly when retiring test data.
- A failed or interrupted write does not establish a safe application retry policy.
  Inspect the destination and reconcile before resubmitting. Back up persistent
  warehouses before changing schema or replacing important data.

For durable transactional external storage, use a provider with its own commit
and reconciliation contract. DuckDB execution receipts are a **server** feature
and do not supply exactly-once recovery to local warehouse writes.
