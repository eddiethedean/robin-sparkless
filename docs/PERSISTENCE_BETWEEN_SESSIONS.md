# Persistence Between Sessions: Design

**Status: Implemented** (Feb 2026). Both Option A (global temp views) and Option B (disk-backed saveAsTable) are implemented.

This document explores **persistence between sessions** as an optional feature that aligns with PySpark's interface.

## PySpark Reference Model

| Mechanism | Scope | Lifetime | Access |
|-----------|-------|----------|--------|
| **createOrReplaceTempView** | Session | Until session closes | `spark.table("name")` |
| **createOrReplaceGlobalTempView** | Application | Until process ends | `spark.table("global_temp.name")` |
| **saveAsTable** (Hive metastore) | Application / cluster | Disk-backed | `spark.table("db.table")` or `spark.table("table")` |

Robin-sparkless currently implements temp views and saveAsTable as **in-memory, session-scoped** only.

---

## Proposed Options

### Option A: Global Temp Views (PySpark-aligned, in-memory)

**Goal**: `createOrReplaceGlobalTempView` persists across sessions within the same process.

**PySpark behavior**:
- `df.createOrReplaceGlobalTempView("people")` registers in `global_temp` database
- Any `SparkSession` in the same application can access via `spark.table("global_temp.people")`
- Dropped when the Spark application (process) ends

**Implementation**:
1. Add a **process-wide** `GlobalTempViewCatalog` (e.g. `Arc<Mutex<HashMap<String, DataFrame>>>`)
2. `SparkSession` holds an `Arc` reference to this shared catalog (injected at construction or via `OnceLock`)
3. `create_or_replace_global_temp_view` / `create_global_temp_view` → write to global catalog (not session catalog)
4. `drop_global_temp_view` → remove from global catalog
5. `table("global_temp.xyz")` → resolve from global catalog (separate from temp view / saved table lookup)
6. SQL translator: support `FROM global_temp.xyz` in addition to plain table names

**Resolution order** for `table(name)`:
- If name is `global_temp.xyz`: look up in global catalog only
- Else: (1) temp view, (2) saved table (unchanged)

**Rust**: Need a way to share the global catalog across `SparkSession` instances. Options:
- `lazy_static!` / `OnceLock` holding the catalog; all sessions reference it
- `SparkSessionBuilder` accepts optional `SharedCatalog`; when using default builder, use global `OnceLock`

**Python**: The PyO3 extension lives in one process. All `SparkSession` instances can share the same global catalog. No extra config needed.

**Effort**: ~1–2 days. Low risk, backward compatible (global temp views are currently stubs that delegate to temp views).

---

### Option B: Disk-Backed saveAsTable (Warehouse)

**Goal**: `saveAsTable(name)` can optionally persist to disk so new sessions (or restarted processes) can load the table.

**PySpark behavior**:
- With Hive metastore, `saveAsTable` writes to `spark.sql.warehouse.dir` (or table location)
- `spark.table("name")` resolves from metastore + warehouse

**Implementation**:
1. Config: `spark.sql.warehouse.dir` (default: `None` = in-memory only, current behavior)
2. When `spark.sql.warehouse.dir` is set (e.g. `"/tmp/robin_warehouse"`):
   - `saveAsTable(name, mode)` writes DataFrame to `{warehouse}/{name}/` as Parquet
   - Mode semantics: overwrite = replace dir; append = read existing + concat + write; error/ignore = current
3. `table(name)` resolution when not in session catalogs:
   - If warehouse configured and `{warehouse}/{name}/` exists: `read_parquet` that path
4. `dropTable(name)`: if warehouse-backed, delete the directory (or just remove from session catalog if we don't track persistence explicitly)

**Considerations**:
- Schema evolution: append mode needs compatible schemas
- We don't have a "metastore" — just a directory layout. Table = directory.
- Optional feature flag? e.g. `--features persistence` or always-on when warehouse is set

**Effort**: ~3–5 days. Medium complexity (IO, path handling, schema on read).

---

### Option C: Both (Recommended)

1. **Phase 1**: Implement Option A (global temp views) — quick win, full PySpark parity for global temp
2. **Phase 2**: Implement Option B (disk-backed saveAsTable) — enables true cross-process persistence

---

## API Surface Changes

### New / Updated Methods

| Method | Current | After Option A | After Option B |
|--------|---------|----------------|----------------|
| `create_or_replace_global_temp_view` | Stub → temp view | Writes to global catalog | Same |
| `create_global_temp_view` | Stub → temp view | Writes to global catalog | Same |
| `drop_global_temp_view` | Stub → drop temp view | Drops from global catalog | Same |
| `table(name)` | temp → saved | + `global_temp.x` → global catalog | + fallback to warehouse |
| `saveAsTable(name, mode)` | In-memory only | Same | + optional disk when warehouse set |
| `listTables(dbName)` | Session only | + list global when `dbName=global_temp`? | + warehouse tables? |

### Config

| Config Key | Default | Description |
|------------|---------|-------------|
| `spark.sql.warehouse.dir` | (unset) | When set, saveAsTable persists to this directory as Parquet |
| (none for global temp) | — | Global temp is always on when sql feature enabled |

### SQL

- `FROM global_temp.people` → resolve from global catalog
- `FROM people` → existing resolution (temp view, saved table, then warehouse if Option B)

---

## Backward Compatibility

- **Option A**: Current `create_global_temp_view` is a stub. Real implementation is additive; no breaking changes.
- **Option B**: When `spark.sql.warehouse.dir` is unset, behavior unchanged (in-memory only).

---

## Open Questions

1. **Rust singleton**: Should Rust have a global catalog at process level, or should it be explicit (e.g. `SparkSessionBuilder::with_global_catalog()`)? PySpark implicitly shares; we could use `OnceLock` for zero-config.
2. **Python multi-session**: Currently each `get_or_create()` creates a new inner `SparkSession` (with fresh catalogs) but overwrites the default. Do we need true multi-session support, or is "one default session" sufficient for most use cases?
3. **Catalog listing**: Should `listTables(dbName="global_temp")` return global temp view names? PySpark's catalog supports this.
4. **Warehouse layout**: Flat `{warehouse}/{table_name}/` or `{warehouse}/default/{table_name}/` to mirror `default` database?

---

## Implementation Summary

Both options are implemented:

- **Option A (Global Temp Views)**: Process-wide catalog via `OnceLock`. `createOrReplaceGlobalTempView` / `createGlobalTempView` write to shared catalog. `table("global_temp.xyz")` resolves from it. `listTables(dbName="global_temp")` returns global temp view names. SQL supports `FROM global_temp.xyz`.
- **Option B (Warehouse)**: When `spark.sql.warehouse.dir` is set, `saveAsTable` writes to `{warehouse}/{name}/data.parquet`. `table(name)` falls back to warehouse when not in session catalogs. Supports all modes: error, overwrite, append, ignore.
