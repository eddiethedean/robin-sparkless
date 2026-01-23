# Quick Start Guide

## Building the Project

### Prerequisites
- Rust (latest stable)
- Python 3.8+
- maturin (`pip install maturin`)

### Development Build

```bash
# Install in development mode
maturin develop

# Or use the Makefile
make dev
```

### Release Build

```bash
maturin build --release
```

## Usage

```python
from robin_sparkless import SparkSession, functions as F

# Create SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Create DataFrame from Python data
data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Transformations
df_filtered = df.filter(df["age"] > 25)
df_selected = df.select(["name", "age"])
df_grouped = df.groupBy(["name"]).count()

# Actions
df.show()
print(df.count())
results = df.collect()

# Column operations
from robin_sparkless import col, lit
df_with_expr = df.select([col("name"), (col("age") + lit(1)).alias("age_plus_one")])
```

## API Compatibility

This package aims for PySpark API parity. Most PySpark code should work with minimal changes:

### Differences from PySpark

1. **No Cluster Support**: This is a single-machine implementation
2. **No Java/Spark**: Pure Rust backend, no JVM required
3. **Simplified Execution**: Uses DataFusion for query execution

### Supported Operations

- ✅ DataFrame creation from Python data
- ✅ Column operations (filtering, selection, arithmetic)
- ✅ GroupBy and aggregations
- ✅ Joins (basic support)
- ✅ SQL functions (col, lit, count, sum, avg, etc.)
- ✅ Schema inference
- ⚠️ Data source readers (CSV, Parquet, JSON) - basic structure, needs implementation
- ⚠️ SQL queries - basic structure, needs full DataFusion integration

## Testing

```bash
pytest tests/ -v
```

## Architecture

- **Rust Core**: High-performance data processing using Arrow and DataFusion
- **PyO3 Bindings**: Python bindings for Rust code
- **Python API**: PySpark-compatible interface

## Next Steps

To achieve full PySpark parity, consider implementing:

1. Full DataFusion integration for query execution
2. Complete data source readers (CSV, Parquet, JSON)
3. More SQL functions
4. Window functions
5. UDF (User Defined Functions) support
6. Caching and persistence
7. Broadcast variables
8. More join types and optimizations
