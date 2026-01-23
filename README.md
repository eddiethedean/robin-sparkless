# Robin Sparkless

A Rust-backed Python package that provides PySpark API parity without requiring Java Spark or clusters.

## Features

- **PySpark API Compatibility**: Drop-in replacement for PySpark code
- **Rust Performance**: High-performance data processing engine written in Rust
- **No Java/Spark Dependencies**: Pure Rust implementation, no JVM required
- **Lazy Evaluation**: Query optimization through lazy evaluation
- **Multiple Data Sources**: Support for CSV, Parquet, JSON, and more

## Installation

```bash
pip install robin-sparkless
```

Or from source:

```bash
maturin develop
```

## Quick Start

```python
from robin_sparkless import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# Create DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# Transformations
df_filtered = df.filter(df.id > 1)
df_grouped = df.groupBy("name").count()

# Actions
df_filtered.show()
df_grouped.collect()
```

## Development

### Prerequisites

- Rust (latest stable)
- Python 3.8+
- maturin

### Building

```bash
# Install maturin
pip install maturin

# Build in development mode
maturin develop

# Build release
maturin build --release
```

### Testing

```bash
pytest tests/
```

## License

MIT
