# Robin Sparkless

**PySpark-style DataFrames in Rust—no JVM.** A DataFrame library that mirrors PySpark's API and semantics while using [Polars](https://www.pola.rs/) as the execution engine.

## Quick links

- **[User guide](USER_GUIDE.md)** — Learn how to use Robin Sparkless (Rust)
- **[Quickstart](QUICKSTART.md)** — Build, install, and basic usage (Rust)
- **[Persistence guide](PERSISTENCE_GUIDE.md)** — Global temp views and disk-backed saveAsTable
- **[PySpark differences](PYSPARK_DIFFERENCES.md)** — Known divergences and caveats
- **[Roadmap](ROADMAP.md)** — Development phases and Sparkless integration

## What is Robin Sparkless?

Robin Sparkless provides a **PySpark-like API** in Rust so you can write familiar DataFrame code without the JVM. It is designed to power [Sparkless](https://github.com/eddiethedean/sparkless)—the Python PySpark drop-in replacement—as its execution backend via PyO3.

| Feature | Description |
|--------|-------------|
| **Core** | `SparkSession`, `DataFrame`, `Column`; lazy by default (transformations extend plan; actions materialize); filter, select, groupBy, joins |
| **Engine** | [Polars](https://www.pola.rs/) for fast, native execution |
| **Optional** | SQL (`spark.sql`, temp views, global temp views, `saveAsTable` in-memory or warehouse), Delta Lake (`read_delta` / `write_delta`) |

## Documentation

- **Getting started** — [Quickstart](QUICKSTART.md), [Persistence guide](PERSISTENCE_GUIDE.md), [Releasing](RELEASING.md)
- **Reference** — [PySpark differences](PYSPARK_DIFFERENCES.md), [Parity status](PARITY_STATUS.md), [Robin-Sparkless missing](ROBIN_SPARKLESS_MISSING.md)
- **Testing** — Run `make check` for Rust checks and tests; `make test-parity-phase-X` for phase-specific parity. See [QUICKSTART](QUICKSTART.md) and [TEST_CREATION_GUIDE](TEST_CREATION_GUIDE.md).
- **Sparkless integration** — [Integration analysis](SPARKLESS_INTEGRATION_ANALYSIS.md), [Full backend roadmap](FULL_BACKEND_ROADMAP.md), [Logical plan format](LOGICAL_PLAN_FORMAT.md)
- **Development** — [Roadmap](ROADMAP.md), [Test creation guide](TEST_CREATION_GUIDE.md), [Converter status](CONVERTER_STATUS.md), [Bugs and improvements plan](BUGS_AND_IMPROVEMENTS_PLAN.md)

For the full list of documents, see the **Doc index** in the navigation.

## Rust API

- [docs.rs/robin-sparkless](https://docs.rs/robin-sparkless) — Crate API reference

## License

MIT
