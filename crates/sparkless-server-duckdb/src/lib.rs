//! DuckDB-enabled Sparkless server product.
//!
//! This product uses the same Polars query execution, HTTP/Connect transports, lifecycle,
//! replay, and admission code as `sparkless-server`. It adds the optional read-only DuckDB
//! catalog source; it does not select DuckDB as a query execution engine.

pub use sparkless_server_polars::*;

/// Run the shared CLI while identifying this product in diagnostics and `--version` output.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    sparkless_server_polars::run_cli_for("sparkless-server-duckdb")
}
