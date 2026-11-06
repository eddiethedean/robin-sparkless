//! DuckDB-enabled Sparkless server product.
//!
//! The 4.20 product defaults to Polars execution and adds read-only DuckDB catalog sources.
//! Trusted startup configuration can select the optional DuckDB Connect execution adapter.
//! HTTP/Connect transports, replay, and admission remain shared with the base product.

pub use sparkless_server_polars::*;

/// Run the shared CLI while identifying this product in diagnostics and `--version` output.
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    sparkless_server_polars::run_cli_for("sparkless-server-duckdb")
}
