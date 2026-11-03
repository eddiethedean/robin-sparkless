//! Engine-neutral identity contracts shared by the server products and runtime.

pub mod config;
pub mod execution;
pub use config::ServerConfig;
pub use execution::{
    ConnectExecutionPort, ConnectExecutionRequest, ConnectExecutionResult, EngineSessionHandle,
    ExecutionError, ExecutionPort, SqlExecutionRequest, SqlExecutionResult,
};

pub const CONTROL_PROTOCOL_VERSION: u32 = 2;
pub const CAPABILITIES_PROTOCOL_VERSION: u32 = 1;
pub const AGENT_PROTOCOL_VERSION: &str = "1";
pub const MUTATION_PROTOCOL_VERSION: &str = "1";
pub const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;

/// The packaged server product selected for one process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerProduct {
    /// Default Sparkless server with the Polars execution backend.
    Base,
    /// Polars execution with optional read-only DuckDB table sources.
    DuckDb,
}

impl ServerProduct {
    pub const fn id(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::DuckDb => "duckdb",
        }
    }

    pub const fn execution_backend(self) -> &'static str {
        "polars"
    }

    pub const fn data_sources(self) -> &'static [&'static str] {
        match self {
            Self::Base => &[],
            Self::DuckDb => &["duckdb"],
        }
    }

    pub const fn quack_owner_modes(self) -> &'static [&'static str] {
        match self {
            Self::Base => &[],
            Self::DuckDb => &["standalone-read-only", "shared-quack-owner"],
        }
    }
}
