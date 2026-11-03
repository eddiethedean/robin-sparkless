use crate::JdbcSaveMode;
use robin_sparkless_core::{EngineError, SessionRuntimeConfig};
use robin_sparkless_jdbc::JdbcOptions;

use polars::prelude::DataFrame as PlDataFrame;

// MariaDB is protocol-compatible enough for the mysql crate in our use cases; we route to the
// MySQL backend while allowing the `jdbc:mariadb:` scheme.

pub(crate) fn write_jdbc_mariadb(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: JdbcSaveMode,
    runtime: &SessionRuntimeConfig,
) -> Result<(), EngineError> {
    super::mysql::write_jdbc_mysql(df, opts, mode, runtime)
}

pub(crate) fn read_jdbc_mariadb(
    opts: &JdbcOptions,
    runtime: &SessionRuntimeConfig,
) -> Result<PlDataFrame, EngineError> {
    super::mysql::read_jdbc_mysql(opts, runtime)
}
