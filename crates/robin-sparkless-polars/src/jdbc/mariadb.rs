use crate::error::EngineError;
use crate::jdbc::JdbcOptions;

use polars::prelude::DataFrame as PlDataFrame;

// MariaDB is protocol-compatible enough for the mysql crate in our use cases; we route to the
// MySQL backend while allowing the `jdbc:mariadb:` scheme.

pub(crate) fn write_jdbc_mariadb(
    df: &PlDataFrame,
    opts: &JdbcOptions,
    mode: crate::dataframe::SaveMode,
) -> Result<(), EngineError> {
    super::mysql::write_jdbc_mysql(df, opts, mode)
}

pub(crate) fn read_jdbc_mariadb(opts: &JdbcOptions) -> Result<PlDataFrame, EngineError> {
    super::mysql::read_jdbc_mysql(opts)
}

