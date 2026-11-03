use crate::config::{ConfiguredOwner, QUACK_CATALOG, valid_identifier};
use crate::connection::{duckdb_failure, quack_connection_id};
use crate::snapshot::{CancellationWatchdog, DuckDbReadSnapshot, QuackCancellation};
use crate::{
    DUCKDB_SOURCE_MAX_BATCH_BYTES, DUCKDB_SOURCE_MAX_BATCH_ROWS, DUCKDB_SOURCE_MAX_DECODED_BYTES,
    DUCKDB_SOURCE_MAX_IPC_BYTES, DUCKDB_SOURCE_MAX_ROWS,
};
use robin_sparkless_core::engine::{
    ArrowIpcStream, CancellationToken, CatalogCapabilities, CatalogOwnership, CatalogReadSnapshot,
    ExternalTableDescriptor, ExternalTableFormat,
};
use robin_sparkless_core::{DataType, EngineError};
use std::sync::atomic::Ordering;

impl CatalogReadSnapshot for DuckDbReadSnapshot {
    fn resolve_ownership(&self, identifier: &str) -> Result<CatalogOwnership, EngineError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        let Some(alias) = parts.first() else {
            return Ok(CatalogOwnership::Local);
        };
        if !(2..=3).contains(&parts.len()) || !parts.iter().all(|part| valid_identifier(part)) {
            return Ok(CatalogOwnership::Local);
        }
        let alias = alias.to_ascii_lowercase();
        Ok(self
            .sources
            .get(&alias)
            .map_or(CatalogOwnership::Local, |source| {
                CatalogOwnership::external(
                    format!("duckdb:{}", source.alias),
                    CatalogCapabilities::READ_ONLY,
                )
            }))
    }

    fn resolve_table(
        &self,
        identifier: &str,
    ) -> Result<Option<ExternalTableDescriptor>, EngineError> {
        let parts = identifier.split('.').collect::<Vec<_>>();
        let (alias, schema, table) = match parts.as_slice() {
            [alias, table] => (*alias, "main", *table),
            [alias, schema, table] => (*alias, *schema, *table),
            _ => return Ok(None),
        };
        if !valid_identifier(alias) || !valid_identifier(schema) || !valid_identifier(table) {
            return Ok(None);
        }
        let Some(source) = self.sources.get(&alias.to_ascii_lowercase()).cloned() else {
            return Ok(None);
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| EngineError::internal("DuckDB snapshot lock poisoned".into()))?;
        let result = self.resolve_in_source(&mut state, source, schema, table);
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }

    fn scan_table(
        &self,
        table: &ExternalTableDescriptor,
        token: &CancellationToken,
        consume: &mut dyn FnMut(ArrowIpcStream) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        let result = (|| {
            table.validate()?;
            if table.format != ExternalTableFormat::DuckDb {
                return Err(EngineError::invalid_input(
                    "DuckDB source received a descriptor for another table format".into(),
                ));
            }
            let context: serde_json::Value =
                serde_json::from_str(table.permissions_context.as_deref().ok_or_else(|| {
                    EngineError::invalid_input("DuckDB table descriptor is incomplete".into())
                })?)
                .map_err(|error| {
                    EngineError::invalid_input(format!("invalid DuckDB table context: {error}"))
                })?;
            let alias = context["alias"].as_str().ok_or_else(|| {
                EngineError::invalid_input(
                    "DuckDB table descriptor is missing its source alias".into(),
                )
            })?;
            let schema = context["schema"].as_str().ok_or_else(|| {
                EngineError::invalid_input("DuckDB table descriptor is missing its schema".into())
            })?;
            let table_name = context["table"].as_str().ok_or_else(|| {
                EngineError::invalid_input(
                    "DuckDB table descriptor is missing its table name".into(),
                )
            })?;
            if !valid_identifier(alias)
                || !valid_identifier(schema)
                || !valid_identifier(table_name)
            {
                return Err(EngineError::invalid_input(
                    "DuckDB table descriptor contains an invalid identifier".into(),
                ));
            }
            let source = self.source_for(alias)?;
            if table.canonical_location != source.owner.identity() {
                return Err(EngineError::invalid_input(
                    "DuckDB table descriptor does not match its configured source owner".into(),
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| EngineError::internal("DuckDB snapshot lock poisoned".into()))?;
            let connection = self.ensure_connection(&mut state, &source)?;
            token.check_cancelled()?;
            let quack_cancel = match &source.owner {
                ConfiguredOwner::Local { .. } => None,
                ConfiguredOwner::Quack { .. } => Some(QuackCancellation {
                    owner: source.owner.clone(),
                    connection_id: quack_connection_id(connection)?,
                }),
            };
            let _watchdog = CancellationWatchdog::new(
                token.clone(),
                connection.interrupt_handle(),
                quack_cancel,
            )?;
            let table_reference = match &source.owner {
                ConfiguredOwner::Local { .. } => format!(
                    "{}.{}",
                    quote_identifier(schema),
                    quote_identifier(table_name)
                ),
                ConfiguredOwner::Quack { .. } => format!(
                    "{}.{}.{}",
                    quote_identifier(QUACK_CATALOG),
                    quote_identifier(schema),
                    quote_identifier(table_name)
                ),
            };
            let sql = format!("SELECT * FROM {table_reference}");
            let mut statement = connection.prepare(&sql).map_err(duckdb_failure)?;
            let batches = statement.stream_arrow([]).map_err(duckdb_failure)?;
            for batch in batches {
                token.check_cancelled()?;
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => {
                        token.check_cancelled()?;
                        return Err(duckdb_failure(error));
                    }
                };
                if batch.num_rows() > DUCKDB_SOURCE_MAX_BATCH_ROWS {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB source batch exceeds {DUCKDB_SOURCE_MAX_BATCH_ROWS} rows"
                    )));
                }
                validate_batch_scalars(&batch)?;
                let bytes = encode_batch(&batch)?;
                if bytes.len() > DUCKDB_SOURCE_MAX_BATCH_BYTES {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB source batch exceeds {DUCKDB_SOURCE_MAX_BATCH_BYTES} IPC bytes"
                    )));
                }
                let next_rows = self
                    .rows
                    .load(Ordering::Acquire)
                    .checked_add(batch.num_rows() as u64)
                    .ok_or_else(|| {
                        EngineError::invalid_input("DuckDB source row count overflow".into())
                    })?;
                let next_bytes = self
                    .ipc_bytes
                    .load(Ordering::Acquire)
                    .checked_add(bytes.len())
                    .ok_or_else(|| {
                        EngineError::invalid_input("DuckDB source IPC byte count overflow".into())
                    })?;
                if next_rows > DUCKDB_SOURCE_MAX_ROWS {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB source exceeds the {DUCKDB_SOURCE_MAX_ROWS}-row operation limit"
                    )));
                }
                if next_bytes > DUCKDB_SOURCE_MAX_IPC_BYTES {
                    self.failed.store(true, Ordering::Release);
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB source exceeds the {DUCKDB_SOURCE_MAX_IPC_BYTES}-byte operation limit"
                    )));
                }
                self.rows.store(next_rows, Ordering::Release);
                self.ipc_bytes.store(next_bytes, Ordering::Release);
                consume(ArrowIpcStream {
                    bytes,
                    row_count: batch.num_rows() as u64,
                })?;
            }
            token.check_cancelled()?;
            Ok(())
        })();
        if result.is_err() {
            self.failed.store(true, Ordering::Release);
        }
        result
    }
    fn charge_decoded_bytes(&self, bytes: usize) -> Result<(), EngineError> {
        match self
            .decoded_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(bytes)
                    .filter(|next| *next <= DUCKDB_SOURCE_MAX_DECODED_BYTES)
            }) {
            Ok(_) => Ok(()),
            Err(_) => {
                self.failed.store(true, Ordering::Release);
                Err(EngineError::invalid_input(format!(
                    "DuckDB source decoded data exceeds the {DUCKDB_SOURCE_MAX_DECODED_BYTES}-byte statement limit"
                )))
            }
        }
    }
}

/// Interrupts an in-flight DuckDB query promptly when its operation token is cancelled.
pub(crate) fn validate_batch_scalars(
    batch: &duckdb::arrow::record_batch::RecordBatch,
) -> Result<(), EngineError> {
    use arrow60::array::{
        Array, Date32Array, Float64Array, StringArray, TimestampMicrosecondArray,
    };
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        match field.data_type() {
            arrow60::datatypes::DataType::Date32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .ok_or_else(|| {
                        EngineError::invalid_input(format!(
                            "DuckDB DATE column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len()).any(|index| {
                    let value = values.value(index);
                    !values.is_null(index)
                        && (value == i32::MIN || value == i32::MIN + 1 || value == i32::MAX)
                }) {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB DATE column {:?} contains an infinite value",
                        field.name()
                    )));
                }
            }
            arrow60::datatypes::DataType::Timestamp(
                arrow60::datatypes::TimeUnit::Microsecond,
                _,
            ) => {
                let values = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        EngineError::invalid_input(format!(
                            "DuckDB TIMESTAMP column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len()).any(|index| {
                    let value = values.value(index);
                    !values.is_null(index)
                        && (value == i64::MIN || value == i64::MIN + 1 || value == i64::MAX)
                }) {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB TIMESTAMP column {:?} contains an infinite value",
                        field.name()
                    )));
                }
            }
            arrow60::datatypes::DataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        EngineError::invalid_input(format!(
                            "DuckDB DOUBLE column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len())
                    .any(|index| !values.is_null(index) && !values.value(index).is_finite())
                {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB DOUBLE column {:?} contains a non-finite value",
                        field.name()
                    )));
                }
            }
            arrow60::datatypes::DataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        EngineError::invalid_input(format!(
                            "DuckDB VARCHAR column {:?} has an unexpected Arrow representation",
                            field.name()
                        ))
                    })?;
                if (0..values.len())
                    .any(|index| !values.is_null(index) && values.value(index).len() > 512 * 1024)
                {
                    return Err(EngineError::invalid_input(format!(
                        "DuckDB VARCHAR column {:?} contains a value larger than 512 KiB",
                        field.name()
                    )));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn duckdb_type_to_spark(type_name: &str) -> Result<DataType, EngineError> {
    let normalized = type_name.trim().to_ascii_uppercase();
    match normalized.as_str() {
        "BOOLEAN" => Ok(DataType::Boolean),
        "SMALLINT" | "INT2" => Ok(DataType::Short),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => Ok(DataType::Integer),
        "BIGINT" | "INT8" | "LONG" => Ok(DataType::Long),
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Double),
        "VARCHAR" | "STRING" | "TEXT" => Ok(DataType::String),
        "DATE" => Ok(DataType::Date),
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => Ok(DataType::Timestamp),
        _ if normalized.starts_with("DECIMAL(") && normalized.ends_with(')') => {
            let inner = &normalized[8..normalized.len() - 1];
            let (precision, scale) = inner.split_once(',').ok_or_else(|| {
                EngineError::invalid_input(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            let precision: u8 = precision.trim().parse().map_err(|_| {
                EngineError::invalid_input(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            let scale: u8 = scale.trim().parse().map_err(|_| {
                EngineError::invalid_input(format!("unsupported DuckDB decimal type '{type_name}'"))
            })?;
            if precision == 0 || precision > 38 || scale > precision {
                return Err(EngineError::invalid_input(format!(
                    "DuckDB decimal type '{type_name}' is outside the supported precision/scale range"
                )));
            }
            Ok(DataType::Decimal(precision, scale))
        }
        _ => Err(EngineError::invalid_input(format!(
            "DuckDB type '{type_name}' is not in the supported source type set"
        ))),
    }
}

pub(crate) fn encode_batch(
    batch: &duckdb::arrow::record_batch::RecordBatch,
) -> Result<Vec<u8>, EngineError> {
    use arrow60::ipc::writer::StreamWriter;
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, &batch.schema()).map_err(|error| {
            EngineError::invalid_input(format!("cannot encode DuckDB Arrow schema: {error}"))
        })?;
        writer.write(batch).map_err(|error| {
            EngineError::invalid_input(format!("cannot encode DuckDB Arrow batch: {error}"))
        })?;
        writer.finish().map_err(|error| {
            EngineError::invalid_input(format!("cannot finish DuckDB Arrow batch: {error}"))
        })?;
    }
    Ok(bytes)
}

pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

pub(crate) fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
