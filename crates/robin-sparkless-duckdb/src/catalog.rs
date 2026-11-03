use crate::config::{
    ConfiguredOwner, ConfiguredSource, DuckDbSourceConfig, DuckDbSourceSpec,
    resolve_quack_extension_path, valid_identifier, validate_quack_uri,
};
use crate::connection_pool::SourceConnectionPool;
use crate::snapshot::DuckDbReadSnapshot;
use robin_sparkless_core::EngineError;
use robin_sparkless_core::engine::{
    CancellationToken, CatalogCapabilities, CatalogOwnership, CatalogReadSnapshot, CatalogReader,
    CatalogResolver, ExternalCatalog, ExternalTableDescriptor,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Read-only DuckDB table resolver. This source is opt-in through the `duckdb-source` feature.
pub struct DuckDbCatalog {
    pub(crate) sources: HashMap<String, Arc<ConfiguredSource>>,
    pub(crate) pool: Arc<SourceConnectionPool>,
}

impl DuckDbCatalog {
    /// Validate and freeze trusted source aliases and paths.
    pub fn new(configs: impl IntoIterator<Item = DuckDbSourceConfig>) -> Result<Self, EngineError> {
        Self::new_with_sources(configs.into_iter().map(DuckDbSourceSpec::Local))
    }

    /// Validate and freeze trusted local-file and Quack source definitions.
    pub fn new_with_sources(
        configs: impl IntoIterator<Item = DuckDbSourceSpec>,
    ) -> Result<Self, EngineError> {
        let mut sources = HashMap::new();
        for spec in configs {
            let (alias_value, owner) = match spec {
                DuckDbSourceSpec::Local(config) => {
                    let database_path =
                        std::fs::canonicalize(&config.database_path).map_err(|error| {
                            EngineError::invalid_input(format!(
                                "DuckDB source '{}' database path is unavailable: {error}",
                                config.alias
                            ))
                        })?;
                    if !database_path.is_file() {
                        return Err(EngineError::invalid_input(format!(
                            "DuckDB source '{}' path is not a database file",
                            config.alias
                        )));
                    }
                    (config.alias, ConfiguredOwner::Local { database_path })
                }
                DuckDbSourceSpec::Quack(config) => {
                    validate_quack_uri(&config.quack_uri)?;
                    if config.token.trim().len() < 4 {
                        return Err(EngineError::invalid_input(format!(
                            "DuckDB Quack source '{}' requires a token of at least four characters",
                            config.alias
                        )));
                    }
                    if config.disable_ssl == Some(true) && config.ssl_fingerprint.is_some() {
                        return Err(EngineError::invalid_input(format!(
                            "DuckDB Quack source '{}' cannot set an SSL fingerprint when SSL is disabled",
                            config.alias
                        )));
                    }
                    let extension_path = resolve_quack_extension_path(&config.extension_path)
                        .map_err(|error| {
                            EngineError::invalid_input(format!(
                                "DuckDB Quack source '{}' extension path is unavailable: {error}",
                                config.alias
                            ))
                        })?;
                    if !extension_path.is_file()
                        || extension_path.file_name().and_then(|name| name.to_str())
                            != Some("quack.duckdb_extension")
                    {
                        return Err(EngineError::invalid_input(format!(
                            "DuckDB Quack source '{}' extension path must name quack.duckdb_extension",
                            config.alias
                        )));
                    }
                    (
                        config.alias,
                        ConfiguredOwner::Quack {
                            uri: config.quack_uri,
                            token: config.token,
                            extension_path,
                            disable_ssl: config.disable_ssl,
                            ssl_fingerprint: config.ssl_fingerprint,
                        },
                    )
                }
            };
            if let Some(endpoint) = owner.quack_endpoint()
                && sources.values().any(|configured: &Arc<ConfiguredSource>| {
                    configured.owner.quack_endpoint() == Some(endpoint)
                        && !configured.owner.same_quack_connection(&owner)
                })
            {
                return Err(EngineError::invalid_input(
                    "DuckDB Quack aliases for one endpoint must use the same token and transport settings"
                        .into(),
                ));
            }
            if !valid_identifier(&alias_value) {
                return Err(EngineError::invalid_input(format!(
                    "DuckDB source alias {:?} must be a simple identifier",
                    alias_value
                )));
            }
            let alias = alias_value.to_ascii_lowercase();
            if sources.contains_key(&alias) {
                return Err(EngineError::invalid_input(format!(
                    "duplicate DuckDB source alias '{alias}'"
                )));
            }
            sources.insert(alias.clone(), Arc::new(ConfiguredSource { alias, owner }));
        }
        if sources.is_empty() {
            return Err(EngineError::invalid_input(
                "at least one DuckDB source alias is required".into(),
            ));
        }
        Ok(Self {
            sources,
            pool: Arc::new(SourceConnectionPool::default()),
        })
    }

    /// Parse the JSON value used by `SPARKLESS_DUCKDB_SOURCES`.
    pub fn from_json(json: &str) -> Result<Self, EngineError> {
        let configs: Vec<DuckDbSourceSpec> = serde_json::from_str(json).map_err(|error| {
            EngineError::invalid_input(format!("invalid SPARKLESS_DUCKDB_SOURCES JSON: {error}"))
        })?;
        Self::new_with_sources(configs)
    }
}

impl CatalogResolver for DuckDbCatalog {
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
        let Some((source, schema_name, table_name)) = self.parse_identifier(identifier)? else {
            return Ok(None);
        };
        let snapshot = DuckDbReadSnapshot::new(
            self.sources.clone(),
            Arc::clone(&self.pool),
            CancellationToken::new(),
        );
        let mut state = snapshot
            .state
            .lock()
            .map_err(|_| EngineError::internal("DuckDB snapshot lock poisoned".into()))?;
        snapshot.resolve_in_source(&mut state, source, &schema_name, &table_name)
    }
}

impl CatalogReader for DuckDbCatalog {
    fn begin_snapshot(
        &self,
        token: &CancellationToken,
    ) -> Result<Arc<dyn CatalogReadSnapshot>, EngineError> {
        token.check_cancelled()?;
        Ok(Arc::new(DuckDbReadSnapshot::new(
            self.sources.clone(),
            Arc::clone(&self.pool),
            token.clone(),
        )))
    }
}

impl DuckDbCatalog {
    /// Compose the configured source as a read-only external catalog capability.
    pub fn into_external_catalog(self: Arc<Self>) -> Arc<ExternalCatalog> {
        let resolver: Arc<dyn CatalogResolver> = self.clone();
        let reader: Arc<dyn CatalogReader> = self;
        Arc::new(ExternalCatalog::new(resolver, Some(reader), None))
    }
}

impl DuckDbCatalog {
    pub(crate) fn parse_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<(Arc<ConfiguredSource>, String, String)>, EngineError> {
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
        Ok(Some((source, schema.to_string(), table.to_string())))
    }
}
