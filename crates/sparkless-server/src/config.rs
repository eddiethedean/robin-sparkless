use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

/// Enforced transport and admission limits. CPU and intermediate native memory remain
/// cooperative budgets because a Polars call cannot be forcibly interrupted in-process.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub max_request_bytes: usize,
    pub max_sql_bytes: usize,
    pub max_result_rows: usize,
    pub max_result_bytes: usize,
    pub max_cell_bytes: usize,
    pub max_active_operations: usize,
    pub max_pending_operations: usize,
    pub max_sessions: usize,
    pub max_mutation_records: usize,
    pub max_mutation_bytes: usize,
    pub body_read_timeout: Duration,
    pub queue_timeout: Duration,
    pub response_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8099),
            max_request_bytes: 1024 * 1024,
            max_sql_bytes: 256 * 1024,
            max_result_rows: 10_000,
            max_result_bytes: 8 * 1024 * 1024,
            max_cell_bytes: 1024 * 1024,
            max_active_operations: 2,
            max_pending_operations: 16,
            max_sessions: 64,
            max_mutation_records: 1024,
            max_mutation_bytes: 4 * 1024 * 1024,
            body_read_timeout: Duration::from_secs(10),
            queue_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(30),
        }
    }
}

impl ServerConfig {
    /// Load defaults overridden by environment variables. CLI arguments are applied later.
    pub fn from_env() -> Result<Self, String> {
        let mut config = Self::default();
        if let Ok(host) = std::env::var("SPARKLESS_SERVER_HOST") {
            let ip: IpAddr = host
                .parse()
                .map_err(|_| "SPARKLESS_SERVER_HOST must be an IP address".to_string())?;
            config.bind.set_ip(ip);
        }
        if let Ok(port) = std::env::var("SPARKLESS_SERVER_PORT") {
            let port = parse_positive("SPARKLESS_SERVER_PORT", &port)?;
            let port = u16::try_from(port)
                .map_err(|_| "SPARKLESS_SERVER_PORT must be in 1..=65535".to_string())?;
            config.bind.set_port(port);
        }
        set_usize("SPARKLESS_MAX_REQUEST_BYTES", &mut config.max_request_bytes)?;
        set_usize("SPARKLESS_MAX_SQL_BYTES", &mut config.max_sql_bytes)?;
        set_usize("SPARKLESS_MAX_RESULT_ROWS", &mut config.max_result_rows)?;
        set_usize("SPARKLESS_MAX_RESULT_BYTES", &mut config.max_result_bytes)?;
        set_usize("SPARKLESS_MAX_CELL_BYTES", &mut config.max_cell_bytes)?;
        set_usize(
            "SPARKLESS_MAX_ACTIVE_OPERATIONS",
            &mut config.max_active_operations,
        )?;
        set_usize(
            "SPARKLESS_MAX_PENDING_OPERATIONS",
            &mut config.max_pending_operations,
        )?;
        set_usize("SPARKLESS_MAX_SESSIONS", &mut config.max_sessions)?;
        set_usize(
            "SPARKLESS_MAX_MUTATION_RECORDS",
            &mut config.max_mutation_records,
        )?;
        set_usize(
            "SPARKLESS_MAX_MUTATION_BYTES",
            &mut config.max_mutation_bytes,
        )?;
        set_duration_ms(
            "SPARKLESS_BODY_READ_TIMEOUT_MS",
            &mut config.body_read_timeout,
        )?;
        set_duration_ms("SPARKLESS_QUEUE_TIMEOUT_MS", &mut config.queue_timeout)?;
        set_duration_ms(
            "SPARKLESS_RESPONSE_TIMEOUT_MS",
            &mut config.response_timeout,
        )?;
        if config.max_result_bytes > config.max_request_bytes.saturating_mul(64)
            || config.max_cell_bytes > config.max_result_bytes
            || config.max_sql_bytes > config.max_request_bytes
        {
            return Err("server byte limits are contradictory".to_string());
        }
        if config.max_active_operations == 0 || config.max_sessions == 0 {
            return Err("active operation and session limits must be positive".to_string());
        }
        if config.max_mutation_bytes < config.max_mutation_records.saturating_mul(256) {
            return Err("mutation retention byte and record limits are contradictory".to_string());
        }
        Ok(config)
    }
}

fn set_usize(name: &str, target: &mut usize) -> Result<(), String> {
    if let Ok(value) = std::env::var(name) {
        *target = parse_positive(name, &value)?;
    }
    Ok(())
}

fn parse_positive(name: &str, value: &str) -> Result<usize, String> {
    let parsed: usize = value
        .parse()
        .map_err(|_| format!("{name} must be a positive integer"))?;
    if parsed == 0 {
        return Err(format!("{name} must be a positive integer"));
    }
    Ok(parsed)
}

fn set_duration_ms(name: &str, target: &mut Duration) -> Result<(), String> {
    if let Ok(value) = std::env::var(name) {
        let millis = parse_positive(name, &value)?;
        let millis = u64::try_from(millis).map_err(|_| format!("{name} is too large"))?;
        *target = Duration::from_millis(millis);
    }
    Ok(())
}
