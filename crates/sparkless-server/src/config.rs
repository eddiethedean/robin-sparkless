use std::env;
use std::net::{IpAddr, Ipv4Addr};

pub const DEFAULT_BIND: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);
pub const DEFAULT_PORT: u16 = 8099;

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_active_operations: usize,
    pub max_pending_operations: usize,
    pub max_pending_per_session: usize,
    pub max_sessions: usize,
    pub session_idle_ttl_secs: u64,
    pub queue_wait_ms: u64,
    pub response_deadline_secs: u64,
    pub live_http_requests: usize,
    pub max_request_bytes: usize,
    pub max_sql_bytes: usize,
    pub body_read_timeout_secs: u64,
    pub result_send_timeout_secs: u64,
    pub max_sql_depth: usize,
    pub max_sql_nodes: usize,
    pub max_columns: usize,
    pub max_rows: usize,
    pub max_response_bytes: usize,
    pub max_cell_bytes: usize,
    pub retained_result_envelopes: usize,
    pub startup_timeout_secs: u64,
    pub drain_timeout_secs: u64,
    pub kill_timeout_secs: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_active_operations: 2,
            max_pending_operations: 16,
            max_pending_per_session: 4,
            max_sessions: 64,
            session_idle_ttl_secs: 15 * 60,
            queue_wait_ms: 5_000,
            response_deadline_secs: 30,
            live_http_requests: 32,
            max_request_bytes: 1024 * 1024,
            max_sql_bytes: 256 * 1024,
            body_read_timeout_secs: 5,
            result_send_timeout_secs: 5,
            max_sql_depth: 64,
            max_sql_nodes: 10_000,
            max_columns: 256,
            max_rows: 10_000,
            max_response_bytes: 8 * 1024 * 1024,
            max_cell_bytes: 1024 * 1024,
            retained_result_envelopes: 2,
            startup_timeout_secs: 30,
            drain_timeout_secs: 10,
            kill_timeout_secs: 5,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: IpAddr,
    pub port: u16,
    pub control: bool,
    pub launch_id: Option<String>,
    pub limits: Limits,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: DEFAULT_BIND,
            port: DEFAULT_PORT,
            control: false,
            launch_id: None,
            limits: Limits::default(),
        }
    }
}

impl ServerConfig {
    /// Load defaults, then `SPARKLESS_SERVER_*` environment overrides.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Self::default();
        if let Some(value) = env_value::<IpAddr>("SPARKLESS_SERVER_BIND")? {
            config.bind = value;
        }
        if let Some(value) = env_value::<u16>("SPARKLESS_SERVER_PORT")? {
            config.port = value;
        }
        macro_rules! setting {
            ($field:ident, $name:literal, $kind:ty) => {
                if let Some(value) = env_value::<$kind>($name)? {
                    config.limits.$field = value;
                }
            };
        }
        setting!(
            max_active_operations,
            "SPARKLESS_SERVER_MAX_ACTIVE_OPERATIONS",
            usize
        );
        setting!(
            max_pending_operations,
            "SPARKLESS_SERVER_MAX_PENDING_OPERATIONS",
            usize
        );
        setting!(
            max_pending_per_session,
            "SPARKLESS_SERVER_MAX_PENDING_PER_SESSION",
            usize
        );
        setting!(max_sessions, "SPARKLESS_SERVER_MAX_SESSIONS", usize);
        setting!(
            session_idle_ttl_secs,
            "SPARKLESS_SERVER_SESSION_IDLE_TTL_SECS",
            u64
        );
        setting!(queue_wait_ms, "SPARKLESS_SERVER_QUEUE_WAIT_MS", u64);
        setting!(
            response_deadline_secs,
            "SPARKLESS_SERVER_RESPONSE_DEADLINE_SECS",
            u64
        );
        setting!(
            live_http_requests,
            "SPARKLESS_SERVER_LIVE_HTTP_REQUESTS",
            usize
        );
        setting!(
            max_request_bytes,
            "SPARKLESS_SERVER_MAX_REQUEST_BYTES",
            usize
        );
        setting!(max_sql_bytes, "SPARKLESS_SERVER_MAX_SQL_BYTES", usize);
        setting!(
            body_read_timeout_secs,
            "SPARKLESS_SERVER_BODY_READ_TIMEOUT_SECS",
            u64
        );
        setting!(
            result_send_timeout_secs,
            "SPARKLESS_SERVER_RESULT_SEND_TIMEOUT_SECS",
            u64
        );
        setting!(max_sql_depth, "SPARKLESS_SERVER_MAX_SQL_DEPTH", usize);
        setting!(max_sql_nodes, "SPARKLESS_SERVER_MAX_SQL_NODES", usize);
        setting!(max_columns, "SPARKLESS_SERVER_MAX_COLUMNS", usize);
        setting!(max_rows, "SPARKLESS_SERVER_MAX_ROWS", usize);
        setting!(
            max_response_bytes,
            "SPARKLESS_SERVER_MAX_RESPONSE_BYTES",
            usize
        );
        setting!(max_cell_bytes, "SPARKLESS_SERVER_MAX_CELL_BYTES", usize);
        setting!(
            retained_result_envelopes,
            "SPARKLESS_SERVER_RETAINED_RESULT_ENVELOPES",
            usize
        );
        setting!(
            startup_timeout_secs,
            "SPARKLESS_SERVER_STARTUP_TIMEOUT_SECS",
            u64
        );
        setting!(
            drain_timeout_secs,
            "SPARKLESS_SERVER_DRAIN_TIMEOUT_SECS",
            u64
        );
        setting!(kill_timeout_secs, "SPARKLESS_SERVER_KILL_TIMEOUT_SECS", u64);
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let l = &self.limits;
        let nonzero = [
            ("max_active_operations", l.max_active_operations),
            ("max_pending_operations", l.max_pending_operations),
            ("max_pending_per_session", l.max_pending_per_session),
            ("max_sessions", l.max_sessions),
            ("session_idle_ttl_secs", l.session_idle_ttl_secs as usize),
            ("queue_wait_ms", l.queue_wait_ms as usize),
            ("response_deadline_secs", l.response_deadline_secs as usize),
            ("live_http_requests", l.live_http_requests),
            ("max_request_bytes", l.max_request_bytes),
            ("max_sql_bytes", l.max_sql_bytes),
            ("body_read_timeout_secs", l.body_read_timeout_secs as usize),
            (
                "result_send_timeout_secs",
                l.result_send_timeout_secs as usize,
            ),
            ("max_sql_depth", l.max_sql_depth),
            ("max_sql_nodes", l.max_sql_nodes),
            ("max_columns", l.max_columns),
            ("max_rows", l.max_rows),
            ("max_response_bytes", l.max_response_bytes),
            ("max_cell_bytes", l.max_cell_bytes),
            ("retained_result_envelopes", l.retained_result_envelopes),
            ("startup_timeout_secs", l.startup_timeout_secs as usize),
            ("drain_timeout_secs", l.drain_timeout_secs as usize),
            ("kill_timeout_secs", l.kill_timeout_secs as usize),
        ];
        if let Some((name, _)) = nonzero.iter().find(|(_, value)| *value == 0) {
            return Err(ConfigError(format!("{name} must be greater than zero")));
        }
        if l.max_active_operations
            .checked_add(l.max_pending_operations)
            .is_none()
        {
            return Err(ConfigError(
                "active plus pending operation capacity overflows".into(),
            ));
        }
        if l.max_pending_per_session > l.max_pending_operations {
            return Err(ConfigError(
                "max_pending_per_session cannot exceed max_pending_operations".into(),
            ));
        }
        if l.max_sql_bytes > l.max_request_bytes {
            return Err(ConfigError(
                "max_sql_bytes cannot exceed max_request_bytes".into(),
            ));
        }
        if l.max_cell_bytes > l.max_response_bytes {
            return Err(ConfigError(
                "max_cell_bytes cannot exceed max_response_bytes".into(),
            ));
        }
        if l.max_request_bytes > 64 * 1024 * 1024
            || l.max_sql_bytes > 16 * 1024 * 1024
            || l.max_response_bytes > 64 * 1024 * 1024
            || l.max_cell_bytes > 8 * 1024 * 1024
            || l.live_http_requests > 4096
            || l.retained_result_envelopes > 128
            || l.max_sessions > 4096
            || l.max_active_operations > 128
            || l.max_pending_operations > 4096
            || l.max_rows > 1_000_000
            || l.max_columns > 4096
            || l.max_sql_depth > 256
            || l.max_sql_nodes > 1_000_000
            || l.session_idle_ttl_secs > 365 * 24 * 60 * 60
            || l.queue_wait_ms > 60 * 60 * 1000
            || l.response_deadline_secs > 24 * 60 * 60
            || l.body_read_timeout_secs > 60 * 60
            || l.result_send_timeout_secs > 60 * 60
            || l.startup_timeout_secs > 60 * 60
            || l.drain_timeout_secs > 60 * 60
            || l.kill_timeout_secs > 60 * 60
        {
            return Err(ConfigError(
                "one or more limits exceed the server hard ceiling".into(),
            ));
        }
        Ok(())
    }
}

fn env_value<T: std::str::FromStr>(name: &str) -> Result<Option<T>, ConfigError>
where
    T::Err: std::fmt::Display,
{
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|e| ConfigError(format!("invalid {name}: {e}"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(ConfigError(format!("invalid {name}: {error}"))),
    }
}

#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}
