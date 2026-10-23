use clap::{Parser, Subcommand};
use std::net::IpAddr;

use sparkless_server::config::ServerConfig;

#[derive(Debug, Parser)]
#[command(
    name = "sparkless-server",
    version,
    about = "Native Sparkless read-only SQL service"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the HTTP statement-agent server.
    Serve(ServeArgs),
}

#[derive(Debug, clap::Args)]
struct ServeArgs {
    /// Bind address (default: 127.0.0.1; env: SPARKLESS_SERVER_BIND).
    #[arg(long)]
    bind: Option<IpAddr>,
    /// HTTP port (default: 8099; use 0 for an ephemeral port).
    #[arg(long)]
    port: Option<u16>,
    /// Enable the owned JSON-lines stdin/stdout lifecycle protocol.
    #[arg(long)]
    control: bool,
    /// Per-launch identity supplied by an owning launcher.
    #[arg(long)]
    launch_id: Option<String>,
    #[arg(long)]
    max_active_operations: Option<usize>,
    #[arg(long)]
    max_pending_operations: Option<usize>,
    #[arg(long)]
    max_pending_per_session: Option<usize>,
    #[arg(long)]
    max_sessions: Option<usize>,
    #[arg(long)]
    session_idle_ttl_secs: Option<u64>,
    #[arg(long)]
    queue_wait_ms: Option<u64>,
    #[arg(long)]
    response_deadline_secs: Option<u64>,
    #[arg(long)]
    live_http_requests: Option<usize>,
    #[arg(long)]
    max_request_bytes: Option<usize>,
    #[arg(long)]
    max_sql_bytes: Option<usize>,
    #[arg(long)]
    body_read_timeout_secs: Option<u64>,
    #[arg(long)]
    result_send_timeout_secs: Option<u64>,
    #[arg(long)]
    max_sql_depth: Option<usize>,
    #[arg(long)]
    max_sql_nodes: Option<usize>,
    #[arg(long)]
    max_columns: Option<usize>,
    #[arg(long)]
    max_rows: Option<usize>,
    #[arg(long)]
    max_response_bytes: Option<usize>,
    #[arg(long)]
    max_cell_bytes: Option<usize>,
    #[arg(long)]
    retained_result_envelopes: Option<usize>,
    #[arg(long)]
    startup_timeout_secs: Option<u64>,
    #[arg(long)]
    drain_timeout_secs: Option<u64>,
    #[arg(long)]
    kill_timeout_secs: Option<u64>,
}

impl ServeArgs {
    fn apply(self, config: &mut ServerConfig) {
        if let Some(value) = self.bind {
            config.bind = value;
        }
        if let Some(value) = self.port {
            config.port = value;
        }
        config.control = self.control;
        config.launch_id = self.launch_id;
        macro_rules! override_setting {
            ($name:ident) => {
                if let Some(value) = self.$name {
                    config.limits.$name = value;
                }
            };
        }
        override_setting!(max_active_operations);
        override_setting!(max_pending_operations);
        override_setting!(max_pending_per_session);
        override_setting!(max_sessions);
        override_setting!(session_idle_ttl_secs);
        override_setting!(queue_wait_ms);
        override_setting!(response_deadline_secs);
        override_setting!(live_http_requests);
        override_setting!(max_request_bytes);
        override_setting!(max_sql_bytes);
        override_setting!(body_read_timeout_secs);
        override_setting!(result_send_timeout_secs);
        override_setting!(max_sql_depth);
        override_setting!(max_sql_nodes);
        override_setting!(max_columns);
        override_setting!(max_rows);
        override_setting!(max_response_bytes);
        override_setting!(max_cell_bytes);
        override_setting!(retained_result_envelopes);
        override_setting!(startup_timeout_secs);
        override_setting!(drain_timeout_secs);
        override_setting!(kill_timeout_secs);
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let Command::Serve(args) = cli.command;
    let result = ServerConfig::from_env().and_then(|mut config| {
        args.apply(&mut config);
        config.validate()?;
        Ok::<_, sparkless_server::ConfigError>(config)
    });
    match result {
        Ok(config) => {
            if let Err(error) = sparkless_server::http::serve(config).await {
                eprintln!("sparkless-server: {error}");
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("sparkless-server: invalid configuration: {error}");
            std::process::exit(2);
        }
    }
}
