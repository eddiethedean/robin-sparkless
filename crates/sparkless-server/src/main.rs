use serde::Deserialize;
use sparkless_server::{CONTROL_PROTOCOL_VERSION, Server, ServerConfig};
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use tokio::io::{AsyncBufRead, AsyncBufReadExt};

const MAX_CONTROL_FRAME_BYTES: usize = 16 * 1024;

fn main() {
    if let Err(error) = run() {
        eprintln!("sparkless-server: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let mode = match args.next().as_deref() {
        Some("serve") => false,
        Some("control") => true,
        Some("--help") | Some("help") | None => {
            println!(
                "Usage:\n  sparkless-server serve [--host IP] [--port PORT] [--connect-host IP] [--connect-port PORT] [--disable-http] [--disable-connect]\n  sparkless-server control [--host IP] [--port PORT] [--connect-host IP] [--connect-port PORT] [--disable-http] [--disable-connect]\n  sparkless-server --version\n\nSpark Connect defaults to 127.0.0.1:15002 when built with the connect feature. Port 0 asks the OS to allocate an ephemeral port. Use --disable-http or --disable-connect to select one listener.\nThe control command writes bounded JSON-lines readiness and stopped records to stdout and accepts a shutdown command on stdin.\n\nEnvironment: SPARKLESS_SERVER_HOST, SPARKLESS_SERVER_PORT, SPARKLESS_SESSION_IDLE_TTL_MS"
            );
            return Ok(());
        }
        Some("--version") | Some("-V") => {
            println!("sparkless-server {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Some(command) => {
            return Err(
                format!("unknown command '{command}'; expected 'serve' or 'control'").into(),
            );
        }
    };
    #[cfg(feature = "http")]
    let mut config = ServerConfig::from_env().map_err(std::io::Error::other)?;
    #[cfg(not(feature = "http"))]
    let config = ServerConfig::from_env().map_err(std::io::Error::other)?;
    #[cfg(feature = "http")]
    let mut http_bind = Some(config.bind);
    #[cfg(not(feature = "http"))]
    let mut http_bind: Option<SocketAddr> = None;
    #[cfg(feature = "connect")]
    let mut connect_bind = Some(SocketAddr::new(
        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        15002,
    ));
    #[cfg(not(feature = "connect"))]
    let mut connect_bind: Option<SocketAddr> = None;
    while let Some(flag) = args.next() {
        if flag == "--disable-http" {
            http_bind = None;
            continue;
        }
        if flag == "--disable-connect" {
            connect_bind = None;
            continue;
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--host" => {
                #[cfg(feature = "http")]
                {
                    let ip: IpAddr = value.parse().map_err(|_| "--host must be an IP address")?;
                    config.bind = SocketAddr::new(ip, config.bind.port());
                    if http_bind.is_some() {
                        http_bind = Some(config.bind);
                    }
                }
                #[cfg(not(feature = "http"))]
                return Err("--host requires the server http feature".into());
            }
            "--port" => {
                #[cfg(feature = "http")]
                {
                    let port: u16 = value.parse().map_err(|_| "--port must be in 0..=65535")?;
                    config.bind.set_port(port);
                    if http_bind.is_some() {
                        http_bind = Some(config.bind);
                    }
                }
                #[cfg(not(feature = "http"))]
                return Err("--port requires the server http feature".into());
            }
            "--connect-host" => {
                #[cfg(feature = "connect")]
                {
                    let ip: IpAddr = value
                        .parse()
                        .map_err(|_| "--connect-host must be an IP address")?;
                    let address = connect_bind.unwrap_or_else(|| {
                        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 15002)
                    });
                    connect_bind = Some(SocketAddr::new(ip, address.port()));
                }
                #[cfg(not(feature = "connect"))]
                return Err("--connect-host requires the server connect feature".into());
            }
            "--connect-port" => {
                #[cfg(feature = "connect")]
                {
                    let port: u16 = value
                        .parse()
                        .map_err(|_| "--connect-port must be in 0..=65535")?;
                    let address = connect_bind.unwrap_or_else(|| {
                        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 15002)
                    });
                    connect_bind = Some(SocketAddr::new(address.ip(), port));
                }
                #[cfg(not(feature = "connect"))]
                return Err("--connect-port requires the server connect feature".into());
            }
            _ => return Err(format!("unknown argument '{flag}'").into()),
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let server = Server::new(config)?;
    if mode {
        runtime.block_on(serve_control(server, http_bind, connect_bind))?;
    } else {
        runtime.block_on(server.serve_until_with_transports(
            shutdown_signal(),
            http_bind,
            connect_bind,
            |_, _| Ok(()),
        ))?;
    }
    Ok(())
}

async fn serve_control(
    server: Server,
    http_bind: Option<SocketAddr>,
    connect_bind: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ready_server = server.clone();
    server
        .serve_until_with_transports(
            control_shutdown(),
            http_bind,
            connect_bind,
            move |http_bound, _| {
                write_control_record(&ready_server.readiness_record_with_endpoints(http_bound))
            },
        )
        .await?;
    write_control_record(&serde_json::json!({
        "type": "stopped",
        "control_protocol_version": CONTROL_PROTOCOL_VERSION
    }))?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn write_control_record(value: &serde_json::Value) -> Result<(), sparkless_server::ServerError> {
    let mut record = serde_json::to_vec(value)
        .map_err(|error| io::Error::other(format!("could not encode control record: {error}")))?;
    if record.len().saturating_add(1) > MAX_CONTROL_FRAME_BYTES {
        return Err(io::Error::other("control record exceeds the frame limit").into());
    }
    record.push(b'\n');
    let mut stdout = io::stdout().lock();
    stdout.write_all(&record)?;
    stdout.flush()?;
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlCommand {
    control_protocol_version: u32,
    command: String,
}

enum ControlFrame {
    Line(Vec<u8>),
    TooLarge,
}

async fn control_shutdown() {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate_signal() => {},
        _ = read_control_commands() => {},
    }
}

#[cfg(unix)]
async fn terminate_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    let _ = terminate.recv().await;
}

#[cfg(not(unix))]
async fn terminate_signal() {
    std::future::pending::<()>().await;
}

async fn read_control_commands() {
    let stdin = tokio::io::stdin();
    let mut reader = tokio::io::BufReader::new(stdin);
    loop {
        let frame = match read_control_frame(&mut reader).await {
            Ok(Some(frame)) => frame,
            Ok(None) => return,
            Err(error) => {
                eprintln!("sparkless-server: control input failed: {error}");
                return;
            }
        };
        let ControlFrame::Line(line) = frame else {
            eprintln!("sparkless-server: ignored oversized control frame");
            continue;
        };
        let command: ControlCommand = match serde_json::from_slice(&line) {
            Ok(command) => command,
            Err(error) => {
                eprintln!("sparkless-server: ignored invalid control frame: {error}");
                continue;
            }
        };
        if command.control_protocol_version != CONTROL_PROTOCOL_VERSION {
            eprintln!(
                "sparkless-server: ignored control command with unsupported protocol version {}",
                command.control_protocol_version
            );
            continue;
        }
        if command.command == "shutdown" {
            return;
        }
        eprintln!("sparkless-server: ignored unsupported control command");
    }
}

async fn read_control_frame<R>(reader: &mut R) -> io::Result<Option<ControlFrame>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    let mut too_large = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if line.is_empty() && !too_large {
                return Ok(None);
            }
            return Ok(Some(if too_large {
                ControlFrame::TooLarge
            } else {
                ControlFrame::Line(line)
            }));
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let content_len = newline.unwrap_or(consumed);
        if !too_large {
            // The frame limit includes its terminating newline when present.
            if line.len().saturating_add(consumed) > MAX_CONTROL_FRAME_BYTES {
                too_large = true;
                line.clear();
            } else {
                line.extend_from_slice(&available[..content_len]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            return Ok(Some(if too_large {
                ControlFrame::TooLarge
            } else {
                ControlFrame::Line(line)
            }));
        }
    }
}
