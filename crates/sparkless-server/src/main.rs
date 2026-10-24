use sparkless_server::{Server, ServerConfig};
use std::net::{IpAddr, SocketAddr};

fn main() {
    if let Err(error) = run() {
        eprintln!("sparkless-server: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = ServerConfig::from_env().map_err(std::io::Error::other)?;
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") => {}
        Some("--help") | Some("help") | None => {
            println!(
                "Usage: sparkless-server serve [--host IP] [--port PORT]\n\nEnvironment: SPARKLESS_SERVER_HOST, SPARKLESS_SERVER_PORT"
            );
            return Ok(());
        }
        Some(command) => {
            return Err(format!("unknown command '{command}'; expected 'serve'").into());
        }
    }
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} requires a value"))?;
        match flag.as_str() {
            "--host" => {
                let ip: IpAddr = value.parse().map_err(|_| "--host must be an IP address")?;
                config.bind = SocketAddr::new(ip, config.bind.port());
            }
            "--port" => {
                let port: u16 = value.parse().map_err(|_| "--port must be in 0..=65535")?;
                config.bind.set_port(port);
            }
            _ => return Err(format!("unknown argument '{flag}'").into()),
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(Server::new(config)?.serve())?;
    Ok(())
}
