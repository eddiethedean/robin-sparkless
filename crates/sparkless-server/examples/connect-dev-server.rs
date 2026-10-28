//! Minimal listener used by `tests/connect/smoke_public_client.py` for local client checks.

#[cfg(feature = "connect")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:15002".to_string())
        .parse()?;
    let server = sparkless_server::Server::new(sparkless_server::ServerConfig::default())?;
    server
        .serve_connect_until(address, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[cfg(not(feature = "connect"))]
fn main() {}
