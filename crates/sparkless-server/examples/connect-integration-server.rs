//! HTTP and Spark Connect listeners sharing one native `Server` instance.

#[cfg(feature = "connect")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use sparkless_server::{Server, ServerConfig};
    use std::net::SocketAddr;
    use tokio::sync::watch;

    let mut args = std::env::args().skip(1);
    let http_bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:18099".to_string())
        .parse()?;
    let connect_bind: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:15002".to_string())
        .parse()?;
    if args.next().is_some() {
        return Err("usage: connect-integration-server [HTTP_ADDR] [CONNECT_ADDR]".into());
    }

    let mut config = ServerConfig::from_env().map_err(std::io::Error::other)?;
    config.bind = http_bind;
    let server = Server::new(config)?;
    let (shutdown_tx, _) = watch::channel(false);

    let http_server = server.clone();
    let mut http_shutdown = shutdown_tx.subscribe();
    let mut http = tokio::spawn(async move {
        http_server
            .serve_until(async move {
                let _ = http_shutdown.changed().await;
            })
            .await
    });

    let connect_server = server.clone();
    let mut connect_shutdown = shutdown_tx.subscribe();
    let mut connect = tokio::spawn(async move {
        connect_server
            .serve_connect_until(connect_bind, async move {
                let _ = connect_shutdown.changed().await;
            })
            .await
    });

    let outcome = tokio::select! {
        signal = shutdown_signal() => {
            signal?;
            Ok(())
        }
        result = &mut http => {
            result??;
            Ok(())
        }
        result = &mut connect => {
            result??;
            Ok(())
        }
    };
    let _ = shutdown_tx.send(true);
    if !http.is_finished() {
        http.await??;
    }
    if !connect.is_finished() {
        connect.await??;
    }
    outcome
}

#[cfg(feature = "connect")]
async fn shutdown_signal() -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

#[cfg(not(feature = "connect"))]
fn main() {}
