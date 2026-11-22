//! rfbouncer: one port in front of a pool of read-only Rayforce replicas.

pub mod admin;
pub mod client;
pub mod config;
pub mod core;
pub mod listener;
pub mod metrics;
pub mod pool;
pub mod stats;
pub mod web;
pub mod wire;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

use crate::config::Config;
use crate::core::Core;

/// Runs until told to stop: SIGTERM or SIGINT, or the admin's `shutdown`.
pub async fn run(config_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(&config_path)?;
    let listener = TcpListener::bind(config.listen).await?;
    let stats = Arc::new(stats::Stats::start(&config.stats));
    let core = Core::new(config_path, config, stats);
    tracing::info!(listen = %listener.local_addr()?, "rfbouncer started");

    let mut hangup = signal(SignalKind::hangup())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut stop = core.shutdown.subscribe();
    let serving = tokio::spawn(listener::run(listener, core.clone()));
    loop {
        tokio::select! {
            _ = hangup.recv() => {
                if let Err(e) = core.reload() {
                    tracing::error!(error = %e, "reload failed; the old configuration stays");
                }
            }
            _ = terminate.recv() => break,
            _ = interrupt.recv() => break,
            _ = stop.wait_for(|stop| *stop) => break,
        }
    }

    // Stop taking connections, let the requests in flight end, then go.
    core.shutdown.send_replace(true);
    let _ = serving.await;
    let deadline = tokio::time::Instant::now() + core.config().shutdown_timeout;
    while core.client_count() > 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    core.stats.flush().await;
    tracing::info!(clients_left = core.client_count(), "rfbouncer stopped");
    Ok(())
}
