//! rfbouncer: one port in front of a pool of read-only Rayforce replicas.

pub mod admin;
pub mod client;
pub mod config;
pub mod core;
pub mod listener;
pub mod lockout;
pub mod metrics;
pub mod pool;
pub mod stats;
pub mod web;
pub mod wire;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal::unix::{SignalKind, signal};

use crate::config::Config;
use crate::core::Core;
use crate::pool::Cmd;

/// How long the replicas are given to take a cancel sent on the way out.
const CANCEL_MOMENT: std::time::Duration = std::time::Duration::from_secs(1);

/// Runs until told to stop: SIGTERM or SIGINT, or the admin's `shutdown`.
pub async fn run(config_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(&config_path)?;
    let listener = listener::bind(config.listen, config.reuse_port)?;
    let stats = Arc::new(stats::Stats::start(&config.stats));
    let core = Core::new(config_path, config, stats);
    tracing::info!(listen = %listener.local_addr()?, "rfbouncer started");

    let mut hangup = signal(SignalKind::hangup())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut stop = core.shutdown.subscribe();
    // With an address of its own for administration, the clients' port
    // serves the clients alone.
    let admin = match core.config().admin_listen {
        Some(addr) => Some(listener::bind(addr, core.config().reuse_port)?),
        None => None,
    };
    let scope = if admin.is_some() {
        listener::Scope::Clients
    } else {
        listener::Scope::All
    };
    let serving = tokio::spawn(listener::run(listener, scope, core.clone()));
    let administering =
        admin.map(|l| tokio::spawn(listener::run(l, listener::Scope::Admin, core.clone())));
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
    if let Some(administering) = administering {
        let _ = administering.await;
    }
    let deadline = tokio::time::Instant::now() + core.config().shutdown_timeout;
    while (core.client_count() > 0 || core.pending_count() > 0)
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    if core.client_count() > 0 {
        // Out of time. A replica evaluating does not look at its sockets, so
        // closing ours would leave it computing an answer for nobody: it is
        // told to stop, and its answer to that is the client's answer.
        for pool in core.pools() {
            pool.send(Cmd::CancelRunning);
        }
        let moment = tokio::time::Instant::now() + CANCEL_MOMENT;
        while core.client_count() > 0 && tokio::time::Instant::now() < moment {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }
    core.stats.flush().await;
    tracing::info!(clients_left = core.client_count(), "rfbouncer stopped");
    Ok(())
}
