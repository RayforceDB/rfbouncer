//! The one port. A Rayforce client opens with its wire version, a small
//! number; an HTTP client opens with a method, an upper-case letter. The
//! first byte tells them apart. A TLS client opens with a handshake record,
//! 0x16, and says which of the two it is inside the tunnel.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::core::Core;
use crate::{client, web};

const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections over HTTP let in past `max_client_conn`.
const HTTP_RESERVE: usize = 16;

/// The first byte of a TLS connection: the record type of a handshake.
const TLS_HANDSHAKE: u8 = 0x16;

/// What a listener serves.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// Everything: there is one port.
    All,
    /// Pool clients and `/healthz`: administration has its own address.
    Clients,
    /// The admin console, the report pages, the metrics and `/healthz`.
    Admin,
}

pub async fn run(listener: TcpListener, scope: Scope, core: Arc<Core>) {
    let open = Arc::new(AtomicUsize::new(0));
    let mut shutdown = core.shutdown.subscribe();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = shutdown.wait_for(|stop| *stop) => return,
        };
        let (stream, peer) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                // Out of descriptors, most likely: wait rather than spin.
                tracing::warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        // The pages and the metrics are let in a little past the limit: the
        // moment every place is taken is the moment someone needs to look.
        let limit = core.config().max_client_conn;
        if open.load(Relaxed) >= limit + HTTP_RESERVE {
            core.refused.fetch_add(1, Relaxed);
            continue;
        }
        let counted = Counted::new(open.clone());
        let core = core.clone();
        tokio::spawn(async move {
            // Held to the end of the task however it ends, a panic included:
            // a place that leaked would never be given back.
            let counted = counted;
            let _ = stream.set_nodelay(true);
            let over_limit = counted.0.load(Relaxed) > limit;
            let first = first_byte(&stream).await;
            if first != Some(TLS_HANDSHAKE) {
                return route(stream, first, false, peer, scope, over_limit, core).await;
            }
            // A certificate is the configuration's, so the one in force is
            // asked for at each connection: a reload changes it for the next.
            let Some(tls) = core.config().tls.as_ref().and_then(|t| t.server.clone()) else {
                tracing::debug!(%peer, "TLS offered and none is configured; closing");
                return;
            };
            let accepting = TlsAcceptor::from(tls).accept(stream);
            let mut stream = match timeout(FIRST_BYTE_TIMEOUT, accepting).await {
                Ok(Ok(stream)) => BufReader::new(stream),
                Ok(Err(e)) => return tracing::debug!(%peer, error = %e, "TLS handshake failed"),
                Err(_) => return,
            };
            // A tunnel cannot be peeked into, so its first bytes are read
            // into a buffer and left there for whoever serves it.
            let first = match timeout(FIRST_BYTE_TIMEOUT, stream.fill_buf()).await {
                Ok(Ok([first, ..])) => Some(*first),
                _ => None,
            };
            route(stream, first, true, peer, scope, over_limit, core).await;
        });
    }
}

/// Hands a connection to what its first byte asks for. `secure` is whether
/// it came through TLS.
async fn route<S: client::Transport + 'static>(
    stream: S,
    first: Option<u8>,
    secure: bool,
    peer: std::net::SocketAddr,
    scope: Scope,
    over_limit: bool,
    core: Arc<Core>,
) {
    if first.is_some_and(|b| b.is_ascii_uppercase()) {
        return web::serve(stream, peer, scope, secure, core).await;
    }
    if over_limit {
        core.refused.fetch_add(1, Relaxed);
        tracing::warn!(%peer, "max_client_conn reached; connection refused");
    } else if !secure
        && core
            .config()
            .tls
            .as_ref()
            .is_some_and(|t| t.require_for_ipc)
    {
        // Closed before the greeting, so no password is sent in the clear.
        core.cleartext_refused.fetch_add(1, Relaxed);
        tracing::warn!(%peer, "clear-text IPC refused: [tls] require_for_ipc is set");
    } else {
        client::serve(stream, peer, scope, core).await;
    }
}

/// One open connection, counted for as long as this lives.
struct Counted(Arc<AtomicUsize>);

impl Counted {
    fn new(open: Arc<AtomicUsize>) -> Self {
        open.fetch_add(1, Relaxed);
        Self(open)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Relaxed);
    }
}

async fn first_byte(stream: &TcpStream) -> Option<u8> {
    let mut first = [0u8; 1];
    match timeout(FIRST_BYTE_TIMEOUT, stream.peek(&mut first)).await {
        Ok(Ok(1)) => Some(first[0]),
        _ => None,
    }
}
