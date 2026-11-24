//! The one port. A Rayforce client opens with its wire version, a small
//! number; an HTTP client opens with a method, an upper-case letter. The
//! first byte tells them apart.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::core::Core;
use crate::{client, web};

const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections over HTTP let in past `max_client_conn`.
const HTTP_RESERVE: usize = 16;

pub async fn run(listener: TcpListener, core: Arc<Core>) {
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
            if is_http(&stream).await {
                web::serve(stream, core).await;
            } else if counted.0.load(Relaxed) > limit {
                core.refused.fetch_add(1, Relaxed);
                tracing::warn!(%peer, "max_client_conn reached; connection refused");
            } else {
                client::serve(stream, peer, core).await;
            }
        });
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

async fn is_http(stream: &TcpStream) -> bool {
    let mut first = [0u8; 1];
    matches!(timeout(FIRST_BYTE_TIMEOUT, stream.peek(&mut first)).await, Ok(Ok(1)) if first[0].is_ascii_uppercase())
}
