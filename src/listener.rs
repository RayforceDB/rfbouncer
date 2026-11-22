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
        if open.load(Relaxed) >= core.config().max_client_conn {
            tracing::warn!(%peer, "max_client_conn reached; connection refused");
            continue;
        }
        open.fetch_add(1, Relaxed);
        let (core, open) = (core.clone(), open.clone());
        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            if is_http(&stream).await {
                web::serve(stream, core).await;
            } else {
                client::serve(stream, peer, core).await;
            }
            open.fetch_sub(1, Relaxed);
        });
    }
}

async fn is_http(stream: &TcpStream) -> bool {
    let mut first = [0u8; 1];
    matches!(timeout(FIRST_BYTE_TIMEOUT, stream.peek(&mut first)).await, Ok(Ok(1)) if first[0].is_ascii_uppercase())
}
