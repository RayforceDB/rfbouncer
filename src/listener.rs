//! The one port. A Rayforce client opens with its wire version, a small
//! number; an HTTP client opens with a method, an upper-case letter. The
//! first byte tells them apart. A TLS client opens with a handshake record,
//! 0x16, and says which of the two it is inside the tunnel.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use socket2::SockRef;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::time::timeout;
use tokio_rustls::TlsAcceptor;

use crate::core::{Core, Pending};
use crate::{client, web};

const FIRST_BYTE_TIMEOUT: Duration = Duration::from_secs(10);
/// Connections over HTTP let in past `max_client_conn`.
const HTTP_RESERVE: usize = 16;
/// Connections the system holds for us between two accepts.
const BACKLOG: u32 = 1024;

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

/// Opens the port. With `reuse_port` another process may open it too, which
/// is how a new bouncer takes over from one that is still serving.
pub fn bind(addr: SocketAddr, reuse_port: bool) -> std::io::Result<TcpListener> {
    let socket = if addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    if reuse_port {
        socket.set_reuseport(true)?;
    }
    socket.bind(addr)?;
    socket.listen(BACKLOG)
}

/// A connection that is already waiting on the port, if there is one. It
/// asks the system itself, not the runtime, which may not have heard yet.
fn waiting(listener: &TcpListener) -> Option<(TcpStream, SocketAddr)> {
    let (socket, peer) = SockRef::from(listener).accept().ok()?;
    socket.set_nonblocking(true).ok()?;
    Some((TcpStream::from_std(socket.into()).ok()?, peer.as_socket()?))
}

/// Returns once told to stop, and the port is closed by then: the listener
/// is owned here and dropped on the way out, so the system hands this
/// process no connection it will not serve.
pub async fn run(listener: TcpListener, scope: Scope, core: Arc<Core>) {
    let open = Arc::new(AtomicUsize::new(0));
    let mut shutdown = core.shutdown.subscribe();
    let mut stopping = false;
    loop {
        let accepted = if stopping {
            // Whoever finished connecting before the stop is still served:
            // closing the port with them on it would reset them.
            match waiting(&listener) {
                Some(pair) => Ok(pair),
                None => return,
            }
        } else {
            tokio::select! {
                accepted = listener.accept() => accepted,
                _ = shutdown.wait_for(|stop| *stop) => {
                    stopping = true;
                    continue;
                }
            }
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
        let pending = core.pending();
        let core = core.clone();
        tokio::spawn(async move {
            // Held to the end of the task however it ends, a panic included:
            // a place that leaked would never be given back.
            let counted = counted;
            // Held through a TLS handshake as well: a stop waits for a client
            // that is half way into its tunnel.
            let pending = pending;
            let _ = stream.set_nodelay(true);
            let over_limit = counted.0.load(Relaxed) > limit;
            let first = first_byte(&stream).await;
            if first != Some(TLS_HANDSHAKE) {
                return route(stream, first, false, peer, scope, over_limit, core, pending).await;
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
            route(stream, first, true, peer, scope, over_limit, core, pending).await;
        });
    }
}

/// Hands a connection to what its first byte asks for. `secure` is whether
/// it came through TLS.
// One more than the lint likes: each is a fact about the connection that
// its caller has just found out, and a struct for a single call adds nothing.
#[allow(clippy::too_many_arguments)]
async fn route<S: client::Transport + 'static>(
    stream: S,
    first: Option<u8>,
    secure: bool,
    peer: std::net::SocketAddr,
    scope: Scope,
    over_limit: bool,
    core: Arc<Core>,
    pending: Pending,
) {
    if first.is_some_and(|b| b.is_ascii_uppercase()) {
        drop(pending);
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
        client::serve(stream, peer, scope, core, pending).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::stats::Stats;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn connections_already_on_the_port_are_taken_before_it_is_closed() {
        let text = "listen = \"127.0.0.1:0\"\n[[pool]]\nname = \"p\"\nbackends = [\"127.0.0.1:1\"]";
        let config: Config = toml::from_str(text).unwrap();
        let listener = bind(config.listen, false).unwrap();
        let addr = listener.local_addr().unwrap();
        let core = Core::new("unused".into(), config, Arc::new(Stats::default()));
        // Connected and not yet accepted when the stop comes.
        let mut clients = Vec::new();
        for _ in 0..20 {
            let mut client = TcpStream::connect(addr).await.unwrap();
            client.write_all(&[4, 0]).await.unwrap();
            clients.push(client);
        }
        core.shutdown.send_replace(true);
        run(listener, Scope::All, core).await;
        assert!(TcpStream::connect(addr).await.is_err());
        for mut client in clients {
            let mut hello = [0u8; 2];
            let greeted = timeout(Duration::from_secs(10), client.read_exact(&mut hello)).await;
            assert!(matches!(greeted, Ok(Ok(_))), "{greeted:?}");
            assert_eq!(hello, [4, 0]);
        }
    }
}
