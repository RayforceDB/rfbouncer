//! The cancel byte. A server evaluates while it is not reading its sockets,
//! so a client cancels out of band: one byte of TCP urgent data
//! (`ipc_sigurg_handler` in rayforce's `src/core/ipc.c`).

use std::io;

use socket2::SockRef;
use tokio::net::TcpStream;

pub fn send(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).send_out_of_band(b"!").map(drop)
}

/// Takes a pending urgent byte off the socket. No byte and an error read the
/// same: nothing to act on.
fn take(socket: SockRef<'_>) -> bool {
    let mut byte = [std::mem::MaybeUninit::<u8>::uninit()];
    matches!(socket.recv_out_of_band(&mut byte), Ok(1))
}

/// Waits for a client's cancel byte.
///
/// On Linux the kernel says when urgent data has arrived, but only to who
/// asked for that, and the runtime's own registration of a stream does not.
/// So a second handle to the same socket is registered for it alone. That is
/// one more descriptor for each client, and no work at all while it waits.
#[cfg(target_os = "linux")]
pub struct Watch(tokio::io::unix::AsyncFd<std::net::TcpStream>);

#[cfg(target_os = "linux")]
impl Watch {
    pub fn new(stream: &TcpStream) -> io::Result<Self> {
        use tokio::io::Interest;
        use tokio::io::unix::AsyncFd;
        let twin: std::net::TcpStream = SockRef::from(stream).try_clone()?.into();
        // Deprecated because a type could close its descriptor while it is
        // registered. This one cannot: the socket is owned here, by the
        // registration, and closed only with it. The constructors that
        // replace it are unsafe, which this crate forbids.
        #[allow(deprecated)]
        let registered = AsyncFd::with_interest(twin, Interest::PRIORITY)?;
        Ok(Self(registered))
    }

    /// Resolves once the peer has sent an urgent byte. `&mut` only because
    /// the other platforms' watch needs it.
    pub async fn wait(&mut self) {
        loop {
            let Ok(mut ready) = self.0.ready(tokio::io::Interest::PRIORITY).await else {
                return std::future::pending().await;
            };
            if !ready.ready().is_priority() {
                // Closed or broken: no cancel will come this way, and
                // whoever reads the stream is the one to notice.
                return std::future::pending().await;
            }
            if take(SockRef::from(self.0.get_ref())) {
                return;
            }
            ready.clear_ready();
        }
    }
}

/// Waits for a client's cancel byte.
// shortcut: elsewhere than Linux the socket is asked every 20 ms, so a cancel
// is up to 20 ms late and each waiting client costs 50 syscalls a second;
// use the platform's own readiness where that starts to matter.
#[cfg(not(target_os = "linux"))]
pub struct Watch {
    socket: std::net::TcpStream,
    /// When the socket is asked next. One timer for the connection, kept
    /// from one request to the next: a client whose requests follow each
    /// other within the interval then costs no timer and no syscall at all,
    /// where a timer made for every wait cost several of each a request.
    next: std::pin::Pin<Box<tokio::time::Sleep>>,
}

#[cfg(not(target_os = "linux"))]
const POLL: std::time::Duration = std::time::Duration::from_millis(20);

#[cfg(not(target_os = "linux"))]
impl Watch {
    pub fn new(stream: &TcpStream) -> io::Result<Self> {
        Ok(Self {
            socket: SockRef::from(stream).try_clone()?.into(),
            next: Box::pin(tokio::time::sleep(POLL)),
        })
    }

    /// Resolves once the peer has sent an urgent byte.
    pub async fn wait(&mut self) {
        loop {
            self.next.as_mut().await;
            self.next.as_mut().reset(tokio::time::Instant::now() + POLL);
            if take(SockRef::from(&self.socket)) {
                return;
            }
        }
    }
}
