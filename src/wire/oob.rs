//! The cancel byte. A server evaluates while it is not reading its sockets,
//! so a client cancels out of band: one byte of TCP urgent data
//! (`ipc_sigurg_handler` in rayforce's `src/core/ipc.c`).

use std::io;
#[cfg(not(target_os = "linux"))]
use std::time::Duration;

use socket2::SockRef;
use tokio::net::TcpStream;

/// How often a waiting connection is asked for a pending urgent byte.
#[cfg(not(target_os = "linux"))]
const POLL: Duration = Duration::from_millis(20);

pub fn send(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).send_out_of_band(b"!").map(drop)
}

/// Takes a pending urgent byte off the socket. No byte and an error read the
/// same: nothing to act on.
pub fn take(stream: &TcpStream) -> bool {
    let mut byte = [std::mem::MaybeUninit::<u8>::uninit()];
    matches!(SockRef::from(stream).recv_out_of_band(&mut byte), Ok(1))
}

/// Resolves once the peer has sent an urgent byte.
///
/// On Linux the socket says when one has arrived, so a waiting client costs
/// nothing until then.
#[cfg(target_os = "linux")]
pub async fn wait(stream: &TcpStream) {
    use tokio::io::Interest;
    loop {
        match stream.ready(Interest::PRIORITY).await {
            Ok(ready) if ready.is_priority() => {}
            // Closed or broken: no cancel will come this way, and whoever
            // reads the stream is the one to notice.
            _ => return std::future::pending().await,
        }
        // Taking the byte through `try_io` is what tells the runtime the
        // readiness has been used when there was no byte after all.
        let taken = stream.try_io(Interest::PRIORITY, || {
            if take(stream) {
                Ok(())
            } else {
                Err(io::ErrorKind::WouldBlock.into())
            }
        });
        if taken.is_ok() {
            return;
        }
    }
}

/// Resolves once the peer has sent an urgent byte.
// shortcut: elsewhere than Linux the socket is asked every 20 ms, so a cancel
// is up to 20 ms late and each waiting client costs 50 syscalls a second;
// use the platform's own readiness where that starts to matter.
#[cfg(not(target_os = "linux"))]
pub async fn wait(stream: &TcpStream) {
    let mut tick = tokio::time::interval(POLL);
    loop {
        tick.tick().await;
        if take(stream) {
            return;
        }
    }
}
