//! The cancel byte. A server evaluates while it is not reading its sockets,
//! so a client cancels out of band: one byte of TCP urgent data
//! (`ipc_sigurg_handler` in rayforce's `src/core/ipc.c`).

use std::io;
use std::time::Duration;

use socket2::SockRef;
use tokio::net::TcpStream;

/// How often a waiting connection is asked for a pending urgent byte.
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
// shortcut: polls every 20 ms on every platform, so a cancel is up to 20 ms
// late and each waiting client costs 50 syscalls a second; move to
// `Interest::PRIORITY` readiness on Linux if either shows in a profile.
pub async fn wait(stream: &TcpStream) {
    let mut tick = tokio::time::interval(POLL);
    loop {
        tick.tick().await;
        if take(stream) {
            return;
        }
    }
}
