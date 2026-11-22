//! A stand-in replica that misbehaves on purpose: it passes the greeting,
//! reads one request, then does whatever the test tells it to.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rfbouncer::wire::frame::{self, HEADER_LEN, MSG_RESPONSE};

pub struct Fake {
    pub port: u16,
    /// Connections accepted so far.
    pub accepted: Arc<AtomicUsize>,
}

impl Fake {
    /// `behave` gets each greeted connection and its number, counted from 0.
    pub fn start(behave: impl Fn(TcpStream, usize) + Send + Sync + 'static) -> Self {
        let port = super::free_port();
        let listener = super::listen(port).unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let (count, behave) = (accepted.clone(), Arc::new(behave));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let (n, behave) = (count.fetch_add(1, Ordering::SeqCst), behave.clone());
                std::thread::spawn(move || {
                    let mut hello = [0u8; 2];
                    // Closed for certain when `behave` is done, even if a
                    // process started meanwhile inherited the descriptor.
                    let closer = stream.try_clone();
                    if stream.read_exact(&mut hello).is_ok() && stream.write_all(&[4, 0]).is_ok() {
                        behave(stream, n);
                    }
                    if let Ok(closer) = closer {
                        let _ = closer.shutdown(std::net::Shutdown::Both);
                    }
                });
            }
        });
        Self { port, accepted }
    }

    pub fn addr(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

/// Reads one request frame off `stream`; false when the peer closed instead.
pub fn read_request(stream: &mut TcpStream) -> bool {
    let mut header = [0u8; HEADER_LEN];
    if stream.read_exact(&mut header).is_err() {
        return false;
    }
    let size = u64::from_le_bytes(header[8..].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; size];
    stream.read_exact(&mut payload).is_ok()
}

/// Answers every request with the i64 `n`, until the peer closes.
pub fn answer_forever(mut stream: TcpStream, n: i64) {
    while read_request(&mut stream) {
        let _ = stream.write_all(&frame::frame(4, MSG_RESPONSE, &super::int(n)));
    }
}
