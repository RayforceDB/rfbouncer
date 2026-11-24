//! A stand-in replica that misbehaves on purpose: it passes the greeting,
//! reads one request, then does whatever the test tells it to.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rfbouncer::wire::frame::{self, HEADER_LEN, MSG_RESPONSE};
use rfbouncer::wire::status::{Frame, MSG_STATUS, State};

pub struct Fake {
    pub port: u16,
    /// Connections accepted so far.
    pub accepted: Arc<AtomicUsize>,
    /// While set, a new connection is accepted and not greeted, as by a
    /// server whose evaluating thread hangs.
    pub hung: Arc<AtomicBool>,
}

impl Fake {
    /// `behave` gets each greeted connection and its number, counted from 0.
    pub fn start(behave: impl Fn(TcpStream, usize) + Send + Sync + 'static) -> Self {
        Self::start_speaking(super::wire(), behave)
    }

    /// As `start`, greeting with another wire version.
    pub fn start_speaking(
        version: u8,
        behave: impl Fn(TcpStream, usize) + Send + Sync + 'static,
    ) -> Self {
        let port = super::free_port();
        let listener = super::listen(port).unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let (count, behave) = (accepted.clone(), Arc::new(behave));
        let hung = Arc::new(AtomicBool::new(false));
        let deaf = hung.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let (n, behave) = (count.fetch_add(1, Ordering::SeqCst), behave.clone());
                let deaf = deaf.clone();
                std::thread::spawn(move || {
                    while deaf.load(Ordering::SeqCst) {
                        if !matches!(stream.read(&mut [0u8; 64]), Ok(n) if n > 0) {
                            return;
                        }
                    }
                    let mut hello = [0u8; 2];
                    // Closed for certain when `behave` is done, even if a
                    // process started meanwhile inherited the descriptor.
                    let closer = stream.try_clone();
                    if stream.read_exact(&mut hello).is_ok()
                        && stream.write_all(&[version, 0]).is_ok()
                    {
                        behave(stream, n);
                    }
                    if let Ok(closer) = closer {
                        let _ = closer.shutdown(std::net::Shutdown::Both);
                    }
                });
            }
        });
        Self {
            port,
            accepted,
            hung,
        }
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
        let _ = stream.write_all(&frame::frame(super::wire(), MSG_RESPONSE, &super::int(n)));
    }
}

/// Reads one frame off `stream`: its type and payload. `None` when the peer
/// closed instead.
pub fn read_frame(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).ok()?;
    let size = u64::from_le_bytes(header[8..].try_into().unwrap()) as usize;
    let mut payload = vec![0u8; size];
    stream.read_exact(&mut payload).ok()?;
    Some((header[7], payload))
}

/// What a stand-in's status stream does at one tick.
pub enum Tick {
    /// Write a status frame with this payload.
    Frame(Vec<u8>),
    /// Write these bytes as they are.
    Raw(Vec<u8>),
    /// Write nothing and keep the connection.
    Quiet,
    Close,
}

/// Serves one connection as a replica with a status stream does. One that
/// opens with a status request is written what `next` says every `every`
/// and never read again; any other is answered `who`, each answer `delay`
/// late, until the peer closes.
pub fn replica(
    mut stream: TcpStream,
    who: i64,
    delay: Duration,
    every: Duration,
    next: impl Fn() -> Tick,
) {
    let Some((msgtype, _)) = read_frame(&mut stream) else {
        return;
    };
    if msgtype != MSG_STATUS {
        loop {
            std::thread::sleep(delay);
            let answer = frame::frame(super::wire(), MSG_RESPONSE, &super::int(who));
            if stream.write_all(&answer).is_err() || !read_request(&mut stream) {
                return;
            }
        }
    }
    replica_stream(stream, every, next);
}

/// The status stream of `replica`, on a connection whose status request has
/// been read.
pub fn replica_stream(mut stream: TcpStream, every: Duration, next: impl Fn() -> Tick) {
    stream.set_read_timeout(Some(every)).unwrap();
    loop {
        let bytes = match next() {
            Tick::Frame(payload) => frame::frame(super::wire(), MSG_STATUS, &payload).to_vec(),
            Tick::Raw(bytes) => bytes,
            Tick::Quiet => Vec::new(),
            Tick::Close => return,
        };
        if stream.write_all(&bytes).is_err() {
            return;
        }
        // The pause between two frames, cut short when the peer leaves.
        match stream.read(&mut [0u8; 1]) {
            Ok(0) => return,
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}

/// What a stand-in says of itself. Its counters grow with the clock, so
/// that any two frames are `util` of a loop and `util` cores apart.
pub struct Report {
    pub state: State,
    pub query: &'static str,
    pub util: f64,
    pub rss: u64,
    pub limit: u64,
}

impl Report {
    pub fn idle(util: f64) -> Self {
        Self {
            state: State::Idle,
            query: "",
            util,
            rss: 100 << 20,
            limit: 0,
        }
    }

    /// Running `query`, which nobody sent through the bouncer.
    pub fn busy(state: State, query: &'static str) -> Self {
        Self {
            state,
            query,
            ..Self::idle(0.5)
        }
    }

    pub fn payload(&self) -> Vec<u8> {
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        // Not from zero, so that the first frame's counters are not zero.
        let total = START.get_or_init(Instant::now).elapsed().as_nanos() as u64 + 1_000_000_000;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let busy = (self.util * total as f64) as u64;
        Frame {
            state: self.state,
            pid: std::process::id(),
            now,
            busy_since: if self.state == State::Idle {
                0
            } else {
                now - 5_000_000_000
            },
            loop_busy_ns: busy,
            loop_total_ns: total,
            cpu_user_ns: busy,
            cpu_sys_ns: 0,
            rss_bytes: self.rss,
            heap_bytes: self.rss / 2,
            mem_limit_bytes: self.limit,
            conns: 2,
            workers: 4,
            cores: 8,
            query: self.query.to_owned(),
        }
        .encode()
    }
}
