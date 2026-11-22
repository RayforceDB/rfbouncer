//! A pool: equal read-only replicas behind one queue.

pub mod backend;
pub mod dispatcher;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::config::PoolConfig;
use crate::stats::Stats;
use crate::stats::event::{Event, Outcome};
use crate::wire::{frame, value};

/// One request on its way through the bouncer.
pub struct Job {
    pub id: u64,
    /// The whole request frame, forwarded as it came.
    pub frame: Bytes,
    pub reply: mpsc::Sender<Reply>,
    pub client: SocketAddr,
    pub user: Arc<str>,
    pub arrived: Instant,
    pub arrived_at: SystemTime,
    pub retries: u8,
}

/// What a client session writes to its socket, in order.
pub enum Reply {
    Data(Bytes),
    /// The answer is complete; the next request may be read.
    Done,
    /// The answer broke off after part of it was sent: close the connection.
    Abort,
}

impl Job {
    pub fn version(&self) -> u8 {
        self.frame[4]
    }

    pub fn text(&self) -> String {
        request_text(&self.frame)
    }

    /// Answers the client with an error of the bouncer's own.
    pub async fn refuse(&self, code: &str, msg: &str) {
        let payload = value::error(self.version(), code, msg);
        let _ = self
            .reply
            .send(Reply::Data(frame::frame(
                self.version(),
                frame::MSG_RESPONSE,
                &payload,
            )))
            .await;
        let _ = self.reply.send(Reply::Done).await;
    }
}

/// A request frame's source text, or a placeholder for one that is not text.
pub fn request_text(frame: &[u8]) -> String {
    let payload = frame.get(frame::HEADER_LEN..).unwrap_or_default();
    value::request_text(frame.get(5).copied().unwrap_or(0), payload)
        .unwrap_or_else(|| format!("<binary {} bytes>", payload.len()))
}

#[derive(Default)]
pub struct Counters {
    pub requests: AtomicU64,
    pub ok: AtomicU64,
    pub server_errors: AtomicU64,
    pub cancelled: AtomicU64,
    pub timeouts: AtomicU64,
    pub busy: AtomicU64,
    pub failed: AtomicU64,
    pub retries: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub wait_us: AtomicU64,
    pub exec_us: AtomicU64,
}

/// What a pool's tasks share: its name, its counters, the statistics sink.
pub struct Shared {
    pub name: Arc<str>,
    pub counters: Counters,
    pub stats: Arc<Stats>,
}

/// How one request ended, as whoever ended it saw it.
pub struct Ending {
    pub backend: Option<Arc<str>>,
    pub wait: Duration,
    pub exec: Duration,
    pub bytes_out: u64,
    pub outcome: Outcome,
}

impl Shared {
    pub fn report(&self, job: &Job, end: Ending) {
        let c = &self.counters;
        c.requests.fetch_add(1, Relaxed);
        match &end.outcome {
            Outcome::Ok => &c.ok,
            Outcome::ServerError(_) => &c.server_errors,
            Outcome::Cancelled => &c.cancelled,
            Outcome::Timeout => &c.timeouts,
            Outcome::Busy => &c.busy,
            Outcome::Failed => &c.failed,
        }
        .fetch_add(1, Relaxed);
        c.retries.fetch_add(u64::from(job.retries), Relaxed);
        c.bytes_in.fetch_add(job.frame.len() as u64, Relaxed);
        c.bytes_out.fetch_add(end.bytes_out, Relaxed);
        c.wait_us.fetch_add(end.wait.as_micros() as u64, Relaxed);
        c.exec_us.fetch_add(end.exec.as_micros() as u64, Relaxed);
        self.stats.record(Event {
            at: job.arrived_at,
            pool: self.name.clone(),
            backend: end.backend,
            client: job.client.ip(),
            user: job.user.clone(),
            wait: end.wait,
            exec: end.exec,
            bytes_in: job.frame.len() as u64,
            bytes_out: end.bytes_out,
            retries: job.retries,
            outcome: end.outcome,
            frame: job.frame.clone(),
        });
    }
}

pub enum Cmd {
    Submit(Job),
    /// The client of this job sent its cancel byte.
    Cancel(u64),
    Up(usize),
    Down(usize, String),
    /// The replica answered and is free again.
    Finished(usize),
    /// The replica failed before any of the answer reached the client.
    /// `sent` is false when the request never left the bouncer.
    Failed {
        idx: usize,
        job: Job,
        sent: bool,
        reason: String,
    },
    /// The replica's task has ended, after being told to stop.
    Gone(usize),
    Snapshot(oneshot::Sender<Snapshot>),
    Pause(bool),
    /// Stop (true) or resume (false) handing requests to one address.
    Drain(String, bool, oneshot::Sender<bool>),
    Reconnect,
    Reconfigure(Arc<PoolConfig>),
    /// The pool left the configuration: turn the queue away and end.
    Close,
}

#[derive(Clone)]
pub struct PoolHandle {
    pub shared: Arc<Shared>,
    tx: mpsc::UnboundedSender<Cmd>,
}

impl PoolHandle {
    /// False when the pool is gone, in which case the command was dropped.
    pub fn send(&self, cmd: Cmd) -> bool {
        self.tx.send(cmd).is_ok()
    }

    pub async fn snapshot(&self) -> Option<Snapshot> {
        let (tx, rx) = oneshot::channel();
        self.send(Cmd::Snapshot(tx));
        rx.await.ok()
    }
}

pub struct Snapshot {
    pub name: Arc<str>,
    pub paused: bool,
    pub queue: Vec<QueuedInfo>,
    pub backends: Vec<BackendInfo>,
}

pub struct QueuedInfo {
    pub client: SocketAddr,
    pub waited: Duration,
    pub text: String,
}

pub struct BackendInfo {
    pub addr: Arc<str>,
    /// `down`, `idle` or `busy`.
    pub state: &'static str,
    pub draining: bool,
    pub last_error: String,
    pub served: u64,
    pub failures: u64,
    /// The request it is running: who sent it, for how long, and its text.
    pub running: Option<(SocketAddr, Duration, String)>,
}
