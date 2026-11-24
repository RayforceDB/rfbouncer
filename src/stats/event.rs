//! What the bouncer knows about one request once it is over.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// The replica evaluated the request and raised; this is its error code.
    ServerError(String),
    Cancelled,
    Timeout,
    /// Turned away by the bouncer: the queue was full or the wait ran out.
    Busy,
    /// No replica could answer.
    Failed,
    /// Answered from the pool's result cache: no replica was asked.
    Cached,
}

impl Outcome {
    pub fn label(&self) -> &str {
        match self {
            Self::Ok => "ok",
            Self::ServerError(code) => code,
            Self::Cancelled => "cancel",
            Self::Timeout => "timeout",
            Self::Busy => "busy",
            Self::Failed => "io",
            Self::Cached => "cached",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Event {
    pub at: SystemTime,
    pub pool: Arc<str>,
    pub backend: Option<Arc<str>>,
    pub client: IpAddr,
    pub user: Arc<str>,
    /// In the bouncer's queue, before a replica took it.
    pub wait: Duration,
    /// From handing it to a replica to the last byte of the answer.
    pub exec: Duration,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub retries: u8,
    pub outcome: Outcome,
    /// The request frame, header included.
    pub frame: Bytes,
}
