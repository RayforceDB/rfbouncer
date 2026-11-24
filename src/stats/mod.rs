//! Statistics of the requests that passed through: collected off the request
//! path, kept in a Rayforce process of their own, read back by the report
//! pages.

pub mod event;
pub mod normalize;
pub mod rf;
pub mod store;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, oneshot};

use event::Event;
use rf::Conn;

use crate::config::StatsConfig;
use crate::wire::value::Value;

/// Events waiting for the statistics task. Past this they are dropped and
/// counted: statistics never hold a request back.
pub const BACKLOG: usize = 16_384;

pub enum Msg {
    Event(Box<Event>),
    /// Write what is held in memory, then answer.
    Flush(oneshot::Sender<()>),
}

/// Where a finished request is reported, and where the reports are read
/// from. Without a store it counts nothing and costs nothing.
#[derive(Default)]
pub struct Stats {
    tx: Option<mpsc::Sender<Msg>>,
    cfg: Option<StatsConfig>,
    /// The connection the reports are read over, opened when first needed.
    reader: Mutex<Option<Conn>>,
    pub dropped: AtomicU64,
}

impl Stats {
    /// Starts the task that writes to the store. With no store configured,
    /// statistics are off. Must be called inside the runtime.
    pub fn start(cfg: &StatsConfig) -> Self {
        if cfg.rayforce.is_none() {
            return Self::default();
        }
        let (tx, rx) = mpsc::channel(BACKLOG);
        tokio::spawn(store::run(cfg.clone(), rx));
        Self {
            tx: Some(tx),
            cfg: Some(cfg.clone()),
            ..Self::default()
        }
    }

    pub fn enabled(&self) -> bool {
        self.tx.is_some()
    }

    pub fn record(&self, event: Event) {
        if let Some(tx) = &self.tx
            && tx.try_send(Msg::Event(Box::new(event))).is_err()
        {
            self.dropped.fetch_add(1, Relaxed);
        }
    }

    /// The directory the store keeps days that are over in, if it has one.
    pub fn disk(&self) -> Option<&str> {
        self.cfg.as_ref()?.disk.as_deref()
    }

    /// The last day the store has on disk, in days since 1970.
    pub async fn disk_day(&self) -> Result<Option<i64>, String> {
        let Some(root) = self.disk() else {
            return Ok(None);
        };
        Ok(match self.query(&store::disk_day(root)).await? {
            Value::I64(day) if day >= 0 => Some(day),
            _ => None,
        })
    }

    /// Has everything reported so far written to the store, waiting five
    /// seconds at most.
    pub async fn flush(&self) {
        if let Some(tx) = &self.tx {
            let (done, wait) = oneshot::channel();
            if tx.send(Msg::Flush(done)).await.is_ok() {
                let _ = tokio::time::timeout(Duration::from_secs(5), wait).await;
            }
        }
    }

    /// Evaluates `source` in the store, for a report. One at a time: the
    /// store evaluates one message at a time anyway.
    pub async fn query(&self, source: &str) -> Result<Value, String> {
        let cfg = self.cfg.as_ref().ok_or("statistics are off")?;
        let mut reader = self.reader.lock().await;
        if reader.is_none() {
            let mut conn = Conn::open(cfg).await?;
            // A report asked for before any request has ended finds its tables.
            store::ensure_schema(&mut conn).await?;
            *reader = Some(conn);
        }
        let Some(conn) = reader.as_mut() else {
            return Err("no connection to the statistics store".into());
        };
        let answer = conn.eval(source).await;
        if answer.is_err() {
            // Whatever went wrong, the next report starts on a fresh connection.
            *reader = None;
        }
        answer
    }
}
