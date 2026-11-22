//! Statistics of the requests that passed through: collected off the request
//! path, kept in SQLite, read back by the report pages.

pub mod event;
pub mod normalize;
pub mod store;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Duration;

use event::Event;

use crate::config::StatsConfig;

/// Events waiting for the statistics thread. Past this they are dropped and
/// counted: statistics never hold a request back.
pub const BACKLOG: usize = 16_384;

pub enum Msg {
    Event(Box<Event>),
    /// Write what is held in memory, then answer.
    Flush(SyncSender<()>),
}

/// Where a finished request is reported. Without a database it counts
/// nothing and costs nothing.
#[derive(Default)]
pub struct Stats {
    tx: Option<SyncSender<Msg>>,
    path: Option<PathBuf>,
    pub dropped: AtomicU64,
}

impl Stats {
    /// Opens the database and starts the thread that writes to it. With no
    /// path configured, statistics are off.
    pub fn start(cfg: &StatsConfig) -> Result<Self, String> {
        let Some(path) = cfg.path.clone() else {
            return Ok(Self::default());
        };
        let db = store::open(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        let (tx, rx) = sync_channel(BACKLOG);
        let cfg = cfg.clone();
        std::thread::Builder::new()
            .name("stats".into())
            .spawn(move || store::run(db, &cfg, &rx))
            .map_err(|e| format!("cannot start the statistics thread: {e}"))?;
        Ok(Self {
            tx: Some(tx),
            path: Some(path),
            dropped: AtomicU64::new(0),
        })
    }

    pub fn record(&self, event: Event) {
        if let Some(tx) = &self.tx
            && tx.try_send(Msg::Event(Box::new(event))).is_err()
        {
            self.dropped.fetch_add(1, Relaxed);
        }
    }

    /// Has everything reported so far written to the database. Blocks, for
    /// five seconds at most.
    pub fn flush(&self) {
        if let Some(tx) = &self.tx {
            let (done, wait) = sync_channel(1);
            if tx.send(Msg::Flush(done)).is_ok() {
                let _ = wait.recv_timeout(Duration::from_secs(5));
            }
        }
    }

    /// The database, for whoever reads the reports.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}
