//! Statistics of the requests that passed through: collected off the request
//! path, kept in SQLite, read back by the report pages.

pub mod event;

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::SyncSender;

use event::Event;

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
    pub dropped: AtomicU64,
}

impl Stats {
    pub fn record(&self, event: Event) {
        if let Some(tx) = &self.tx
            && tx.try_send(Msg::Event(Box::new(event))).is_err()
        {
            self.dropped.fetch_add(1, Relaxed);
        }
    }
}
