//! The statistics thread and its database. One thread owns the write
//! connection; it holds the minute being filled in memory and writes it out
//! whole every few seconds, so a row is always replaced, never merged in SQL.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, params};

use super::Msg;
use super::event::{Event, Outcome};
use super::normalize::normalize;
use crate::config::StatsConfig;
use crate::pool::request_text;

const FLUSH_EVERY: Duration = Duration::from_secs(10);
const PRUNE_EVERY: Duration = Duration::from_secs(3600);
pub const BUCKETS: usize = 32;
/// The text kept of a query, normalized or sampled.
const TEXT_MAX: usize = 4096;
/// The slowest requests kept per pool and hour.
const SLOW_PER_HOUR: usize = 20;
/// Failed requests kept per pool and minute; more of one minute's failures
/// say nothing the first ones did not.
const ERRORS_PER_MINUTE: usize = 20;
/// Where requests past `max_fingerprints_per_minute` are counted.
const OTHER: (u64, &str) = (0, "<other queries>");

const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS fingerprints(
        fp INTEGER PRIMARY KEY, text TEXT NOT NULL, first_seen INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS agg(
        minute INTEGER NOT NULL, pool TEXT NOT NULL, fp INTEGER NOT NULL,
        count INTEGER NOT NULL, errors INTEGER NOT NULL, cancels INTEGER NOT NULL,
        timeouts INTEGER NOT NULL, refused INTEGER NOT NULL, failed INTEGER NOT NULL,
        retries INTEGER NOT NULL, dur_sum INTEGER NOT NULL, dur_max INTEGER NOT NULL,
        wait_sum INTEGER NOT NULL, wait_max INTEGER NOT NULL,
        bytes_in INTEGER NOT NULL, bytes_out INTEGER NOT NULL, hist BLOB NOT NULL,
        PRIMARY KEY(minute, pool, fp)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS agg_backend(
        minute INTEGER NOT NULL, pool TEXT NOT NULL, backend TEXT NOT NULL,
        count INTEGER NOT NULL, errors INTEGER NOT NULL, dur_sum INTEGER NOT NULL,
        PRIMARY KEY(minute, pool, backend)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS agg_client(
        minute INTEGER NOT NULL, pool TEXT NOT NULL, client TEXT NOT NULL,
        count INTEGER NOT NULL, errors INTEGER NOT NULL, dur_sum INTEGER NOT NULL,
        PRIMARY KEY(minute, pool, client)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS samples(
        id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, pool TEXT NOT NULL, fp INTEGER NOT NULL,
        kind TEXT NOT NULL, dur INTEGER NOT NULL, wait INTEGER NOT NULL,
        backend TEXT NOT NULL, client TEXT NOT NULL, status TEXT NOT NULL, text TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS samples_by ON samples(pool, kind, ts);
";

/// Durations fall into buckets by powers of two of a microsecond: bucket `i`
/// holds those below `2^i` µs, and the last one everything longer.
pub fn bucket(micros: u64) -> usize {
    ((u64::BITS - micros.leading_zeros()) as usize).min(BUCKETS - 1)
}

/// The duration, in microseconds, below which `q` of the histogram's
/// requests fall: the upper edge of the bucket that holds that request.
pub fn quantile(hist: &[u64; BUCKETS], q: f64) -> u64 {
    let total: u64 = hist.iter().sum();
    if total == 0 {
        return 0;
    }
    let rank = (q * total as f64).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (i, n) in hist.iter().enumerate() {
        seen += n;
        if seen >= rank {
            return 1 << i;
        }
    }
    1 << (BUCKETS - 1)
}

pub fn hist_from_blob(blob: &[u8]) -> [u64; BUCKETS] {
    let mut hist = [0u64; BUCKETS];
    for (slot, raw) in hist.iter_mut().zip(blob.as_chunks::<4>().0) {
        *slot = u64::from(u32::from_le_bytes(*raw));
    }
    hist
}

fn hist_to_blob(hist: &[u64; BUCKETS]) -> Vec<u8> {
    hist.iter()
        .flat_map(|n| u32::try_from(*n).unwrap_or(u32::MAX).to_le_bytes())
        .collect()
}

#[derive(Default, Clone)]
struct Agg {
    count: u64,
    errors: u64,
    cancels: u64,
    timeouts: u64,
    refused: u64,
    failed: u64,
    retries: u64,
    dur_sum: u64,
    dur_max: u64,
    wait_sum: u64,
    wait_max: u64,
    bytes_in: u64,
    bytes_out: u64,
    hist: [u64; BUCKETS],
}

#[derive(Default, Clone)]
struct Small {
    count: u64,
    errors: u64,
    dur_sum: u64,
}

struct Writer {
    db: Connection,
    store_samples: bool,
    max_fingerprints: usize,
    retention_aggregates: Duration,
    retention_samples: Duration,
    /// The minute held in memory. Everything below is of this minute.
    minute: i64,
    agg: HashMap<(String, u64), Agg>,
    backends: HashMap<(String, String), Small>,
    clients: HashMap<(String, String), Small>,
    /// Fingerprints already in the database, so each text is written once.
    known: HashSet<u64>,
    new_texts: Vec<(u64, String, i64)>,
    samples: Vec<Sample>,
    errors_this_minute: HashMap<String, usize>,
    /// The hour the slowest requests are being collected for, and the
    /// durations kept so far per pool.
    hour: i64,
    slowest: HashMap<String, Vec<u64>>,
    dirty: bool,
}

struct Sample {
    ts_ms: i64,
    pool: String,
    fp: u64,
    kind: &'static str,
    dur: u64,
    wait: u64,
    backend: String,
    client: String,
    status: String,
    text: String,
}

fn unix(at: SystemTime) -> Duration {
    at.duration_since(UNIX_EPOCH).unwrap_or_default()
}

fn clip(mut text: String) -> String {
    if text.len() > TEXT_MAX {
        let mut end = TEXT_MAX;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push('…');
    }
    text
}

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let db = Connection::open(path)?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "NORMAL")?;
    db.execute_batch(SCHEMA)?;
    Ok(db)
}

/// The statistics thread's body: runs until every sender is gone.
pub fn run(db: Connection, cfg: &StatsConfig, rx: &Receiver<Msg>) {
    let mut w = Writer {
        db,
        store_samples: cfg.store_samples,
        max_fingerprints: cfg.max_fingerprints_per_minute,
        retention_aggregates: cfg.retention_aggregates,
        retention_samples: cfg.retention_samples,
        minute: -1,
        agg: HashMap::new(),
        backends: HashMap::new(),
        clients: HashMap::new(),
        known: HashSet::new(),
        new_texts: Vec::new(),
        samples: Vec::new(),
        errors_this_minute: HashMap::new(),
        hour: -1,
        slowest: HashMap::new(),
        dirty: false,
    };
    if let Err(e) = w.load_known() {
        tracing::error!(error = %e, "statistics: cannot read the database");
    }
    let mut next_flush = Instant::now() + FLUSH_EVERY;
    let mut next_prune = Instant::now();
    loop {
        match rx.recv_timeout(next_flush.saturating_duration_since(Instant::now())) {
            Ok(Msg::Event(event)) => w.add(*event),
            Ok(Msg::Flush(done)) => {
                w.flush();
                let _ = done.send(());
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                w.flush();
                return;
            }
        }
        if Instant::now() >= next_flush {
            w.flush();
            next_flush = Instant::now() + FLUSH_EVERY;
            if Instant::now() >= next_prune {
                w.prune();
                next_prune = Instant::now() + PRUNE_EVERY;
            }
        }
    }
}

impl Writer {
    fn load_known(&mut self) -> rusqlite::Result<()> {
        let mut query = self.db.prepare("SELECT fp FROM fingerprints")?;
        let rows = query.query_map([], |row| row.get::<_, i64>(0))?;
        self.known = rows.filter_map(Result::ok).map(|fp| fp as u64).collect();
        Ok(())
    }

    /// Moves on to `minute`: what is held is written out, and whatever the
    /// database already has of the new minute, from a process before this
    /// one, is taken up so it is added to and not replaced.
    fn roll(&mut self, minute: i64) {
        self.flush();
        self.agg.clear();
        self.backends.clear();
        self.clients.clear();
        self.errors_this_minute.clear();
        self.minute = minute;
        if let Err(e) = self.load_minute() {
            tracing::error!(error = %e, "statistics: cannot read the database");
        }
        if minute / 60 != self.hour {
            self.hour = minute / 60;
            self.slowest.clear();
        }
    }

    fn load_minute(&mut self) -> rusqlite::Result<()> {
        let mut query = self.db.prepare(
            "SELECT pool, fp, count, errors, cancels, timeouts, refused, failed, retries, dur_sum, dur_max,
                    wait_sum, wait_max, bytes_in, bytes_out, hist FROM agg WHERE minute = ?",
        )?;
        let u = |row: &rusqlite::Row<'_>, i: usize| row.get::<_, i64>(i).map(|n| n as u64);
        let rows = query.query_map([self.minute], |row| {
            let agg = Agg {
                count: u(row, 2)?,
                errors: u(row, 3)?,
                cancels: u(row, 4)?,
                timeouts: u(row, 5)?,
                refused: u(row, 6)?,
                failed: u(row, 7)?,
                retries: u(row, 8)?,
                dur_sum: u(row, 9)?,
                dur_max: u(row, 10)?,
                wait_sum: u(row, 11)?,
                wait_max: u(row, 12)?,
                bytes_in: u(row, 13)?,
                bytes_out: u(row, 14)?,
                hist: hist_from_blob(&row.get::<_, Vec<u8>>(15)?),
            };
            Ok(((row.get::<_, String>(0)?, u(row, 1)?), agg))
        })?;
        self.agg = rows.filter_map(Result::ok).collect();
        for (table, column, into) in [
            ("agg_backend", "backend", &mut self.backends),
            ("agg_client", "client", &mut self.clients),
        ] {
            let sql = format!(
                "SELECT pool, {column}, count, errors, dur_sum FROM {table} WHERE minute = ?"
            );
            let mut query = self.db.prepare(&sql)?;
            let rows = query.query_map([self.minute], |row| {
                let small = Small {
                    count: u(row, 2)?,
                    errors: u(row, 3)?,
                    dur_sum: u(row, 4)?,
                };
                Ok(((row.get::<_, String>(0)?, row.get::<_, String>(1)?), small))
            })?;
            *into = rows.filter_map(Result::ok).collect();
        }
        Ok(())
    }

    fn add(&mut self, e: Event) {
        // A request is counted in the minute it ended, which is now.
        let now = unix(SystemTime::now());
        let minute = (now.as_secs() / 60) as i64;
        if minute != self.minute {
            self.roll(minute);
        }
        let pool = e.pool.to_string();
        let text = request_text(&e.frame);
        let (normal, mut fp) = normalize(&text);
        let key = (pool.clone(), fp);
        if !self.agg.contains_key(&key) && self.agg.len() >= self.max_fingerprints {
            fp = OTHER.0;
        }
        if self.known.insert(fp) {
            let stored = if fp == OTHER.0 {
                OTHER.1.to_owned()
            } else {
                clip(normal)
            };
            self.new_texts.push((fp, stored, now.as_secs() as i64));
        }
        let (dur, wait) = (e.exec.as_micros() as u64, e.wait.as_micros() as u64);
        let failed = !matches!(e.outcome, Outcome::Ok);

        let a = self.agg.entry((pool.clone(), fp)).or_default();
        a.count += 1;
        match e.outcome {
            Outcome::Ok => {}
            Outcome::ServerError(_) => a.errors += 1,
            Outcome::Cancelled => a.cancels += 1,
            Outcome::Timeout => a.timeouts += 1,
            Outcome::Busy => a.refused += 1,
            Outcome::Failed => a.failed += 1,
        }
        a.retries += u64::from(e.retries);
        a.dur_sum += dur;
        a.dur_max = a.dur_max.max(dur);
        a.wait_sum += wait;
        a.wait_max = a.wait_max.max(wait);
        a.bytes_in += e.bytes_in;
        a.bytes_out += e.bytes_out;
        a.hist[bucket(dur)] += 1;

        let backend = e.backend.as_deref().unwrap_or_default().to_owned();
        let client = e.client.to_string();
        let groups = [
            (!backend.is_empty()).then(|| {
                self.backends
                    .entry((pool.clone(), backend.clone()))
                    .or_default()
            }),
            Some(
                self.clients
                    .entry((pool.clone(), client.clone()))
                    .or_default(),
            ),
        ];
        for small in groups.into_iter().flatten() {
            small.count += 1;
            small.errors += u64::from(failed);
            small.dur_sum += dur;
        }
        self.dirty = true;

        if !self.store_samples {
            return;
        }
        let kind = if failed {
            let seen = self.errors_this_minute.entry(pool.clone()).or_default();
            *seen += 1;
            (*seen <= ERRORS_PER_MINUTE).then_some("error")
        } else {
            // Kept when it is among the slowest of its hour so far.
            let kept = self.slowest.entry(pool.clone()).or_default();
            let slowest_kept =
                kept.len() < SLOW_PER_HOUR || kept.iter().min().is_some_and(|least| dur > *least);
            if slowest_kept {
                if kept.len() >= SLOW_PER_HOUR
                    && let Some(least) = kept
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, d)| **d)
                        .map(|(i, _)| i)
                {
                    kept.swap_remove(least);
                }
                kept.push(dur);
            }
            slowest_kept.then_some("slow")
        };
        if let Some(kind) = kind {
            self.samples.push(Sample {
                ts_ms: unix(e.at).as_millis() as i64,
                pool,
                fp,
                kind,
                dur,
                wait,
                backend,
                client,
                status: e.outcome.label().to_owned(),
                text: clip(text),
            });
        }
    }

    fn flush(&mut self) {
        if !self.dirty && self.new_texts.is_empty() && self.samples.is_empty() {
            return;
        }
        if let Err(e) = self.write() {
            tracing::error!(error = %e, "statistics: cannot write to the database");
        }
    }

    fn write(&mut self) -> rusqlite::Result<()> {
        let tx = self.db.transaction()?;
        {
            let mut text = tx.prepare_cached(
                "INSERT OR IGNORE INTO fingerprints(fp, text, first_seen) VALUES (?, ?, ?)",
            )?;
            for (fp, normal, at) in &self.new_texts {
                text.execute(params![*fp as i64, normal, at])?;
            }
            let mut agg = tx.prepare_cached(
                "INSERT OR REPLACE INTO agg VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for ((pool, fp), a) in &self.agg {
                agg.execute(params![
                    self.minute,
                    pool,
                    *fp as i64,
                    a.count as i64,
                    a.errors as i64,
                    a.cancels as i64,
                    a.timeouts as i64,
                    a.refused as i64,
                    a.failed as i64,
                    a.retries as i64,
                    a.dur_sum as i64,
                    a.dur_max as i64,
                    a.wait_sum as i64,
                    a.wait_max as i64,
                    a.bytes_in as i64,
                    a.bytes_out as i64,
                    hist_to_blob(&a.hist),
                ])?;
            }
            for (table, rows) in [
                ("agg_backend", &self.backends),
                ("agg_client", &self.clients),
            ] {
                let mut small = tx.prepare_cached(&format!(
                    "INSERT OR REPLACE INTO {table} VALUES (?, ?, ?, ?, ?, ?)"
                ))?;
                for ((pool, who), s) in rows {
                    small.execute(params![
                        self.minute,
                        pool,
                        who,
                        s.count as i64,
                        s.errors as i64,
                        s.dur_sum as i64
                    ])?;
                }
            }
            let mut sample = tx.prepare_cached(
                "INSERT INTO samples(ts, pool, fp, kind, dur, wait, backend, client, status, text)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for s in &self.samples {
                sample.execute(params![
                    s.ts_ms,
                    s.pool,
                    s.fp as i64,
                    s.kind,
                    s.dur as i64,
                    s.wait as i64,
                    s.backend,
                    s.client,
                    s.status,
                    s.text
                ])?;
            }
        }
        tx.commit()?;
        self.new_texts.clear();
        self.samples.clear();
        self.dirty = false;
        Ok(())
    }

    /// Drops what is past its retention, and of each past hour's slow
    /// samples all but the slowest: a request was kept when it was among the
    /// slowest so far, and later ones may have pushed it out.
    fn prune(&mut self) {
        let now = unix(SystemTime::now());
        let aggregates = (now.saturating_sub(self.retention_aggregates).as_secs() / 60) as i64;
        let samples = now.saturating_sub(self.retention_samples).as_millis() as i64;
        let result = self.db.execute_batch(&format!(
            "DELETE FROM agg WHERE minute < {aggregates};
             DELETE FROM agg_backend WHERE minute < {aggregates};
             DELETE FROM agg_client WHERE minute < {aggregates};
             DELETE FROM samples WHERE ts < {samples};
             DELETE FROM samples WHERE kind = 'slow' AND id IN (
                 SELECT id FROM (
                     SELECT id, row_number() OVER (PARTITION BY pool, ts / 3600000 ORDER BY dur DESC) AS place
                     FROM samples WHERE kind = 'slow')
                 WHERE place > {SLOW_PER_HOUR});
             DELETE FROM fingerprints WHERE fp NOT IN (SELECT fp FROM agg) AND fp NOT IN (SELECT fp FROM samples);"
        ));
        match result.and_then(|()| self.load_known()) {
            Ok(()) => {}
            Err(e) => tracing::error!(error = %e, "statistics: cannot prune the database"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_fall_into_buckets_by_powers_of_two() {
        assert_eq!(bucket(0), 0);
        assert_eq!(bucket(1), 1);
        assert_eq!(bucket(1000), 10);
        assert_eq!(bucket(u64::MAX), BUCKETS - 1);
        let mut hist = [0u64; BUCKETS];
        for micros in [100, 200, 300, 400, 50_000] {
            hist[bucket(micros)] += 1;
        }
        assert_eq!(quantile(&hist, 0.5), 512);
        assert_eq!(quantile(&hist, 0.99), 65_536);
        assert_eq!(quantile(&[0; BUCKETS], 0.5), 0);
        assert_eq!(hist_from_blob(&hist_to_blob(&hist)), hist);
    }
}
