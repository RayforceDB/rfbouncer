//! The task that writes the statistics, and the tables it writes them to.
//!
//! The statistics live in a Rayforce process of their own. This task holds
//! the minute being filled in memory and, every few seconds, upserts the
//! rows of it that changed: a row is keyed by its leading columns, so
//! writing it again replaces it. Nothing is ever merged on the server.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

use super::Msg;
use super::event::{Event, Outcome};
use super::normalize::normalize;
use super::rf::Conn;
use crate::config::StatsConfig;
use crate::pool::{binary_fingerprint, request_source, request_text};
use crate::wire::value::{self, Column, Value};

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

/// The counters of a row of `rfb_agg`, in column order, after its key
/// (minute, pool, fp, src) and before its histogram (h0..h31).
pub const COUNTERS: [&str; 13] = [
    "count",
    "errors",
    "cancels",
    "timeouts",
    "refused",
    "failed",
    "retries",
    "dur_sum",
    "dur_max",
    "wait_sum",
    "wait_max",
    "bytes_in",
    "bytes_out",
];

// Where each counter sits in `Agg::counters`, as `COUNTERS` orders them.
const COUNT: usize = 0;
const ERRORS: usize = 1;
const CANCELS: usize = 2;
const TIMEOUTS: usize = 3;
const REFUSED: usize = 4;
const FAILED: usize = 5;
const RETRIES: usize = 6;
const DUR_SUM: usize = 7;
const DUR_MAX: usize = 8;
const WAIT_SUM: usize = 9;
const WAIT_MAX: usize = 10;
const BYTES_IN: usize = 11;
const BYTES_OUT: usize = 12;

/// Every table, with the source that makes it empty. A table's key is its
/// leading columns, which is what `upsert` matches on. `src` names the
/// process that wrote a row and is part of every key: several bouncers, or
/// one bouncer before and after a restart, each write rows of their own,
/// and a report sums them.
fn schema() -> Vec<(&'static str, String)> {
    let int = "(as 'I64 [])";
    let sym = "(as 'SYMBOL [])";
    let make = |names: &[String], types: &[&str]| {
        format!("(table [{}] (list {}))", names.join(" "), types.join(" "))
    };
    let own = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();

    let mut agg = own(&["minute", "pool", "fp", "src"]);
    agg.extend(COUNTERS.iter().map(|c| (*c).to_owned()));
    agg.extend((0..BUCKETS).map(|i| format!("h{i}")));
    let mut agg_types = vec![int, sym, int, sym];
    agg_types.resize(agg.len(), int);

    let small = own(&["minute", "pool", "who", "src", "count", "errors", "dur_sum"]);
    let status = own(&["minute", "pool", "status", "src", "count"]);
    let sample = own(&[
        "kind", "pool", "bucket", "slot", "src", "ts", "fp", "dur", "wait", "backend", "client",
        "status", "text",
    ]);
    let sample_types = [
        sym, sym, int, int, sym, int, int, int, int, sym, sym, sym, "(list)",
    ];
    vec![
        ("rfb_agg", make(&agg, &agg_types)),
        (
            "rfb_backend",
            make(&small, &[int, sym, sym, sym, int, int, int]),
        ),
        (
            "rfb_client",
            make(&small, &[int, sym, sym, sym, int, int, int]),
        ),
        ("rfb_status", make(&status, &[int, sym, sym, sym, int])),
        (
            "rfb_fp",
            make(&own(&["fp", "first_seen", "text"]), &[int, int, "(list)"]),
        ),
        ("rfb_sample", make(&sample, &sample_types)),
    ]
}

/// Makes the tables that are not there yet, and refuses a store whose
/// tables are of another layout than this build writes.
pub async fn ensure_schema(conn: &mut Conn) -> Result<(), String> {
    for (name, empty) in schema() {
        if conn.eval(&format!("(count {name})")).await.is_err() {
            conn.eval(&format!("(set {name} {empty})")).await?;
        } else if name != "rfb_fp"
            && conn
                .eval(&format!("(count (at {name} 'src))"))
                .await
                .is_err()
        {
            return Err(format!(
                "{name} is of an older layout: start an empty store, or delete the rfb_ tables in this one"
            ));
        }
    }
    Ok(())
}

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

#[derive(Default, Clone)]
struct Agg {
    counters: [u64; COUNTERS.len()],
    hist: [u64; BUCKETS],
}

#[derive(Default, Clone)]
struct Small {
    count: u64,
    errors: u64,
    dur_sum: u64,
}

#[derive(Clone)]
struct Sample {
    kind: &'static str,
    pool: String,
    /// The hour of a slow sample, the minute of a failed one.
    bucket: i64,
    slot: usize,
    ts_ms: i64,
    fp: u64,
    dur: u64,
    wait: u64,
    backend: String,
    client: String,
    status: String,
    text: String,
}

type Key = (String, String);

struct Writer {
    cfg: StatsConfig,
    conn: Option<Conn>,
    /// What tells this process's rows from any other's.
    src: String,
    /// The minute held in memory. The maps below are of this minute.
    minute: i64,
    agg: HashMap<(String, u64), Agg>,
    backends: HashMap<Key, Small>,
    clients: HashMap<Key, Small>,
    statuses: HashMap<Key, u64>,
    /// The normalized text of every fingerprint of this minute.
    texts: HashMap<u64, String>,
    /// Fingerprints whose text the store already has.
    known: HashSet<u64>,
    /// The hour the slowest requests are being collected for, and per pool
    /// the duration kept in each slot.
    hour: i64,
    slowest: HashMap<String, Vec<u64>>,
    errors_this_minute: HashMap<String, usize>,
    /// Samples not written yet.
    samples: Vec<Sample>,
    /// What changed since the last write. Only those rows are written: one
    /// that did not change would only make the store's journal longer.
    dirty_agg: HashSet<(String, u64)>,
    dirty_backends: HashSet<Key>,
    dirty_clients: HashSet<Key>,
    dirty_statuses: HashSet<Key>,
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

fn ints(values: impl Iterator<Item = u64>) -> Column {
    Column::I64(values.map(|v| v as i64).collect())
}

/// The cells of a table read back, by column name.
pub struct Rows(Vec<(String, Column)>);

impl Rows {
    pub fn of(value: Value) -> Result<Self, String> {
        match value {
            Value::Table(columns) => Ok(Self(columns)),
            _ => Err("the statistics store answered with something that is not a table".into()),
        }
    }

    pub fn len(&self) -> usize {
        self.0.first().map_or(0, |(_, column)| match column {
            Column::I64(v) => v.len(),
            Column::F64(v) => v.len(),
            Column::Sym(v) | Column::Str(v) => v.len(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn column(&self, name: &str) -> Option<&Column> {
        self.0.iter().find(|(n, _)| n == name).map(|(_, c)| c)
    }

    /// An integer cell; zero where the column is missing or of another type.
    pub fn int(&self, name: &str, row: usize) -> u64 {
        match self.column(name) {
            Some(Column::I64(v)) => v.get(row).copied().unwrap_or(0) as u64,
            Some(Column::F64(v)) => v.get(row).copied().unwrap_or(0.0) as u64,
            _ => 0,
        }
    }

    pub fn text(&self, name: &str, row: usize) -> String {
        match self.column(name) {
            Some(Column::Sym(v) | Column::Str(v)) => v.get(row).cloned().unwrap_or_default(),
            _ => String::new(),
        }
    }
}

/// The statistics task's body: runs until every sender is gone.
pub async fn run(cfg: StatsConfig, mut rx: Receiver<Msg>) {
    let mut w = Writer {
        cfg,
        conn: None,
        src: format!(
            "{}-{}",
            std::process::id(),
            unix(SystemTime::now()).as_secs()
        ),
        minute: -1,
        agg: HashMap::new(),
        backends: HashMap::new(),
        clients: HashMap::new(),
        statuses: HashMap::new(),
        texts: HashMap::new(),
        known: HashSet::new(),
        hour: -1,
        slowest: HashMap::new(),
        errors_this_minute: HashMap::new(),
        samples: Vec::new(),
        dirty_agg: HashSet::new(),
        dirty_backends: HashSet::new(),
        dirty_clients: HashSet::new(),
        dirty_statuses: HashSet::new(),
    };
    let mut next_flush = Instant::now() + FLUSH_EVERY;
    let mut next_prune = Instant::now();
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(Msg::Event(event)) => w.add(*event).await,
                Some(Msg::Flush(done)) => {
                    w.flush().await;
                    let _ = done.send(());
                }
                None => {
                    w.flush().await;
                    return;
                }
            },
            () = tokio::time::sleep_until(next_flush) => {
                w.flush().await;
                next_flush = Instant::now() + FLUSH_EVERY;
                if Instant::now() >= next_prune {
                    w.prune().await;
                    next_prune = Instant::now() + PRUNE_EVERY;
                }
            }
        }
    }
}

impl Writer {
    /// The connection, opened if it is not: the tables are made, and which
    /// query texts the store has is asked again, since it may be another
    /// process than before.
    async fn conn(&mut self) -> Result<&mut Conn, String> {
        if self.conn.is_none() {
            let mut conn = Conn::open(&self.cfg).await?;
            ensure_schema(&mut conn).await?;
            let known = Rows::of(conn.eval("(select {from: rfb_fp fp: fp})").await?)?;
            self.known = (0..known.len()).map(|row| known.int("fp", row)).collect();
            self.conn = Some(conn);
        }
        self.conn.as_mut().ok_or_else(|| "no connection".to_owned())
    }

    fn dirty(&self) -> bool {
        !(self.dirty_agg.is_empty()
            && self.dirty_backends.is_empty()
            && self.dirty_clients.is_empty()
            && self.dirty_statuses.is_empty()
            && self.samples.is_empty())
    }

    fn written(&mut self) {
        self.dirty_agg.clear();
        self.dirty_backends.clear();
        self.dirty_clients.clear();
        self.dirty_statuses.clear();
        self.samples.clear();
    }

    /// Moves on to `minute`: what is held of the one before is written out
    /// and let go.
    async fn roll(&mut self, minute: i64) {
        self.flush().await;
        if self.dirty() {
            tracing::error!(
                minute = self.minute,
                "statistics: a minute could not be written and is lost"
            );
            self.written();
        }
        self.agg.clear();
        self.backends.clear();
        self.clients.clear();
        self.statuses.clear();
        self.texts.clear();
        self.errors_this_minute.clear();
        self.minute = minute;
        if minute / 60 != self.hour {
            self.hour = minute / 60;
            self.slowest.clear();
        }
    }

    async fn add(&mut self, e: Event) {
        // A request is counted in the minute it ended, which is now.
        let now = unix(SystemTime::now());
        let minute = (now.as_secs() / 60) as i64;
        if minute != self.minute {
            self.roll(minute).await;
        }
        let pool = e.pool.to_string();
        // A request sent as text is known by its shape; one sent as a value
        // has no text to normalize and is known by its bytes.
        let (text, normal, mut fp) = match request_source(&e.frame) {
            Some(text) => {
                let (normal, fp) = normalize(&text);
                (text, normal, fp)
            }
            None => {
                let text = request_text(&e.frame);
                (text.clone(), text, binary_fingerprint(&e.frame))
            }
        };
        if !self.agg.contains_key(&(pool.clone(), fp))
            && self.agg.len() >= self.cfg.max_fingerprints_per_minute
        {
            fp = OTHER.0;
        }
        self.texts.entry(fp).or_insert_with(|| {
            if fp == OTHER.0 {
                OTHER.1.to_owned()
            } else {
                clip(normal)
            }
        });
        let (dur, wait) = (e.exec.as_micros() as u64, e.wait.as_micros() as u64);
        let failed = !matches!(e.outcome, Outcome::Ok);
        let backend = e.backend.as_deref().unwrap_or_default().to_owned();
        let client = e.client.to_string();
        let status = e.outcome.label().to_owned();

        let a = self.agg.entry((pool.clone(), fp)).or_default();
        let c = &mut a.counters;
        c[COUNT] += 1;
        match e.outcome {
            Outcome::Ok => {}
            Outcome::ServerError(_) => c[ERRORS] += 1,
            Outcome::Cancelled => c[CANCELS] += 1,
            Outcome::Timeout => c[TIMEOUTS] += 1,
            Outcome::Busy => c[REFUSED] += 1,
            Outcome::Failed => c[FAILED] += 1,
        }
        c[RETRIES] += u64::from(e.retries);
        c[DUR_SUM] += dur;
        c[DUR_MAX] = c[DUR_MAX].max(dur);
        c[WAIT_SUM] += wait;
        c[WAIT_MAX] = c[WAIT_MAX].max(wait);
        c[BYTES_IN] += e.bytes_in;
        c[BYTES_OUT] += e.bytes_out;
        a.hist[bucket(dur)] += 1;
        self.dirty_agg.insert((pool.clone(), fp));

        let count = |rows: &mut HashMap<Key, Small>, dirty: &mut HashSet<Key>, who: &str| {
            let key = (pool.clone(), who.to_owned());
            let small = rows.entry(key.clone()).or_default();
            small.count += 1;
            small.errors += u64::from(failed);
            small.dur_sum += dur;
            dirty.insert(key);
        };
        count(&mut self.clients, &mut self.dirty_clients, &client);
        if !backend.is_empty() {
            count(&mut self.backends, &mut self.dirty_backends, &backend);
        }
        let key = (pool.clone(), status.clone());
        *self.statuses.entry(key.clone()).or_default() += 1;
        self.dirty_statuses.insert(key);

        if !self.cfg.store_samples {
            return;
        }
        let place = if failed {
            let seen = self.errors_this_minute.entry(pool.clone()).or_default();
            *seen += 1;
            (*seen <= ERRORS_PER_MINUTE).then(|| ("error", minute, *seen - 1))
        } else {
            // Kept when it is among the slowest of its hour so far, in the
            // place of the least slow one once every place is taken.
            let kept = self.slowest.entry(pool.clone()).or_default();
            let slot = if kept.len() < SLOW_PER_HOUR {
                kept.push(0);
                Some(kept.len() - 1)
            } else {
                kept.iter()
                    .enumerate()
                    .min_by_key(|(_, d)| **d)
                    .filter(|(_, least)| dur > **least)
                    .map(|(i, _)| i)
            };
            slot.map(|slot| {
                kept[slot] = dur;
                ("slow", self.hour, slot)
            })
        };
        if let Some((kind, bucket, slot)) = place {
            self.samples.push(Sample {
                kind,
                pool,
                bucket,
                slot,
                ts_ms: unix(e.at).as_millis() as i64,
                fp,
                dur,
                wait,
                backend,
                client,
                status,
                text: clip(text),
            });
        }
    }

    async fn flush(&mut self) {
        if !self.dirty() {
            return;
        }
        match self.write().await {
            Ok(()) => self.written(),
            Err(e) => {
                // Kept in memory and tried again at the next flush.
                self.conn = None;
                tracing::error!(error = %e, "statistics: cannot write to the store");
            }
        }
    }

    async fn write(&mut self) -> Result<(), String> {
        let minute = self.minute as u64;
        self.conn().await?;
        let now = unix(SystemTime::now()).as_secs();
        let src = |n: usize| Column::Sym(vec![self.src.clone(); n]);

        let new: Vec<(u64, String)> = self
            .texts
            .iter()
            .filter(|(fp, _)| !self.known.contains(fp))
            .map(|(fp, text)| (*fp, text.clone()))
            .collect();
        let fps = (!new.is_empty()).then(|| {
            value::table(&[
                ("fp", ints(new.iter().map(|(fp, _)| *fp))),
                ("first_seen", ints(new.iter().map(|_| now))),
                (
                    "text",
                    Column::Str(new.iter().map(|(_, text)| text.clone()).collect()),
                ),
            ])
        });

        let rows: Vec<(&(String, u64), &Agg)> = self
            .dirty_agg
            .iter()
            .filter_map(|k| self.agg.get_key_value(k))
            .collect();
        let mut agg: Vec<(String, Column)> = vec![
            ("minute".into(), ints(rows.iter().map(|_| minute))),
            (
                "pool".into(),
                Column::Sym(rows.iter().map(|(k, _)| k.0.clone()).collect()),
            ),
            ("fp".into(), ints(rows.iter().map(|(k, _)| k.1))),
            ("src".into(), src(rows.len())),
        ];
        for (i, name) in COUNTERS.iter().enumerate() {
            agg.push((
                (*name).into(),
                ints(rows.iter().map(|(_, a)| a.counters[i])),
            ));
        }
        for i in 0..BUCKETS {
            agg.push((format!("h{i}"), ints(rows.iter().map(|(_, a)| a.hist[i]))));
        }
        let agg = (!rows.is_empty()).then(|| value::table(&agg));

        let small = |all: &HashMap<Key, Small>, dirty: &HashSet<Key>| {
            let rows: Vec<(&Key, &Small)> =
                dirty.iter().filter_map(|k| all.get_key_value(k)).collect();
            (!rows.is_empty()).then(|| {
                value::table(&[
                    ("minute", ints(rows.iter().map(|_| minute))),
                    (
                        "pool",
                        Column::Sym(rows.iter().map(|(k, _)| k.0.clone()).collect()),
                    ),
                    (
                        "who",
                        Column::Sym(rows.iter().map(|(k, _)| k.1.clone()).collect()),
                    ),
                    ("src", src(rows.len())),
                    ("count", ints(rows.iter().map(|(_, s)| s.count))),
                    ("errors", ints(rows.iter().map(|(_, s)| s.errors))),
                    ("dur_sum", ints(rows.iter().map(|(_, s)| s.dur_sum))),
                ])
            })
        };
        let backends = small(&self.backends, &self.dirty_backends);
        let clients = small(&self.clients, &self.dirty_clients);
        let statuses: Vec<(&Key, &u64)> = self
            .dirty_statuses
            .iter()
            .filter_map(|k| self.statuses.get_key_value(k))
            .collect();
        let statuses = (!statuses.is_empty()).then(|| {
            value::table(&[
                ("minute", ints(statuses.iter().map(|_| minute))),
                (
                    "pool",
                    Column::Sym(statuses.iter().map(|(k, _)| k.0.clone()).collect()),
                ),
                (
                    "status",
                    Column::Sym(statuses.iter().map(|(k, _)| k.1.clone()).collect()),
                ),
                ("src", src(statuses.len())),
                ("count", ints(statuses.iter().map(|(_, n)| **n))),
            ])
        });
        let s = &self.samples;
        let sym = |f: fn(&Sample) -> String| Column::Sym(s.iter().map(f).collect());
        let samples = (!s.is_empty()).then(|| {
            value::table(&[
                ("kind", sym(|x| x.kind.to_owned())),
                ("pool", sym(|x| x.pool.clone())),
                ("bucket", Column::I64(s.iter().map(|x| x.bucket).collect())),
                ("slot", ints(s.iter().map(|x| x.slot as u64))),
                ("src", src(s.len())),
                ("ts", Column::I64(s.iter().map(|x| x.ts_ms).collect())),
                ("fp", ints(s.iter().map(|x| x.fp))),
                ("dur", ints(s.iter().map(|x| x.dur))),
                ("wait", ints(s.iter().map(|x| x.wait))),
                ("backend", sym(|x| x.backend.clone())),
                ("client", sym(|x| x.client.clone())),
                ("status", sym(|x| x.status.clone())),
                (
                    "text",
                    Column::Str(s.iter().map(|x| x.text.clone()).collect()),
                ),
            ])
        });

        let writes = [
            ("rfb_fp", 1, fps),
            ("rfb_agg", 4, agg),
            ("rfb_backend", 4, backends),
            ("rfb_client", 4, clients),
            ("rfb_status", 4, statuses),
            ("rfb_sample", 5, samples),
        ];
        let conn = self.conn().await?;
        for (table, keys, rows) in writes {
            if let Some(rows) = rows {
                conn.call(&value::call_with_table("upsert", table, keys, &rows))
                    .await
                    .map_err(|e| format!("{table}: {e}"))?;
            }
        }
        self.known.extend(new.into_iter().map(|(fp, _)| fp));
        Ok(())
    }

    /// Drops what is past its retention, and the text of every query that
    /// no row refers to any more.
    async fn prune(&mut self) {
        let now = unix(SystemTime::now());
        let aggregates = now.saturating_sub(self.cfg.retention_aggregates).as_secs() / 60;
        let samples = now.saturating_sub(self.cfg.retention_samples).as_millis();
        let statements = [
            format!("(delete {{from: 'rfb_agg where: (< minute {aggregates})}})"),
            format!("(delete {{from: 'rfb_backend where: (< minute {aggregates})}})"),
            format!("(delete {{from: 'rfb_client where: (< minute {aggregates})}})"),
            format!("(delete {{from: 'rfb_status where: (< minute {aggregates})}})"),
            format!("(delete {{from: 'rfb_sample where: (< ts {samples})}})"),
            "(set rfb_live_agg (at rfb_agg 'fp))".to_owned(),
            "(set rfb_live_sample (at rfb_sample 'fp))".to_owned(),
            "(delete {from: 'rfb_fp where: (and (not (in fp rfb_live_agg)) (not (in fp rfb_live_sample)))})".to_owned(),
            "(select {from: rfb_fp fp: fp})".to_owned(),
        ];
        let result: Result<(), String> = async {
            let conn = self.conn().await?;
            let mut last = Value::Other;
            for statement in &statements {
                last = conn.eval(statement).await?;
            }
            let known = Rows::of(last)?;
            self.known = (0..known.len()).map(|row| known.int("fp", row)).collect();
            Ok(())
        }
        .await;
        if let Err(e) = result {
            self.conn = None;
            tracing::error!(error = %e, "statistics: cannot prune the store");
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
    }

    #[test]
    fn every_table_has_as_many_types_as_columns() {
        for (name, source) in schema() {
            let names = source
                .split('[')
                .nth(1)
                .and_then(|s| s.split(']').next())
                .unwrap_or_default();
            let types = source.matches("(as '").count() + source.matches("(list)").count();
            assert_eq!(names.split(' ').count(), types, "{name}: {source}");
        }
    }
}
