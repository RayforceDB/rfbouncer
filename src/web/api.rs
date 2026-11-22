//! What the report pages ask for, read out of the statistics database.
//! Everything here blocks, so it is called through `spawn_blocking`.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};

use crate::stats::store::{BUCKETS, hist_from_blob, quantile};

/// Points a time series is thinned to, whatever the range.
const POINTS: i64 = 120;

/// The part of the statistics a request is about.
#[derive(Debug, Default, Clone)]
pub struct Filter {
    /// Empty for every pool.
    pub pool: String,
    /// Unix seconds, both ends included.
    pub from: i64,
    pub to: i64,
    pub fp: Option<u64>,
    pub sort: String,
    pub limit: usize,
}

impl Filter {
    /// Reads `pool`, `from`, `to`, `fp`, `sort` and `limit` off a query
    /// string. Without a range it is the last hour.
    pub fn parse(query: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        let mut f = Self {
            from: now - 3600,
            to: now,
            limit: 50,
            ..Self::default()
        };
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            match key {
                "pool" => f.pool = value.to_owned(),
                "from" => f.from = value.parse().unwrap_or(f.from),
                "to" => f.to = value.parse().unwrap_or(f.to),
                "fp" => f.fp = u64::from_str_radix(value, 16).ok(),
                "sort" => f.sort = value.to_owned(),
                "limit" => f.limit = value.parse().unwrap_or(f.limit).clamp(1, 500),
                _ => {}
            }
        }
        f
    }
}

#[derive(Default, Clone)]
struct Acc {
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
    bytes_in: u64,
    bytes_out: u64,
    hist: [u64; BUCKETS],
}

impl Acc {
    fn add(&mut self, o: &Self) {
        self.count += o.count;
        self.errors += o.errors;
        self.cancels += o.cancels;
        self.timeouts += o.timeouts;
        self.refused += o.refused;
        self.failed += o.failed;
        self.retries += o.retries;
        self.dur_sum += o.dur_sum;
        self.dur_max = self.dur_max.max(o.dur_max);
        self.wait_sum += o.wait_sum;
        self.bytes_in += o.bytes_in;
        self.bytes_out += o.bytes_out;
        for (mine, theirs) in self.hist.iter_mut().zip(o.hist) {
            *mine += theirs;
        }
    }

    fn not_ok(&self) -> u64 {
        self.errors + self.cancels + self.timeouts + self.refused + self.failed
    }

    fn json(&self) -> Value {
        let per = |sum: u64| {
            if self.count == 0 {
                0.0
            } else {
                ms(sum) / self.count as f64
            }
        };
        json!({
            "count": self.count,
            "not_ok": self.not_ok(),
            "errors": self.errors,
            "cancels": self.cancels,
            "timeouts": self.timeouts,
            "refused": self.refused,
            "failed": self.failed,
            "retries": self.retries,
            "total_ms": ms(self.dur_sum),
            "avg_ms": per(self.dur_sum),
            "max_ms": ms(self.dur_max),
            "wait_avg_ms": per(self.wait_sum),
            "p50_ms": ms(quantile(&self.hist, 0.50)),
            "p95_ms": ms(quantile(&self.hist, 0.95)),
            "p99_ms": ms(quantile(&self.hist, 0.99)),
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        })
    }
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}

pub fn open(path: &Path) -> rusqlite::Result<Connection> {
    Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
}

/// Every aggregate row the filter covers, as `(minute, fingerprint, row)`.
fn scan(db: &Connection, f: &Filter) -> rusqlite::Result<Vec<(i64, u64, Acc)>> {
    let mut query = db.prepare_cached(
        "SELECT minute, fp, count, errors, cancels, timeouts, refused, failed, retries, dur_sum, dur_max,
                wait_sum, bytes_in, bytes_out, hist
         FROM agg WHERE minute BETWEEN ?1 AND ?2 AND (?3 = '' OR pool = ?3) AND (?4 IS NULL OR fp = ?4)",
    )?;
    let u = |row: &rusqlite::Row<'_>, i: usize| row.get::<_, i64>(i).map(|n| n as u64);
    let rows = query.query_map(
        params![f.from / 60, f.to / 60, f.pool, f.fp.map(|fp| fp as i64)],
        |row| {
            let acc = Acc {
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
                bytes_in: u(row, 12)?,
                bytes_out: u(row, 13)?,
                hist: hist_from_blob(&row.get::<_, Vec<u8>>(14)?),
            };
            Ok((row.get(0)?, u(row, 1)?, acc))
        },
    )?;
    rows.collect()
}

/// The whole range as one row, and as a series of at most `POINTS` steps.
fn series(rows: &[(i64, u64, Acc)], f: &Filter) -> Value {
    let step = (((f.to - f.from) / 60 + POINTS) / POINTS).max(1);
    let mut total = Acc::default();
    let mut points: BTreeMap<i64, Acc> = BTreeMap::new();
    for (minute, _, acc) in rows {
        total.add(acc);
        points
            .entry(minute - minute.rem_euclid(step))
            .or_default()
            .add(acc);
    }
    let points: Vec<Value> = points
        .iter()
        .map(|(minute, acc)| {
            let mut point = acc.json();
            point["t"] = json!(minute * 60);
            point
        })
        .collect();
    json!({ "from": f.from, "to": f.to, "step_s": step * 60, "total": total.json(), "points": points, "hist": total.hist.to_vec() })
}

pub fn overview(db: &Connection, f: &Filter) -> rusqlite::Result<Value> {
    Ok(series(&scan(db, f)?, f))
}

fn text_of(db: &Connection, fp: u64) -> String {
    db.query_row(
        "SELECT text FROM fingerprints WHERE fp = ?",
        [fp as i64],
        |row| row.get(0),
    )
    .unwrap_or_default()
}

/// The queries of the range, the heaviest first by `sort`.
pub fn top(db: &Connection, f: &Filter) -> rusqlite::Result<Value> {
    let mut by_fp: HashMap<u64, Acc> = HashMap::new();
    let mut all = Acc::default();
    for (_, fp, acc) in scan(db, f)? {
        all.add(&acc);
        by_fp.entry(fp).or_default().add(&acc);
    }
    let mut rows: Vec<(u64, Acc)> = by_fp.into_iter().collect();
    let key = |a: &Acc| match f.sort.as_str() {
        "count" => a.count,
        "avg" => a.dur_sum / a.count.max(1),
        "max" => a.dur_max,
        "errors" => a.not_ok(),
        "bytes" => a.bytes_out,
        _ => a.dur_sum,
    };
    rows.sort_by_key(|(fp, acc)| (std::cmp::Reverse(key(acc)), *fp));
    let queries = rows.len();
    let rows: Vec<Value> = rows
        .iter()
        .take(f.limit)
        .map(|(fp, acc)| {
            let mut row = acc.json();
            row["fp"] = json!(format!("{fp:016x}"));
            row["text"] = json!(text_of(db, *fp));
            row["share"] = json!(if all.dur_sum == 0 {
                0.0
            } else {
                acc.dur_sum as f64 / all.dur_sum as f64
            });
            row
        })
        .collect();
    Ok(json!({ "queries": queries, "rows": rows }))
}

fn samples(db: &Connection, f: &Filter, kind: &str, order: &str) -> rusqlite::Result<Vec<Value>> {
    let sql = format!(
        "SELECT ts, pool, fp, dur, wait, backend, client, status, text FROM samples
         WHERE kind = ?1 AND ts BETWEEN ?2 AND ?3 AND (?4 = '' OR pool = ?4) AND (?5 IS NULL OR fp = ?5)
         ORDER BY {order} DESC LIMIT ?6"
    );
    let mut query = db.prepare_cached(&sql)?;
    let rows = query.query_map(
        params![
            kind,
            f.from * 1000,
            f.to * 1000 + 999,
            f.pool,
            f.fp.map(|fp| fp as i64),
            f.limit as i64
        ],
        |row| {
            Ok(json!({
                "ts": row.get::<_, i64>(0)?,
                "pool": row.get::<_, String>(1)?,
                "fp": format!("{:016x}", row.get::<_, i64>(2)? as u64),
                "ms": ms(row.get::<_, i64>(3)? as u64),
                "wait_ms": ms(row.get::<_, i64>(4)? as u64),
                "backend": row.get::<_, String>(5)?,
                "client": row.get::<_, String>(6)?,
                "status": row.get::<_, String>(7)?,
                "text": row.get::<_, String>(8)?,
            }))
        },
    )?;
    rows.collect()
}

/// One query: its series, and the requests kept of it.
pub fn query(db: &Connection, f: &Filter) -> rusqlite::Result<Value> {
    let mut out = series(&scan(db, f)?, f);
    out["text"] = json!(f.fp.map(|fp| text_of(db, fp)).unwrap_or_default());
    out["slow"] = json!(samples(db, f, "slow", "dur")?);
    out["failed"] = json!(samples(db, f, "error", "ts")?);
    Ok(out)
}

pub fn slow(db: &Connection, f: &Filter) -> rusqlite::Result<Value> {
    Ok(json!({ "rows": samples(db, f, "slow", "dur")? }))
}

/// The failures kept of the range, newest first, and how many of each kind
/// were kept. The exact counts per kind are in the overview's total.
pub fn errors(db: &Connection, f: &Filter) -> rusqlite::Result<Value> {
    let mut query = db.prepare_cached(
        "SELECT status, count(*) FROM samples
         WHERE kind = 'error' AND ts BETWEEN ?1 AND ?2 AND (?3 = '' OR pool = ?3) GROUP BY status ORDER BY 2 DESC",
    )?;
    let kinds = query.query_map(params![f.from * 1000, f.to * 1000 + 999, f.pool], |row| {
        Ok(json!({ "status": row.get::<_, String>(0)?, "sampled": row.get::<_, i64>(1)? }))
    })?;
    let kinds: Vec<Value> = kinds.collect::<rusqlite::Result<_>>()?;
    Ok(json!({ "kinds": kinds, "rows": samples(db, f, "error", "ts")? }))
}

/// Requests per client address or per replica, the busiest first.
pub fn grouped(db: &Connection, f: &Filter, table: &str, column: &str) -> rusqlite::Result<Value> {
    let sql = format!(
        "SELECT pool, {column}, sum(count), sum(errors), sum(dur_sum) FROM {table}
         WHERE minute BETWEEN ?1 AND ?2 AND (?3 = '' OR pool = ?3)
         GROUP BY pool, {column} ORDER BY 3 DESC LIMIT ?4"
    );
    let mut query = db.prepare_cached(&sql)?;
    let rows = query.query_map(
        params![f.from / 60, f.to / 60, f.pool, f.limit as i64],
        |row| {
            let (count, dur) = (row.get::<_, i64>(2)? as u64, row.get::<_, i64>(4)? as u64);
            Ok(json!({
                "pool": row.get::<_, String>(0)?,
                "name": row.get::<_, String>(1)?,
                "count": count,
                "not_ok": row.get::<_, i64>(3)?,
                "total_ms": ms(dur),
                "avg_ms": if count == 0 { 0.0 } else { ms(dur) / count as f64 },
            }))
        },
    )?;
    let rows: Vec<Value> = rows.collect::<rusqlite::Result<_>>()?;
    Ok(json!({ "rows": rows }))
}
