//! What the report pages ask for, each a query to the statistics store.
//!
//! The store is a Rayforce process that also takes writes, and part of a
//! filter comes from a URL. Only numbers, and names held to letters, digits,
//! `-` and `_`, ever reach the source of a query.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::stats::Stats;
use crate::stats::store::{BUCKETS, COUNTERS, Rows, quantile};

/// The year 2100, in Unix seconds.
const MAX_TIME: i64 = 4_102_444_800;
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
    /// string. Without a range it is the last hour. A pool name that could
    /// not be a pool's is dropped.
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
                "pool"
                    if value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') =>
                {
                    f.pool = value.to_owned();
                }
                "from" => f.from = value.parse().unwrap_or(f.from),
                "to" => f.to = value.parse().unwrap_or(f.to),
                "fp" => f.fp = u64::from_str_radix(value, 16).ok(),
                "sort" => f.sort = value.to_owned(),
                "limit" => f.limit = value.parse().unwrap_or(f.limit).clamp(1, 500),
                _ => {}
            }
        }
        // Times are held to this century and put in order: they are
        // multiplied and subtracted on the way to a query.
        f.to = f.to.clamp(0, MAX_TIME);
        f.from = f.from.clamp(0, f.to);
        f
    }

    /// The condition every query of this filter shares: `column` between
    /// `lo` and `hi`, the pool, and the fingerprint where one is asked for.
    fn condition(&self, column: &str, lo: i64, hi: i64, with_fp: bool, more: &str) -> String {
        let mut out = format!("(and (>= {column} {lo}) (<= {column} {hi})");
        if !self.pool.is_empty() {
            out.push_str(&format!(" (== pool (as 'SYMBOL \"{}\"))", self.pool));
        }
        if let (true, Some(fp)) = (with_fp, self.fp) {
            out.push_str(&format!(" (== fp {})", fp as i64));
        }
        out.push_str(more);
        out.push(')');
        out
    }

    fn minutes(&self, with_fp: bool) -> String {
        self.condition("minute", self.from / 60, self.to / 60, with_fp, "")
    }

    /// The same filter over the period just before this one: as many
    /// minutes, ending where this one begins. Rows are whole minutes, so
    /// the two are alike to within a minute of requests.
    fn before(&self) -> Self {
        let (first, last) = (self.from / 60, self.to / 60);
        Self {
            from: (2 * first - last - 1) * 60,
            to: (first - 1) * 60,
            ..self.clone()
        }
    }
}

#[derive(Default, Clone)]
struct Acc {
    counters: [u64; COUNTERS.len()],
    hist: [u64; BUCKETS],
    wait_hist: [u64; BUCKETS],
}

impl Acc {
    fn get(&self, name: &str) -> u64 {
        COUNTERS
            .iter()
            .position(|c| *c == name)
            .map_or(0, |i| self.counters[i])
    }

    fn add(&mut self, o: &Self) {
        for (i, name) in COUNTERS.iter().enumerate() {
            self.counters[i] = if name.ends_with("_max") {
                self.counters[i].max(o.counters[i])
            } else {
                self.counters[i] + o.counters[i]
            };
        }
        for (mine, theirs) in self.hist.iter_mut().zip(o.hist) {
            *mine += theirs;
        }
        for (mine, theirs) in self.wait_hist.iter_mut().zip(o.wait_hist) {
            *mine += theirs;
        }
    }

    fn not_ok(&self) -> u64 {
        ["errors", "cancels", "timeouts", "refused", "failed"]
            .iter()
            .map(|c| self.get(c))
            .sum()
    }

    fn json(&self) -> Value {
        let count = self.get("count");
        let per = |sum: u64| {
            if count == 0 {
                0.0
            } else {
                ms(sum) / count as f64
            }
        };
        json!({
            "count": count,
            "not_ok": self.not_ok(),
            "errors": self.get("errors"),
            "cancels": self.get("cancels"),
            "timeouts": self.get("timeouts"),
            "refused": self.get("refused"),
            "failed": self.get("failed"),
            "retries": self.get("retries"),
            "total_ms": ms(self.get("dur_sum")),
            "avg_ms": per(self.get("dur_sum")),
            "max_ms": ms(self.get("dur_max")),
            "wait_avg_ms": per(self.get("wait_sum")),
            "wait_max_ms": ms(self.get("wait_max")),
            "wait_p50_ms": ms(quantile(&self.wait_hist, 0.50)),
            "wait_p95_ms": ms(quantile(&self.wait_hist, 0.95)),
            "wait_p99_ms": ms(quantile(&self.wait_hist, 0.99)),
            "p50_ms": ms(quantile(&self.hist, 0.50)),
            "p95_ms": ms(quantile(&self.hist, 0.95)),
            "p99_ms": ms(quantile(&self.hist, 0.99)),
            "bytes_in": self.get("bytes_in"),
            "bytes_out": self.get("bytes_out"),
        })
    }
}

fn ms(micros: u64) -> f64 {
    micros as f64 / 1000.0
}

/// The aggregate rows the filter covers, summed per `key`: a time step or a
/// fingerprint. The summing is the store's.
async fn summed(stats: &Stats, f: &Filter, key: &str) -> Result<Vec<(i64, Acc)>, String> {
    let mut columns = String::new();
    for name in COUNTERS {
        let how = if name.ends_with("_max") { "max" } else { "sum" };
        columns.push_str(&format!(" s_{name}: ({how} {name})"));
    }
    for i in 0..BUCKETS {
        columns.push_str(&format!(" s_h{i}: (sum h{i}) s_w{i}: (sum w{i})"));
    }
    let source = format!(
        "(select {{from: rfb_agg where: {} by: {{k: {key}}}{columns}}})",
        f.minutes(true)
    );
    let rows = Rows::of(stats.query(&source).await?)?;
    Ok((0..rows.len())
        .map(|row| {
            let mut acc = Acc::default();
            for (slot, name) in acc.counters.iter_mut().zip(COUNTERS) {
                *slot = rows.int(&format!("s_{name}"), row);
            }
            for (i, slot) in acc.hist.iter_mut().enumerate() {
                *slot = rows.int(&format!("s_h{i}"), row);
            }
            for (i, slot) in acc.wait_hist.iter_mut().enumerate() {
                *slot = rows.int(&format!("s_w{i}"), row);
            }
            (rows.int("k", row) as i64, acc)
        })
        .collect())
}

/// The whole range as one row, and as a series of at most `POINTS` steps.
async fn series(stats: &Stats, f: &Filter) -> Result<Value, String> {
    let step = (((f.to - f.from) / 60 + POINTS) / POINTS).max(1);
    let rows = summed(stats, f, &format!("(xbar minute {step})")).await?;
    let mut total = Acc::default();
    let mut points = BTreeMap::new();
    for (minute, acc) in rows {
        total.add(&acc);
        points.insert(minute, acc);
    }
    let points: Vec<Value> = points
        .iter()
        .map(|(minute, acc)| {
            let mut point = acc.json();
            point["t"] = json!(minute * 60);
            point
        })
        .collect();
    // What an operator asks during an incident is what changed: the period
    // before, as one row, is what the totals are held against.
    let mut previous = Acc::default();
    for (_, acc) in summed(stats, &f.before(), &format!("(xbar minute {MAX_TIME})")).await? {
        previous.add(&acc);
    }
    Ok(
        json!({ "from": f.from, "to": f.to, "step_s": step * 60, "total": total.json(), "previous": previous.json(), "points": points }),
    )
}

/// How the requests of the range ended, by outcome: `ok`, the bouncer's own
/// `cancel`, `timeout`, `busy` and `io`, and every error code a replica
/// raised.
async fn outcomes(stats: &Stats, f: &Filter) -> Result<Value, String> {
    let source = format!(
        "(select {{from: rfb_status where: {} by: status n: (sum count)}})",
        f.minutes(false)
    );
    let rows = Rows::of(stats.query(&source).await?)?;
    let mut list: Vec<(String, u64)> = (0..rows.len())
        .map(|row| (rows.text("status", row), rows.int("n", row)))
        .collect();
    list.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(list
        .into_iter()
        .map(|(status, count)| json!({ "status": status, "count": count }))
        .collect())
}

pub async fn overview(stats: &Stats, f: &Filter) -> Result<Value, String> {
    let mut out = series(stats, f).await?;
    out["outcomes"] = outcomes(stats, f).await?;
    Ok(out)
}

/// The text of each of these fingerprints.
async fn texts(stats: &Stats, fps: &[u64]) -> Result<BTreeMap<u64, String>, String> {
    if fps.is_empty() {
        return Ok(BTreeMap::new());
    }
    let list: Vec<String> = fps.iter().map(|fp| (*fp as i64).to_string()).collect();
    let source = format!(
        "(select {{from: rfb_fp where: (in fp [{}])}})",
        list.join(" ")
    );
    let rows = Rows::of(stats.query(&source).await?)?;
    Ok((0..rows.len())
        .map(|row| (rows.int("fp", row), rows.text("text", row)))
        .collect())
}

/// The queries of the range, the heaviest first by `sort`.
pub async fn top(stats: &Stats, f: &Filter) -> Result<Value, String> {
    let mut rows = summed(stats, f, "fp").await?;
    let mut all = Acc::default();
    rows.iter().for_each(|(_, acc)| all.add(acc));
    let key = |a: &Acc| match f.sort.as_str() {
        "count" => a.get("count"),
        "avg" => a.get("dur_sum") / a.get("count").max(1),
        "max" => a.get("dur_max"),
        "errors" => a.not_ok(),
        "bytes" => a.get("bytes_out"),
        _ => a.get("dur_sum"),
    };
    rows.sort_by_key(|(fp, acc)| (std::cmp::Reverse(key(acc)), *fp));
    rows.truncate(f.limit);
    let fps: Vec<u64> = rows.iter().map(|(fp, _)| *fp as u64).collect();
    let texts = texts(stats, &fps).await?;
    let before: BTreeMap<i64, Acc> = summed(stats, &f.before(), "fp")
        .await?
        .into_iter()
        .collect();
    let total = all.get("dur_sum");
    let listed: Vec<Value> = rows
        .iter()
        .map(|(fp, acc)| {
            let fp = *fp as u64;
            let mut row = acc.json();
            row["fp"] = json!(format!("{fp:016x}"));
            row["text"] = json!(texts.get(&fp).cloned().unwrap_or_default());
            let previous = before.get(&(fp as i64)).cloned().unwrap_or_default();
            row["previous"] = json!({
                "count": previous.get("count"),
                "avg_ms": previous.json()["avg_ms"],
            });
            row["share"] = json!(if total == 0 {
                0.0
            } else {
                acc.get("dur_sum") as f64 / total as f64
            });
            row
        })
        .collect();
    Ok(json!({ "rows": listed, "total": all.json() }))
}

async fn samples(stats: &Stats, f: &Filter, kind: &str, order: &str) -> Result<Value, String> {
    let condition = f.condition(
        "ts",
        f.from * 1000,
        f.to * 1000 + 999,
        true,
        &format!(" (== kind '{kind})"),
    );
    let source = format!(
        "(select {{from: rfb_sample where: {condition} desc: {order} take: {}}})",
        f.limit
    );
    let rows = Rows::of(stats.query(&source).await?)?;
    Ok((0..rows.len())
        .map(|row| {
            json!({
                "ts": rows.int("ts", row),
                "pool": rows.text("pool", row),
                "fp": format!("{:016x}", rows.int("fp", row)),
                "ms": ms(rows.int("dur", row)),
                "wait_ms": ms(rows.int("wait", row)),
                "backend": rows.text("backend", row),
                "client": rows.text("client", row),
                "status": rows.text("status", row),
                "text": rows.text("text", row),
            })
        })
        .collect())
}

/// One query: its series, and the requests kept of it.
pub async fn query(stats: &Stats, f: &Filter) -> Result<Value, String> {
    let mut out = series(stats, f).await?;
    let fps: Vec<u64> = f.fp.into_iter().collect();
    out["text"] = json!(
        texts(stats, &fps)
            .await?
            .into_values()
            .next()
            .unwrap_or_default()
    );
    out["slow"] = samples(stats, f, "slow", "dur").await?;
    out["failed"] = samples(stats, f, "error", "ts").await?;
    out["clients"] = json!(grouped_rows(stats, f, "rfb_fp_client", true).await?);
    out["backends"] = json!(grouped_rows(stats, f, "rfb_fp_backend", true).await?);
    Ok(out)
}

pub async fn slow(stats: &Stats, f: &Filter) -> Result<Value, String> {
    Ok(json!({ "rows": samples(stats, f, "slow", "dur").await? }))
}

/// The failed requests kept of the range, newest first, with the exact
/// count of every outcome.
pub async fn errors(stats: &Stats, f: &Filter) -> Result<Value, String> {
    Ok(
        json!({ "outcomes": outcomes(stats, f).await?, "rows": samples(stats, f, "error", "ts").await? }),
    )
}

/// Requests per client address (`rfb_client`) or per replica
/// (`rfb_backend`), the busiest first.
pub async fn grouped(stats: &Stats, f: &Filter, table: &str) -> Result<Value, String> {
    Ok(json!({ "rows": grouped_rows(stats, f, table, false).await? }))
}

/// The rows of `grouped`; `with_fp` for the tables that also have a query
/// shape in their key, of which the filter's is taken.
async fn grouped_rows(
    stats: &Stats,
    f: &Filter,
    table: &str,
    with_fp: bool,
) -> Result<Vec<Value>, String> {
    let source = format!(
        "(select {{from: {table} where: {} by: [pool who] n: (sum count) e: (sum errors) d: (sum dur_sum)}})",
        f.minutes(with_fp)
    );
    let rows = Rows::of(stats.query(&source).await?)?;
    let mut order: Vec<usize> = (0..rows.len()).collect();
    order.sort_by_key(|row| {
        (
            std::cmp::Reverse(rows.int("n", *row)),
            rows.text("who", *row),
        )
    });
    let listed: Vec<Value> = order
        .into_iter()
        .take(f.limit)
        .map(|row| {
            let (count, dur) = (rows.int("n", row), rows.int("d", row));
            json!({
                "pool": rows.text("pool", row),
                "name": rows.text("who", row),
                "count": count,
                "not_ok": rows.int("e", row),
                "total_ms": ms(dur),
                "avg_ms": if count == 0 { 0.0 } else { ms(dur) / count as f64 },
            })
        })
        .collect();
    Ok(listed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_filter_takes_only_what_is_safe_to_put_in_a_query() {
        let f = Filter::parse("pool=main&from=100&to=200&fp=ff&sort=count&limit=7");
        assert_eq!(
            (f.pool.as_str(), f.from, f.to, f.fp, f.limit),
            ("main", 100, 200, Some(255), 7)
        );
        assert_eq!(
            f.minutes(true),
            "(and (>= minute 1) (<= minute 3) (== pool (as 'SYMBOL \"main\")) (== fp 255))"
        );
        // A name that could not be a pool's never reaches a query.
        for pool in ["a\")(exit", "a b", "x'y", "%22"] {
            assert_eq!(Filter::parse(&format!("pool={pool}")).pool, "");
        }
        assert_eq!(Filter::parse("fp=zz").fp, None);
        assert_eq!(Filter::parse("limit=999999").limit, 500);
        assert_eq!(Filter::parse("limit=0").limit, 1);
    }

    #[test]
    fn the_period_before_is_as_long_and_ends_where_the_range_begins() {
        let f = Filter::parse("from=6000&to=9599&pool=main&fp=ff");
        assert_eq!(
            f.before().minutes(true),
            "(and (>= minute 40) (<= minute 99) (== pool (as 'SYMBOL \"main\")) (== fp 255))"
        );
        // At the start of time there is nothing before, and nothing breaks.
        assert_eq!(
            Filter::parse("from=0&to=59").before().minutes(false),
            "(and (>= minute -1) (<= minute -1))"
        );
    }

    #[test]
    fn a_wild_range_is_brought_into_one_that_can_be_computed_with() {
        for query in [
            "to=9223372036854775807",
            "from=-9223372036854775808",
            "from=500&to=100",
            "to=-5",
        ] {
            let f = Filter::parse(query);
            assert!(
                0 <= f.from && f.from <= f.to && f.to <= MAX_TIME,
                "{query}: {f:?}"
            );
            // What the reports compute from it.
            let _ = (f.to * 1000 + 999, ((f.to - f.from) / 60 + POINTS) / POINTS);
        }
    }
}
