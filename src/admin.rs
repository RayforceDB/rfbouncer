//! The admin console: the user `rfbouncer` sends commands as strings and is
//! answered with tables, so any Rayforce client can read them.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;

use bytes::BytesMut;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

use crate::client::read_frame;
use crate::core::Core;
use crate::pool::{Cmd, PoolHandle};
use crate::wire::frame::{self, HEADER_LEN, MSG_RESPONSE, MSG_SYNC};
use crate::wire::value::{self, Column};

/// No command is longer than this.
const COMMAND_MAX: usize = 4096;

pub async fn session(mut stream: TcpStream, version: u8, core: &Arc<Core>) {
    let mut buf = BytesMut::new();
    while let Ok(Some(request)) = read_frame(&mut stream, &mut buf, version, COMMAND_MAX).await {
        if request[7] != MSG_SYNC {
            return;
        }
        let payload = match value::request_text(request[5], &request[HEADER_LEN..]) {
            Some(text) => run(&text, core)
                .await
                .unwrap_or_else(|why| value::error(version, "admin", &why)),
            None => value::error(version, "admin", "send the command as a string"),
        };
        if stream
            .write_all(&frame::frame(version, MSG_RESPONSE, &payload))
            .await
            .is_err()
        {
            return;
        }
    }
}

const HELP: &str = "commands: show pools | servers | clients | stats | config; \
    pause [pool]; resume [pool]; drain <addr>; undrain <addr>; reconnect [pool]; reload; shutdown";

/// Runs one command and returns the payload of its answer.
pub async fn run(text: &str, core: &Arc<Core>) -> Result<Vec<u8>, String> {
    let text = text
        .trim()
        .trim_start_matches('(')
        .trim_end_matches([')', ';']);
    let words: Vec<String> = text.split_whitespace().map(str::to_lowercase).collect();
    let words: Vec<&str> = words.iter().map(String::as_str).collect();
    let ok = Ok(value::str_atom("ok"));
    match words[..] {
        ["show", "pools"] => Ok(show_pools(core).await),
        ["show", "servers"] => Ok(show_servers(core).await),
        ["show", "clients"] => Ok(show_clients(core)),
        ["show", "stats"] => Ok(show_stats(core)),
        ["show", "config"] => Ok(show_config(core)),
        ["pause"] | ["pause", _] => each(core, words.get(1), |p| p.send(Cmd::Pause(true))).and(ok),
        ["resume"] | ["resume", _] => {
            each(core, words.get(1), |p| p.send(Cmd::Pause(false))).and(ok)
        }
        ["reconnect"] | ["reconnect", _] => {
            each(core, words.get(1), |p| p.send(Cmd::Reconnect)).and(ok)
        }
        ["drain", addr] => drain(core, addr, true).await.and(ok),
        ["undrain", addr] => drain(core, addr, false).await.and(ok),
        ["reload"] => core.reload().map_err(|e| e.to_string()).and(ok),
        ["shutdown"] => {
            core.shutdown.send_replace(true);
            ok
        }
        _ => Err(HELP.to_owned()),
    }
}

/// Applies `f` to the pool named, or to every pool when none is.
fn each(core: &Core, name: Option<&&str>, f: impl Fn(&PoolHandle) -> bool) -> Result<(), String> {
    let pools = match name {
        Some(name) => vec![
            core.pool(name)
                .ok_or_else(|| format!("no pool named {name}"))?,
        ],
        None => core.pools(),
    };
    pools.iter().for_each(|p| {
        f(p);
    });
    Ok(())
}

async fn drain(core: &Core, addr: &str, on: bool) -> Result<(), String> {
    let mut found = false;
    for pool in core.pools() {
        let (tx, rx) = oneshot::channel();
        pool.send(Cmd::Drain(addr.to_owned(), on, tx));
        found |= rx.await.unwrap_or(false);
    }
    if found {
        Ok(())
    } else {
        Err(format!("no replica at {addr}"))
    }
}

fn ms(micros: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        micros as f64 / count as f64 / 1000.0
    }
}

async fn show_pools(core: &Core) -> Vec<u8> {
    let (mut name, mut paused, mut queue, mut backends, mut up, mut busy) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut requests, mut ok, mut errors, mut cancelled, mut timeouts) =
        (vec![], vec![], vec![], vec![], vec![]);
    let (mut refused, mut failed, mut retries, mut wait, mut exec) =
        (vec![], vec![], vec![], vec![], vec![]);
    for pool in core.pools() {
        let Some(s) = pool.snapshot().await else {
            continue;
        };
        let c = &pool.shared.counters;
        let n = c.requests.load(Relaxed);
        name.push(s.name.to_string());
        paused.push(i64::from(s.paused));
        queue.push(s.queue.len() as i64);
        backends.push(s.backends.len() as i64);
        up.push(s.backends.iter().filter(|b| b.state != "down").count() as i64);
        busy.push(s.backends.iter().filter(|b| b.state == "busy").count() as i64);
        requests.push(n as i64);
        ok.push(c.ok.load(Relaxed) as i64);
        errors.push(c.server_errors.load(Relaxed) as i64);
        cancelled.push(c.cancelled.load(Relaxed) as i64);
        timeouts.push(c.timeouts.load(Relaxed) as i64);
        refused.push(c.busy.load(Relaxed) as i64);
        failed.push(c.failed.load(Relaxed) as i64);
        retries.push(c.retries.load(Relaxed) as i64);
        wait.push(ms(c.wait_us.load(Relaxed), n));
        exec.push(ms(c.exec_us.load(Relaxed), n));
    }
    value::table(&[
        ("pool", Column::Sym(name)),
        ("paused", Column::I64(paused)),
        ("queue", Column::I64(queue)),
        ("backends", Column::I64(backends)),
        ("up", Column::I64(up)),
        ("busy", Column::I64(busy)),
        ("requests", Column::I64(requests)),
        ("ok", Column::I64(ok)),
        ("errors", Column::I64(errors)),
        ("cancelled", Column::I64(cancelled)),
        ("timeouts", Column::I64(timeouts)),
        ("refused", Column::I64(refused)),
        ("failed", Column::I64(failed)),
        ("retries", Column::I64(retries)),
        ("avg_wait_ms", Column::F64(wait)),
        ("avg_exec_ms", Column::F64(exec)),
    ])
}

async fn show_servers(core: &Core) -> Vec<u8> {
    let (mut pool, mut addr, mut state, mut draining, mut served, mut failures) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    let (mut running_ms, mut client, mut query, mut last_error) = (vec![], vec![], vec![], vec![]);
    for handle in core.pools() {
        let Some(s) = handle.snapshot().await else {
            continue;
        };
        for b in s.backends {
            pool.push(s.name.to_string());
            addr.push(b.addr.to_string());
            state.push(b.state.to_owned());
            draining.push(i64::from(b.draining));
            served.push(b.served as i64);
            failures.push(b.failures as i64);
            let (who, since, text) = b
                .running
                .map_or_else(Default::default, |(who, since, text)| {
                    (who.to_string(), since.as_millis() as i64, text)
                });
            running_ms.push(since);
            client.push(who);
            query.push(text);
            last_error.push(b.last_error);
        }
    }
    value::table(&[
        ("pool", Column::Sym(pool)),
        ("addr", Column::Str(addr)),
        ("state", Column::Sym(state)),
        ("draining", Column::I64(draining)),
        ("served", Column::I64(served)),
        ("failures", Column::I64(failures)),
        ("running_ms", Column::I64(running_ms)),
        ("client", Column::Str(client)),
        ("query", Column::Str(query)),
        ("last_error", Column::Str(last_error)),
    ])
}

fn show_clients(core: &Core) -> Vec<u8> {
    core.with_clients(|clients| {
        let mut rows: Vec<_> = clients.values().collect();
        rows.sort_by_key(|c| c.since);
        value::table(&[
            (
                "pool",
                Column::Sym(rows.iter().map(|c| c.pool.to_string()).collect()),
            ),
            (
                "addr",
                Column::Str(rows.iter().map(|c| c.addr.to_string()).collect()),
            ),
            (
                "user",
                Column::Str(rows.iter().map(|c| c.user.to_string()).collect()),
            ),
            (
                "connected_s",
                Column::I64(
                    rows.iter()
                        .map(|c| c.since.elapsed().as_secs() as i64)
                        .collect(),
                ),
            ),
            (
                "requests",
                Column::I64(rows.iter().map(|c| c.requests as i64).collect()),
            ),
        ])
    })
}

fn pairs(rows: Vec<(&str, String)>) -> Vec<u8> {
    value::table(&[
        (
            "name",
            Column::Sym(rows.iter().map(|r| r.0.to_owned()).collect()),
        ),
        (
            "value",
            Column::Str(rows.into_iter().map(|r| r.1).collect()),
        ),
    ])
}

fn show_stats(core: &Core) -> Vec<u8> {
    pairs(vec![
        ("version", env!("CARGO_PKG_VERSION").to_owned()),
        ("uptime_s", core.started.elapsed().as_secs().to_string()),
        ("clients", core.client_count().to_string()),
        (
            "stats_dropped",
            core.stats.dropped.load(Relaxed).to_string(),
        ),
    ])
}

/// The settings in force. No password is ever shown.
fn show_config(core: &Core) -> Vec<u8> {
    let c = core.config();
    let mut rows = vec![
        ("listen", c.listen.to_string()),
        ("max_client_conn", c.max_client_conn.to_string()),
        ("max_request_bytes", c.max_request_bytes.to_string()),
        ("shutdown_timeout", format!("{:?}", c.shutdown_timeout)),
        (
            "client_idle_timeout",
            format!("{:?}", c.client_idle_timeout),
        ),
        (
            "stats.rayforce",
            c.stats.rayforce.clone().unwrap_or_default(),
        ),
        ("stats.store_samples", c.stats.store_samples.to_string()),
    ];
    for p in &c.pool {
        rows.push((
            "pool",
            format!(
                "{} v{} mode={:?} backends={} slots={} users={:?} default={} restricted={} retry={} affinity={} queue_max={} wait={:?} timeout={:?} grace={:?}",
                p.name,
                p.wire_version,
                p.mode,
                p.backends.join(","),
                p.slots,
                p.users,
                p.default,
                p.require_restricted,
                p.retry,
                p.affinity,
                p.queue_max,
                p.query_wait_timeout,
                p.query_timeout,
                p.cancel_grace
            ),
        ));
    }
    pairs(rows)
}
