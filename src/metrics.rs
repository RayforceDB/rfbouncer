//! The counters, in the Prometheus text format.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use crate::core::Core;
use crate::stats::store::BUCKETS;

/// A histogram's lines: one cumulative count per bucket, then the total.
/// Bucket `i` counts what took under 2^i µs.
fn histogram(out: &mut String, name: &str, pool: &str, hist: &[AtomicU64; BUCKETS], sum_us: u64) {
    let mut seen = 0;
    for (i, n) in hist.iter().enumerate().take(BUCKETS - 1) {
        seen += n.load(Relaxed);
        let le = (1u64 << i) as f64 / 1e6;
        let _ = writeln!(out, "{name}_bucket{{pool=\"{pool}\",le=\"{le}\"}} {seen}");
    }
    seen += hist[BUCKETS - 1].load(Relaxed);
    let _ = writeln!(out, "{name}_bucket{{pool=\"{pool}\",le=\"+Inf\"}} {seen}");
    let _ = writeln!(out, "{name}_sum{{pool=\"{pool}\"}} {}", sum_us as f64 / 1e6);
    let _ = writeln!(out, "{name}_count{{pool=\"{pool}\"}} {seen}");
}

pub async fn render(core: &Core) -> String {
    let mut out = String::new();
    let mut one = |kind: &str, name: &str, help: &str, value: f64| {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}"
        );
    };
    one(
        "gauge",
        "rfbouncer_uptime_seconds",
        "Seconds since the bouncer started.",
        core.started.elapsed().as_secs_f64(),
    );
    one(
        "gauge",
        "rfbouncer_clients",
        "Client connections past their greeting.",
        core.client_count() as f64,
    );
    one(
        "gauge",
        "rfbouncer_stats_enabled",
        "Whether request statistics are kept.",
        f64::from(u8::from(core.stats.enabled())),
    );
    let dropped = core.stats.dropped.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_stats_dropped_total",
        "Requests the statistics did not keep up with.",
        dropped,
    );
    let refused = core.auth_failures.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_auth_failures_total",
        "Credentials refused, over IPC or HTTP.",
        refused,
    );
    let turned_away = core.refused.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_connections_refused_total",
        "Connections turned away at max_client_conn.",
        turned_away,
    );
    let lockouts = core.lockout.lockouts.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_auth_lockouts_total",
        "Times an address was locked out after too many refused logins.",
        lockouts,
    );
    let locked = core.lockout.locked_now() as f64;
    one(
        "gauge",
        "rfbouncer_auth_locked_addresses",
        "Addresses locked out now.",
        locked,
    );
    let turned_away = core.lockout.turned_away.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_auth_locked_out_total",
        "Logins refused unexamined because their address was locked out.",
        turned_away,
    );
    let cleartext = core.cleartext_refused.load(Relaxed) as f64;
    one(
        "counter",
        "rfbouncer_cleartext_refused_total",
        "Connections and requests in clear text refused where TLS is required.",
        cleartext,
    );

    let (mut requests, mut totals, mut gauges, mut replicas) =
        (String::new(), String::new(), String::new(), String::new());
    let (mut exec, mut wait) = (String::new(), String::new());
    let mut health = String::new();
    for pool in core.pools() {
        let Some(s) = pool.snapshot().await else {
            continue;
        };
        let (name, c) = (&s.name, &pool.shared.counters);
        for (outcome, n) in [
            ("ok", &c.ok),
            ("error", &c.server_errors),
            ("cancel", &c.cancelled),
            ("timeout", &c.timeouts),
            ("busy", &c.busy),
            ("io", &c.failed),
            ("cached", &c.cached),
        ] {
            let n = n.load(Relaxed);
            let _ = writeln!(
                requests,
                "rfbouncer_requests_total{{pool=\"{name}\",outcome=\"{outcome}\"}} {n}"
            );
        }
        for (metric, value) in [
            ("retries_total", c.retries.load(Relaxed)),
            ("bytes_in_total", c.bytes_in.load(Relaxed)),
            ("bytes_out_total", c.bytes_out.load(Relaxed)),
            ("compressed_total", c.compressed.load(Relaxed)),
            ("compress_saved_bytes_total", c.compress_saved.load(Relaxed)),
            ("cache_hits_total", pool.shared.cache.hits.load(Relaxed)),
            ("cache_misses_total", pool.shared.cache.misses.load(Relaxed)),
            ("cache_entries", pool.shared.cache.size().0 as u64),
            ("cache_bytes", pool.shared.cache.size().1 as u64),
        ] {
            let _ = writeln!(totals, "rfbouncer_{metric}{{pool=\"{name}\"}} {value}");
        }
        for (result, n) in [
            ("ok", &c.health_checks_ok),
            ("failed", &c.health_checks_failed),
        ] {
            let n = n.load(Relaxed);
            let _ = writeln!(
                health,
                "rfbouncer_health_checks_total{{pool=\"{name}\",result=\"{result}\"}} {n}"
            );
        }
        histogram(
            &mut exec,
            "rfbouncer_exec_seconds",
            name,
            &c.exec_hist,
            c.exec_us.load(Relaxed),
        );
        histogram(
            &mut wait,
            "rfbouncer_wait_seconds",
            name,
            &c.wait_hist,
            c.wait_us.load(Relaxed),
        );
        let _ = writeln!(
            gauges,
            "rfbouncer_queue_length{{pool=\"{name}\"}} {}",
            s.queue.len()
        );
        let _ = writeln!(
            gauges,
            "rfbouncer_paused{{pool=\"{name}\"}} {}",
            u8::from(s.paused)
        );
        for state in ["down", "idle", "busy"] {
            let n = s.backends.iter().filter(|b| b.state() == state).count();
            let _ = writeln!(
                gauges,
                "rfbouncer_backends{{pool=\"{name}\",state=\"{state}\"}} {n}"
            );
        }
        // One line per connection: a replica with several slots has several.
        for (slot, b) in s.backends.iter().enumerate() {
            let labels = format!("pool=\"{name}\",replica=\"{}\",slot=\"{slot}\"", b.addr);
            let _ = writeln!(
                replicas,
                "rfbouncer_replica_up{{{labels}}} {}",
                u8::from(b.connected)
            );
            let _ = writeln!(
                replicas,
                "rfbouncer_replica_served_total{{{labels}}} {}",
                b.served
            );
            let _ = writeln!(
                replicas,
                "rfbouncer_replica_failures_total{{{labels}}} {}",
                b.failures
            );
        }
    }
    out.push_str("# HELP rfbouncer_requests_total Requests ended, by how they ended.\n# TYPE rfbouncer_requests_total counter\n");
    out.push_str(&requests);
    out.push_str(&totals);
    out.push_str("# HELP rfbouncer_exec_seconds Time a replica took over a request.\n# TYPE rfbouncer_exec_seconds histogram\n");
    out.push_str(&exec);
    out.push_str("# HELP rfbouncer_wait_seconds Time a request waited for a replica.\n# TYPE rfbouncer_wait_seconds histogram\n");
    out.push_str(&wait);
    out.push_str("# HELP rfbouncer_health_checks_total Checks of idle replica connections, by whether the replica answered in time.\n# TYPE rfbouncer_health_checks_total counter\n");
    out.push_str(&health);
    out.push_str(&gauges);
    out.push_str(&replicas);
    out
}
