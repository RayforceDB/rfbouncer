//! The counters, in the Prometheus text format.

use std::fmt::Write;
use std::sync::atomic::Ordering::Relaxed;

use crate::core::Core;

pub async fn render(core: &Core) -> String {
    let mut out = String::new();
    let mut gauge = |name: &str, help: &str, value: f64| {
        let _ = writeln!(
            out,
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {value}"
        );
    };
    gauge(
        "rfbouncer_uptime_seconds",
        "Seconds since the bouncer started.",
        core.started.elapsed().as_secs_f64(),
    );
    gauge(
        "rfbouncer_clients",
        "Client connections past their greeting.",
        core.client_count() as f64,
    );
    gauge(
        "rfbouncer_stats_dropped",
        "Requests the statistics did not keep up with.",
        core.stats.dropped.load(Relaxed) as f64,
    );

    let mut requests = String::new();
    let mut totals = String::new();
    let mut gauges = String::new();
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
        ] {
            let _ = writeln!(
                requests,
                "rfbouncer_requests_total{{pool=\"{name}\",outcome=\"{outcome}\"}} {}",
                n.load(Relaxed)
            );
        }
        for (metric, value) in [
            ("retries_total", c.retries.load(Relaxed) as f64),
            ("bytes_in_total", c.bytes_in.load(Relaxed) as f64),
            ("bytes_out_total", c.bytes_out.load(Relaxed) as f64),
            ("wait_seconds_total", c.wait_us.load(Relaxed) as f64 / 1e6),
            ("exec_seconds_total", c.exec_us.load(Relaxed) as f64 / 1e6),
        ] {
            let _ = writeln!(totals, "rfbouncer_{metric}{{pool=\"{name}\"}} {value}");
        }
        let _ = writeln!(
            gauges,
            "rfbouncer_queue_length{{pool=\"{name}\"}} {}",
            s.queue.len()
        );
        for state in ["down", "idle", "busy"] {
            let n = s.backends.iter().filter(|b| b.state == state).count();
            let _ = writeln!(
                gauges,
                "rfbouncer_backends{{pool=\"{name}\",state=\"{state}\"}} {n}"
            );
        }
    }
    out.push_str("# HELP rfbouncer_requests_total Requests ended, by how they ended.\n# TYPE rfbouncer_requests_total counter\n");
    out.push_str(&requests);
    out.push_str(&totals);
    out.push_str(&gauges);
    out
}
