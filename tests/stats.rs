//! The statistics and the pages that show them, on the port clients use.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use serde_json::Value;
use support::{Bouncer, Client, Rayforce, http_get, http_json, int, pool, slow};

/// A bouncer configuration over `replica` that keeps its statistics in
/// `store`.
fn config(replica: &Rayforce, store: &Rayforce) -> String {
    format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\npassword = \"s\"\n{}",
        store.addr(),
        pool(&[replica], "require_restricted = false")
    )
}

fn row<'a>(top: &'a Value, text: &str) -> &'a Value {
    top["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["text"] == text)
        .unwrap_or_else(|| panic!("no row {text:?} in {top}"))
}

#[test]
fn requests_are_counted_by_the_shape_of_their_query() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&["-u", "s"]);
    let bouncer = Bouncer::start(&config(&replica, &store));
    let mut c = Client::connect(bouncer.port);
    for i in 0..30 {
        assert_eq!(c.query(&format!("(+ {i} 1)")), int(i + 1));
    }
    for _ in 0..3 {
        assert_eq!(c.query_error("(nosuch 1)"), Some("name".to_owned()));
    }
    c.send(&slow(60_000));
    bouncer.wait_busy(1);
    // It is let run for a while, so there is a time to find in the reports.
    std::thread::sleep(std::time::Duration::from_millis(300));

    // While that one runs, the page shows it.
    let live = http_json(bouncer.port, "/api/live", "root");
    assert_eq!(live["pools"][0]["backends"][0]["state"], "busy");
    assert_eq!(
        live["pools"][0]["backends"][0]["running"]["text"],
        slow(60_000)
    );
    assert_eq!(live["clients"], 1);
    c.cancel();
    c.recv();

    let top = http_json(bouncer.port, "/api/top?sort=count", "root");
    assert_eq!(top["rows"].as_array().unwrap().len(), 3);
    assert_eq!(top["total"]["count"], 34);
    let plus = row(&top, "(+ ? ?)");
    assert_eq!(
        (plus["count"].as_u64(), plus["not_ok"].as_u64()),
        (Some(30), Some(0))
    );
    assert_eq!(top["rows"][0]["text"], "(+ ? ?)");
    let nosuch = row(&top, "(nosuch ?)");
    assert_eq!(
        (nosuch["count"].as_u64(), nosuch["errors"].as_u64()),
        (Some(3), Some(3))
    );
    let slow_row = row(&top, "(count (map (fn [i] (sum (til ?))) (til ?)))");
    assert_eq!(slow_row["cancels"], 1);
    assert!(slow_row["total_ms"].as_f64().unwrap() > 250.0);
    assert_eq!(
        http_json(bouncer.port, "/api/top", "root")["rows"][0]["text"],
        slow_row["text"]
    );

    let overview = http_json(bouncer.port, "/api/overview", "root");
    assert_eq!(overview["total"]["count"], 34);
    assert_eq!(overview["total"]["not_ok"], 4);
    assert_eq!(
        overview["points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["count"].as_u64().unwrap())
            .sum::<u64>(),
        34
    );

    let one = http_json(
        bouncer.port,
        &format!("/api/query?fp={}", nosuch["fp"].as_str().unwrap()),
        "root",
    );
    assert_eq!(one["text"], "(nosuch ?)");
    assert_eq!(one["total"]["count"], 3);
    assert_eq!(one["failed"][0]["text"], "(nosuch 1)");
    assert_eq!(one["failed"][0]["status"], "name");

    let errors = http_json(bouncer.port, "/api/errors", "root");
    assert_eq!(errors["rows"].as_array().unwrap().len(), 4);
    let count_of = |outcomes: &Value, status: &str| {
        let found = outcomes
            .as_array()
            .unwrap()
            .iter()
            .find(|o| o["status"] == status);
        found.map_or(0, |o| o["count"].as_u64().unwrap())
    };
    assert_eq!(count_of(&errors["outcomes"], "name"), 3);
    assert_eq!(count_of(&errors["outcomes"], "cancel"), 1);
    assert_eq!(count_of(&errors["outcomes"], "ok"), 30);
    assert_eq!(count_of(&overview["outcomes"], "ok"), 30);
    assert!(
        !http_json(bouncer.port, "/api/slow", "root")["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        http_json(bouncer.port, "/api/clients", "root")["rows"][0]["name"],
        "127.0.0.1"
    );
    assert_eq!(
        http_json(bouncer.port, "/api/backends", "root")["rows"][0]["name"],
        replica.addr()
    );

    let (status, metrics) = http_get(bouncer.port, "/metrics", Some("root"));
    assert_eq!(status, 200);
    assert!(
        metrics.contains("rfbouncer_requests_total{pool=\"main\",outcome=\"ok\"} 30"),
        "{metrics}"
    );
    assert!(
        metrics.contains("rfbouncer_backends{pool=\"main\",state=\"idle\"} 1"),
        "{metrics}"
    );

    // What was counted is the store's, not the bouncer's: another bouncer
    // on the same store goes on from it.
    drop(bouncer);
    let again = Bouncer::start(&config(&replica, &store));
    assert_eq!(Client::connect(again.port).query("(+ 5 5)"), int(10));
    let top = http_json(again.port, "/api/top", "root");
    assert_eq!(row(&top, "(+ ? ?)")["count"], 31);

    // The store holds ordinary tables, there for anyone to query.
    let mut direct = Client::connect_as(store.port, support::wire(), Some(("me", "s"))).unwrap();
    assert_eq!(direct.query("(sum (at rfb_agg 'count))"), int(35));
}

#[test]
fn a_store_that_comes_back_empty_is_set_up_again() {
    let replica = Rayforce::start(&[]);
    let mut store = Rayforce::start(&[]);
    let port = store.port;
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\n{}",
        store.addr(),
        pool(&[&replica], "require_restricted = false")
    );
    let bouncer = Bouncer::start(&body);
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    assert_eq!(
        http_json(bouncer.port, "/api/overview", "root")["total"]["count"],
        1
    );

    store.kill();
    // Requests go on while there is nowhere to count them.
    assert_eq!(Client::connect(bouncer.port).query("(+ 2 2)"), int(4));
    assert_eq!(http_get(bouncer.port, "/api/overview", Some("root")).0, 502);

    let _back = Rayforce::start_on(port, &[]);
    assert_eq!(Client::connect(bouncer.port).query("(+ 3 3)"), int(6));
    // The minute was held in memory while the store was away, so nothing of
    // it is missing from the new one.
    support::eventually("the new store has the whole minute", || {
        let (status, body) = http_get(bouncer.port, "/api/overview", Some("root"));
        status == 200 && serde_json::from_str::<Value>(&body).unwrap()["total"]["count"] == 3
    });
    assert_eq!(
        row(&http_json(bouncer.port, "/api/top", "root"), "(+ ? ?)")["count"],
        3
    );
}

#[test]
fn the_pages_want_the_admin_password() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "admin_password = \"root\"\n{}",
        pool(&[&replica], "require_restricted = false")
    ));
    assert_eq!(http_get(bouncer.port, "/", None).0, 401);
    assert_eq!(http_get(bouncer.port, "/api/live", Some("wrong")).0, 401);
    let (status, page) = http_get(bouncer.port, "/", Some("root"));
    assert_eq!(status, 200);
    assert!(page.contains("rfbouncer"));
    assert_eq!(http_get(bouncer.port, "/app.js", Some("root")).0, 200);
    assert_eq!(http_get(bouncer.port, "/nowhere", Some("root")).0, 404);
    // Without a store the reports say so, and the rest still works.
    assert_eq!(http_get(bouncer.port, "/api/top", Some("root")).0, 404);
    assert_eq!(http_json(bouncer.port, "/api/live", "root")["stats"], false);
    // Clients are served on the same port all the while.
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 2)"), int(3));
}

#[test]
fn without_an_admin_password_there_are_no_pages() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start_as_is(&pool(&[&replica], "require_restricted = false"));
    assert_eq!(http_get(bouncer.port, "/", Some("root")).0, 403);
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 2)"), int(3));
}

#[test]
fn the_wait_for_a_replica_has_percentiles() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&["-u", "s"]);
    let bouncer = Bouncer::start(&config(&replica, &store));
    let (mut first, mut second) = (Client::connect(bouncer.port), Client::connect(bouncer.port));
    first.send(&slow(60_000));
    bouncer.wait_busy(1);
    second.send("(+ 1 1)");
    bouncer.wait_queued(1);
    first.cancel();
    first.recv();
    assert_eq!(second.recv(), int(2));

    let overview = http_json(bouncer.port, "/api/overview", "root");
    let of = |name: &str| overview["total"][name].as_f64().unwrap();
    // Of two requests one waited: the 99th percentile is the edge of the
    // bucket that wait fell into, so it is no less than the longest wait.
    assert!(of("wait_max_ms") > 0.0, "{overview}");
    assert!(of("wait_p99_ms") >= of("wait_max_ms"), "{overview}");
    assert!(of("wait_p50_ms") < of("wait_p99_ms"), "{overview}");
    assert!(of("wait_p95_ms") <= of("wait_p99_ms"), "{overview}");
    let point = &overview["points"][0];
    assert_eq!(point["wait_p99_ms"], overview["total"]["wait_p99_ms"]);
}

#[test]
fn a_query_shape_shows_its_clients_and_its_replicas() {
    let (a, b) = (Rayforce::start(&[]), Rayforce::start(&[]));
    let store = Rayforce::start(&["-u", "s"]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\npassword = \"s\"\n{}",
        store.addr(),
        pool(
            &[&a, &b],
            "require_restricted = false\nmode = \"round_robin\""
        )
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for i in 0..4 {
        assert_eq!(c.query(&format!("(+ {i} 1)")), int(i + 1));
    }
    assert_eq!(c.query_error("(nosuch 1)"), Some("name".to_owned()));

    let top = http_json(bouncer.port, "/api/top", "root");
    let fp = |text: &str| row(&top, text)["fp"].as_str().unwrap().to_owned();
    let plus = http_json(
        bouncer.port,
        &format!("/api/query?fp={}", fp("(+ ? ?)")),
        "root",
    );
    let clients = plus["clients"].as_array().unwrap();
    assert_eq!(clients.len(), 1, "{plus}");
    assert_eq!(
        (clients[0]["name"].as_str(), clients[0]["count"].as_u64()),
        (Some("127.0.0.1"), Some(4))
    );
    let replicas = plus["backends"].as_array().unwrap();
    let mut names: Vec<&str> = replicas
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    let (a, b) = (a.addr(), b.addr());
    let mut both = [a.as_str(), b.as_str()];
    both.sort_unstable();
    assert_eq!(names, both, "{plus}");
    assert!(replicas.iter().all(|r| r["count"] == 2), "{plus}");

    // Another shape's requests are its own.
    let nosuch = http_json(
        bouncer.port,
        &format!("/api/query?fp={}", fp("(nosuch ?)")),
        "root",
    );
    assert_eq!(nosuch["clients"][0]["count"], 1);
    assert_eq!(nosuch["clients"][0]["not_ok"], 1);
    assert_eq!(nosuch["backends"].as_array().unwrap().len(), 1);
}

#[test]
fn a_shape_s_replicas_past_the_limit_are_counted_together() {
    let (a, b) = (Rayforce::start(&[]), Rayforce::start(&[]));
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\nmax_fingerprints_per_minute = 1\n{}",
        store.addr(),
        pool(
            &[&a, &b],
            "require_restricted = false\nmode = \"round_robin\""
        )
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for i in 0..6 {
        assert_eq!(c.query(&format!("(+ {i} 1)")), int(i + 1));
    }
    let top = http_json(bouncer.port, "/api/top", "root");
    let one = http_json(
        bouncer.port,
        &format!("/api/query?fp={}", top["rows"][0]["fp"].as_str().unwrap()),
        "root",
    );
    let replicas = one["backends"].as_array().unwrap();
    assert_eq!(replicas.len(), 2, "{one}");
    assert!(replicas.iter().any(|r| r["name"] == "<other>"), "{one}");
    assert!(replicas.iter().all(|r| r["count"] == 3), "{one}");
    assert_eq!(
        Client::connect(store.port).query("(count rfb_fp_backend)"),
        int(2)
    );
}

#[test]
fn symbols_are_values_when_the_setting_says_so() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\nnormalize_symbols = true\n{}",
        store.addr(),
        pool(&[&replica], "require_restricted = false")
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for sym in ["AAPL", "MSFT", "GOOG"] {
        c.query(&format!("(== 'IBM '{sym})"));
    }
    let top = http_json(bouncer.port, "/api/top", "root");
    assert_eq!(top["rows"].as_array().unwrap().len(), 1, "{top}");
    assert_eq!(row(&top, "(== ? ?)")["count"], 3);
}

#[test]
fn totals_come_with_those_of_the_period_before() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\n{}",
        store.addr(),
        pool(&[&replica], "require_restricted = false")
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for i in 0..4 {
        assert_eq!(c.query(&format!("(+ {i} 1)")), int(i + 1));
    }
    let overview = http_json(bouncer.port, "/api/overview", "root");
    assert_eq!(overview["total"]["count"], 4);
    assert_eq!(overview["previous"]["count"], 0);
    let top = http_json(bouncer.port, "/api/top", "root");
    assert_eq!(row(&top, "(+ ? ?)")["previous"]["count"], 0);

    // The same requests an hour and a half ago: before the last hour, and
    // within the hour before it.
    let mut direct = Client::connect(store.port);
    direct.query("(upsert 'rfb_agg 4 (update {from: rfb_agg minute: (- minute 90)}))");
    c.query("(- 1 1)");
    let overview = http_json(bouncer.port, "/api/overview", "root");
    assert_eq!(overview["total"]["count"], 5);
    assert_eq!(overview["previous"]["count"], 4);
    assert_eq!(
        overview["previous"]["p99_ms"],
        row(&top, "(+ ? ?)")["p99_ms"]
    );
    let top = http_json(bouncer.port, "/api/top", "root");
    let plus = row(&top, "(+ ? ?)");
    assert_eq!(plus["previous"]["count"], 4);
    assert_eq!(plus["previous"]["avg_ms"], plus["avg_ms"]);
    assert_eq!(row(&top, "(- ? ?)")["previous"]["count"], 0);
    let one = http_json(
        bouncer.port,
        &format!("/api/query?fp={}", plus["fp"].as_str().unwrap()),
        "root",
    );
    assert_eq!(one["previous"]["count"], 4);
}

#[test]
fn a_store_of_the_layout_before_is_refused_in_so_many_words() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    // `rfb_agg` as it was before the wait had a histogram.
    let mut columns: Vec<String> = [
        "minute",
        "pool",
        "fp",
        "src",
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
    ]
    .iter()
    .map(|c| (*c).to_owned())
    .collect();
    columns.extend((0..32).map(|i| format!("h{i}")));
    let types: Vec<&str> = columns
        .iter()
        .map(|c| match c.as_str() {
            "pool" | "src" => "(as 'SYMBOL [])",
            _ => "(as 'I64 [])",
        })
        .collect();
    Client::connect(store.port).query(&format!(
        "(set rfb_agg (table [{}] (list {})))",
        columns.join(" "),
        types.join(" ")
    ));
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\n{}",
        store.addr(),
        pool(&[&replica], "require_restricted = false")
    );
    let bouncer = Bouncer::start(&body);
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    let (status, body) = http_get(bouncer.port, "/api/overview", Some("root"));
    assert_eq!(status, 502);
    assert!(
        body.contains("rfb_agg is of an older layout (it has no column w0)"),
        "{body}"
    );
    // Nothing was made beside the table it could not use.
    assert!(
        Client::connect(store.port)
            .query_error("(count rfb_fp_client)")
            .is_some()
    );
}
