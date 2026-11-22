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
    let mut direct = Client::connect_as(store.port, 4, Some(("me", "s"))).unwrap();
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
