//! A pool's result cache: the same request, answered again from memory.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::time::Duration;

use rfbouncer::wire::frame::FLAG_COMPRESSED;
use support::{Bouncer, Client, Rayforce, eventually, http_json, int, pool, slow};

const CACHED: &str = "require_restricted = false\npassword = \"pw\"\ncache_ttl = \"60s\"";
/// Counts how often a replica was asked: its answer is one more each time.
const TICK: &str = "(do (set n (+ n 1)) n)";

fn counting() -> Rayforce {
    let replica = Rayforce::start(&[]);
    assert_eq!(Client::connect(replica.port).query_error("(set n 0)"), None);
    replica
}

fn user(bouncer: &Bouncer) -> Client {
    Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap()
}

fn metric(bouncer: &Bouncer, name: &str) -> f64 {
    bouncer.metric(&format!("rfbouncer_{name}{{pool=\"main\"}}"))
}

fn outcome(bouncer: &Bouncer, outcome: &str) -> f64 {
    bouncer.metric(&format!(
        "rfbouncer_requests_total{{pool=\"main\",outcome=\"{outcome}\"}}"
    ))
}

#[test]
fn the_same_request_is_answered_from_memory() {
    let replica = counting();
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{CACHED}\ncache_max_entry = \"4KiB\""),
    ));
    let mut c = user(&bouncer);
    assert_eq!(c.query(TICK), int(1));
    assert_eq!(c.query(TICK), int(1));
    assert_eq!(user(&bouncer).query(TICK), int(1));
    // Another request, if only by a space.
    assert_eq!(c.query(&format!("{TICK} ")), int(2));
    assert_eq!(metric(&bouncer, "cache_hits_total"), 2.0);
    assert_eq!(metric(&bouncer, "cache_misses_total"), 2.0);
    assert_eq!(metric(&bouncer, "cache_entries"), 2.0);
    assert!(metric(&bouncer, "cache_bytes") > 100.0);
    assert_eq!(
        (outcome(&bouncer, "ok"), outcome(&bouncer, "cached")),
        (2.0, 2.0)
    );
    // An answer from memory took no replica's time.
    assert_eq!(metric(&bouncer, "exec_seconds_count"), 2.0);
    assert_eq!(metric(&bouncer, "wait_seconds_count"), 4.0);

    // An error is asked for again every time, and so is an answer too
    // large to be kept.
    for source in ["(nosuch 1)", "(til 1000)"] {
        let first = c.query(source);
        assert!(c.query(source) == first);
    }
    assert_eq!(metric(&bouncer, "cache_hits_total"), 2.0);
    assert_eq!(metric(&bouncer, "cache_misses_total"), 6.0);
    assert_eq!(metric(&bouncer, "cache_entries"), 2.0);

    // A paused pool holds every request, one it has the answer to as well.
    let mut admin =
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root"))).unwrap();
    admin.query("pause");
    c.send(TICK);
    bouncer.wait_queued(1);
    admin.query("resume");
    assert_eq!(c.recv(), int(1));

    // The console's counts add up: every request is under one outcome.
    let pools = String::from_utf8_lossy(&admin.query("show pools")).into_owned();
    assert!(pools.contains("cached"), "{pools}");

    // Emptied, it asks a replica again.
    assert_eq!(admin.query_error("flush main"), None);
    assert_eq!(metric(&bouncer, "cache_entries"), 0.0);
    assert_eq!(metric(&bouncer, "cache_bytes"), 0.0);
    assert_eq!(c.query(TICK), int(3));
    assert_eq!(admin.query_error("flush nosuch"), Some("admin".to_owned()));
}

#[test]
fn without_a_ttl_nothing_is_kept() {
    let replica = counting();
    let bouncer = Bouncer::start(&pool(&[&replica], "require_restricted = false"));
    let mut c = Client::connect(bouncer.port);
    assert_eq!(c.query(TICK), int(1));
    assert_eq!(c.query(TICK), int(2));
    assert_eq!(metric(&bouncer, "cache_misses_total"), 0.0);
    assert_eq!(metric(&bouncer, "cache_entries"), 0.0);
}

#[test]
fn an_answer_is_not_given_past_its_ttl() {
    let replica = counting();
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        "require_restricted = false\ncache_ttl = \"2s\"",
    ));
    let mut c = Client::connect(bouncer.port);
    assert_eq!(c.query(TICK), int(1));
    assert_eq!(c.query(TICK), int(1));
    eventually("a replica is asked again", || c.query(TICK) == int(2));
}

#[test]
fn an_answer_in_memory_waits_for_no_replica() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], CACHED));
    let mut c = user(&bouncer);
    assert_eq!(c.query("(+ 1 2)"), int(3));
    let mut hog = user(&bouncer);
    hog.send(&slow(60_000));
    bouncer.wait_busy(1);
    assert_eq!(c.query("(+ 1 2)"), int(3));
    assert_eq!(bouncer.live()["pools"][0]["backends"][0]["state"], "busy");
    hog.cancel();
    assert_eq!(hog.query_error_after_cancel(), Some("cancel".to_owned()));
    // A cancelled request left nothing behind to be given to the next.
    assert_eq!(metric(&bouncer, "cache_entries"), 1.0);
}

#[test]
fn the_same_request_twice_at_once_runs_twice_where_two_replicas_are_free() {
    let replicas = [Rayforce::start(&[]), Rayforce::start(&[])];
    let all: Vec<&Rayforce> = replicas.iter().collect();
    let bouncer = Bouncer::start(&pool(&all, CACHED));
    let (mut a, mut b) = (user(&bouncer), user(&bouncer));
    a.send(&slow(20_000));
    b.send(&slow(20_000));
    bouncer.wait_busy(2);
    assert_eq!((a.recv(), b.recv()), (int(20_000), int(20_000)));
    assert_eq!(metric(&bouncer, "cache_misses_total"), 2.0);
    assert_eq!(metric(&bouncer, "cache_hits_total"), 0.0);
}

#[test]
fn a_request_that_waited_behind_its_twin_is_given_the_twins_answer() {
    let replica = counting();
    let bouncer = Bouncer::start(&pool(&[&replica], CACHED));
    let (mut a, mut b) = (user(&bouncer), user(&bouncer));
    let twin = format!("(do {} {TICK})", slow(8000));
    a.send(&twin);
    bouncer.wait_busy(1);
    b.send(&twin);
    bouncer.wait_queued(1);
    assert_eq!((a.recv(), b.recv()), (int(1), int(1)));
    assert_eq!(metric(&bouncer, "cache_hits_total"), 1.0);
    assert_eq!(metric(&bouncer, "cache_misses_total"), 1.0);
}

#[test]
fn a_compressed_answer_is_kept_compressed() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{CACHED}\ncompress = \"always\""),
    ));
    let mut c = user(&bouncer);
    c.send("(til 100000)");
    let first = c.try_recv().unwrap();
    c.send("(til 100000)");
    let second = c.try_recv().unwrap();
    assert_eq!(first.0.flags & FLAG_COMPRESSED, FLAG_COMPRESSED);
    assert!(first == second);
    assert_eq!(metric(&bouncer, "cache_hits_total"), 1.0);
    assert_eq!(metric(&bouncer, "compressed_total"), 1.0);
}

#[test]
fn the_statistics_tell_a_cached_answer_from_one_a_replica_gave() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&["-u", "s"]);
    let bouncer = Bouncer::start(&format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\npassword = \"s\"\n{}",
        store.addr(),
        pool(
            &[&replica],
            "require_restricted = false\ncache_ttl = \"60s\""
        )
    ));
    let mut c = Client::connect(bouncer.port);
    c.stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    for _ in 0..3 {
        assert_eq!(c.query("(+ 1 2)"), int(3));
    }
    let overview = http_json(bouncer.port, "/api/overview", "root");
    let count_of = |status: &str| {
        let outcomes = overview["outcomes"].as_array().unwrap();
        let found = outcomes.iter().find(|o| o["status"] == status);
        found.map_or(0, |o| o["count"].as_u64().unwrap())
    };
    assert_eq!((count_of("ok"), count_of("cached")), (1, 2));
    assert_eq!(overview["total"]["count"], 3);
    assert_eq!(overview["total"]["not_ok"], 0);
    // Neither failed, nor served by a replica.
    let errors = http_json(bouncer.port, "/api/errors", "root");
    assert!(errors["rows"].as_array().unwrap().is_empty(), "{errors}");
    let backends = http_json(bouncer.port, "/api/backends", "root");
    assert_eq!(backends["rows"][0]["count"], 1, "{backends}");
}
