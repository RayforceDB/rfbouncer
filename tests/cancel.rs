//! Cancels, budgets and a full queue: a request that is not wanted any more
//! gives its replica back.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::time::{Duration, Instant};

use support::{Bouncer, Client, Rayforce, int, pool, slow};

const OPEN: &str = "require_restricted = false";

/// About eight seconds of work where the tests were written; every test
/// that sends it ends it early.
fn hog() -> String {
    slow(60_000)
}

#[test]
fn a_cancel_stops_the_running_request_and_frees_the_replica() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut c = Client::connect(bouncer.port);
    c.send(&hog());
    bouncer.wait_busy(1);
    let started = Instant::now();
    c.cancel();
    assert_eq!(c.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(c.query("(+ 1 1)"), int(2));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn a_cancel_takes_a_waiting_request_out_of_the_queue() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut first = Client::connect(bouncer.port);
    first.send(&hog());
    bouncer.wait_busy(1);
    let mut waiting = Client::connect(bouncer.port);
    waiting.send("(+ 1 1)");
    std::thread::sleep(Duration::from_millis(100));
    let started = Instant::now();
    waiting.cancel();
    assert_eq!(
        waiting.query_error_after_cancel(),
        Some("cancel".to_owned())
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the cancel waited for the replica"
    );
    // The connection is still good, and the first client was not disturbed.
    first.cancel();
    assert_eq!(first.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(waiting.query("(+ 2 2)"), int(4));
}

#[test]
fn a_client_that_leaves_gives_its_replica_back() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut gone = Client::connect(bouncer.port);
    gone.send(&hog());
    bouncer.wait_busy(1);
    drop(gone);
    let started = Instant::now();
    let mut next = Client::connect(bouncer.port);
    assert_eq!(next.query("(+ 1 1)"), int(2));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the replica finished a query nobody waited for"
    );
}

#[test]
fn a_request_past_its_budget_is_stopped_and_says_so() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{OPEN}\nquery_timeout = \"300ms\""),
    ));
    let mut c = Client::connect(bouncer.port);
    let started = Instant::now();
    assert_eq!(c.query_error(&hog()), Some("timeout".to_owned()));
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(c.query("(+ 1 1)"), int(2));
}

#[test]
fn a_full_queue_turns_a_request_away() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], &format!("{OPEN}\nqueue_max = 1")));
    let mut running = Client::connect(bouncer.port);
    running.send(&hog());
    bouncer.wait_busy(1);
    let mut queued = Client::connect(bouncer.port);
    queued.send("(+ 1 1)");
    std::thread::sleep(Duration::from_millis(100));
    let mut refused = Client::connect(bouncer.port);
    assert_eq!(refused.query_error("(+ 2 2)"), Some("busy".to_owned()));
    running.cancel();
    assert_eq!(queued.recv(), int(2));
}

#[test]
fn a_request_that_waits_too_long_is_turned_away() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{OPEN}\nquery_wait_timeout = \"300ms\""),
    ));
    let mut running = Client::connect(bouncer.port);
    running.send(&hog());
    bouncer.wait_busy(1);
    let mut waiting = Client::connect(bouncer.port);
    let started = Instant::now();
    assert_eq!(waiting.query_error("(+ 1 1)"), Some("busy".to_owned()));
    let waited = started.elapsed();
    assert!(
        waited > Duration::from_millis(250) && waited < Duration::from_secs(2),
        "{waited:?}"
    );
    running.cancel();
}
