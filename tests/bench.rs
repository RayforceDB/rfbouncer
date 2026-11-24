//! The load generator, and what was changed for speed: each change keeps
//! what the bouncer did before it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::Write;
use std::process::Command;
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, MSG_RESPONSE};
use support::fake::{Fake, read_request};
use support::{Bouncer, Client, Rayforce, eventually, int, pool, slow};

const OPEN: &str = "require_restricted = false";

/// Runs rfbench against `port`; whether it succeeded, and what it printed.
fn rfbench(port: u16, args: &[&str]) -> (bool, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_rfbench"))
        .arg(format!("127.0.0.1:{port}"))
        .args(["--wire", &support::wire().to_string()])
        .args(args)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

#[test]
fn the_benchmark_sends_what_it_says_and_counts_what_came_back() {
    let replica = Rayforce::start(&["-U", "pw"]);
    let bouncer = Bouncer::start(&pool(&[&replica], "backend_password = \"pw\""));

    let (ok, out) = rfbench(bouncer.port, &["-c", "4", "-n", "200"]);
    assert!(ok && out.contains("ok 200  errors 0"), "{out}");
    assert!(out.contains("latency_us  p50 "), "{out}");
    assert_eq!(
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"ok\"}"),
        200.0
    );

    // A connection for every request, and the same count.
    let (ok, out) = rfbench(bouncer.port, &["-c", "2", "-n", "20", "--reconnect"]);
    assert!(ok && out.contains("ok 20  errors 0"), "{out}");

    // Straight at the replica, which asks for a password.
    let direct = ["-c", "2", "-n", "20", "--user", "t", "--password", "pw"];
    let (ok, out) = rfbench(replica.port, &direct);
    assert!(ok && out.contains("ok 20  errors 0"), "{out}");
}

#[test]
fn the_benchmark_counts_every_kind_of_error() {
    let replica = Rayforce::start(&["-U", "pw"]);
    let bouncer = Bouncer::start(&pool(&[&replica], "backend_password = \"pw\""));

    // An answer that is an error is counted by its code, and nothing else is.
    let (ok, out) = rfbench(bouncer.port, &["-n", "5", "-q", "(set x 1)"]);
    assert!(!ok && out.contains("ok 0  errors 5"), "{out}");
    assert!(out.contains("error  5  error: "), "{out}");

    // The wrong password is a greeting that failed.
    let (ok, out) = rfbench(replica.port, &["-n", "1", "--password", "no"]);
    assert!(!ok && out.contains("error  1  greeting: "), "{out}");

    // Nobody listening is a connect that failed.
    let (ok, out) = rfbench(support::free_port(), &["-n", "1"]);
    assert!(!ok && out.contains("error  1  connect: "), "{out}");
}

#[test]
fn an_answer_with_more_behind_it_reaches_the_client_and_costs_the_connection() {
    // The answer and a push nobody asked for, in one write: the bouncer
    // reads both at once.
    let fake = Fake::start(|mut stream, _| {
        while read_request(&mut stream) {
            let mut both = frame::frame(support::wire(), MSG_RESPONSE, &int(7)).to_vec();
            both.extend_from_slice(&frame::frame(support::wire(), 0, &int(8)));
            let _ = stream.write_all(&both);
        }
    });
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [\"{}\"]\n",
        fake.addr()
    ));
    let mut c = Client::connect(bouncer.port);
    assert_eq!(c.query("(+ 3 4)"), int(7));
    eventually("the connection is replaced", || fake.accepted() >= 2);
    // The push went to nobody: the next answer is the next request's.
    assert_eq!(c.query("(+ 3 4)"), int(7));
}

#[test]
fn a_client_that_leaves_the_middle_of_the_queue_is_forgotten_while_it_waits() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(600_000));
    bouncer.wait_busy(1);
    let mut ahead = Client::connect(bouncer.port);
    ahead.send("(+ 1 1)");
    bouncer.wait_queued(1);
    let mut leaver = Client::connect(bouncer.port);
    leaver.send("(+ 2 2)");
    bouncer.wait_queued(2);
    drop(leaver);
    // No replica came free and no request reached the head of the queue:
    // it is the pool's own look over the queue that finds it.
    eventually("the request of the client that left is gone", || {
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"cancel\"}") == 1.0
    });
    assert_eq!(
        bouncer.live()["pools"][0]["queue"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    hog.cancel();
    assert_eq!(hog.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(ahead.recv(), int(2));
}

#[test]
fn a_client_refused_at_a_full_queue_cannot_ask_again_faster_than_the_delay() {
    let replica = Rayforce::start(&[]);
    let extra = format!("{OPEN}\nqueue_max = 1\nbusy_delay = \"300ms\"");
    let bouncer = Bouncer::start(&pool(&[&replica], &extra));
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(600_000));
    bouncer.wait_busy(1);
    let mut queued = Client::connect(bouncer.port);
    queued.send("(+ 1 1)");
    bouncer.wait_queued(1);
    // As fast as it can for 1.2 s: told at once it is refused thousands of
    // times, and that is what kept the replicas' tasks from their turn.
    let mut eager = Client::connect(bouncer.port);
    let (started, mut refusals) = (Instant::now(), 0);
    while started.elapsed() < Duration::from_millis(1200) {
        assert_eq!(eager.query_error("(+ 2 2)"), Some("busy".to_owned()));
        refusals += 1;
    }
    assert!((1..=4).contains(&refusals), "refused {refusals} times");
    hog.cancel();
    assert_eq!(hog.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(queued.recv(), int(2));
}
