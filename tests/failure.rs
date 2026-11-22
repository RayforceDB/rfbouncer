//! Replicas that die, hang, lie or speak out of turn.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::Write;
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, MSG_RESPONSE};
use support::fake::{Fake, answer_forever, read_request};
use support::{Bouncer, Client, Rayforce, eventually, int, pool, signal, slow};

const OPEN: &str = "require_restricted = false";

/// Lets every replica of a fresh bouncer finish its greeting, so the first
/// request goes to the first replica listed.
fn settle() {
    std::thread::sleep(Duration::from_millis(300));
}

#[test]
fn a_request_whose_replica_dies_is_run_again_on_another() {
    let mut doomed = Rayforce::start(&[]);
    let other = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&doomed, &other], OPEN));
    settle();
    let mut c = Client::connect(bouncer.port);
    c.send(&slow(8000));
    std::thread::sleep(Duration::from_millis(200));
    doomed.kill();
    assert_eq!(c.recv(), int(8000));
    // The pool goes on with what is left.
    for i in 0..10 {
        assert_eq!(
            Client::connect(bouncer.port).query(&format!("(+ {i} 0)")),
            int(i)
        );
    }
}

#[test]
fn without_retry_the_client_is_told() {
    let mut doomed = Rayforce::start(&[]);
    let other = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&doomed, &other], &format!("{OPEN}\nretry = false")));
    settle();
    let mut c = Client::connect(bouncer.port);
    c.send(&slow(8000));
    std::thread::sleep(Duration::from_millis(200));
    doomed.kill();
    assert_eq!(
        rfbouncer::wire::value::error_code(&c.recv()),
        Some("io".to_owned())
    );
    assert_eq!(c.query("(+ 1 1)"), int(2));
}

#[test]
fn a_replica_that_comes_back_is_used_again() {
    let mut replica = Rayforce::start(&[]);
    let port = replica.port;
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{OPEN}\nquery_wait_timeout = \"500ms\""),
    ));
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    replica.kill();
    assert_eq!(
        Client::connect(bouncer.port).query_error("(+ 1 1)"),
        Some("busy".to_owned())
    );
    let _back = Rayforce::start_on(port, &[]);
    eventually("the replica is back in the pool", || {
        Client::connect(bouncer.port).query("(+ 1 1)") == int(2)
    });
}

#[test]
fn a_frozen_replica_costs_its_request_and_nothing_else() {
    let frozen = Rayforce::start(&[]);
    let other = Rayforce::start(&[]);
    let extra = format!("{OPEN}\nquery_timeout = \"300ms\"\ncancel_grace = \"300ms\"");
    let bouncer = Bouncer::start(&pool(&[&frozen, &other], &extra));
    settle();
    signal(frozen.pid(), "STOP");
    let started = Instant::now();
    let mut c = Client::connect(bouncer.port);
    assert_eq!(c.query_error("(+ 1 1)"), Some("timeout".to_owned()));
    assert!(started.elapsed() < Duration::from_secs(3));
    for i in 0..10 {
        assert_eq!(
            Client::connect(bouncer.port).query(&format!("(+ {i} 0)")),
            int(i)
        );
    }
    signal(frozen.pid(), "CONT");
}

#[test]
fn an_answer_cut_short_closes_the_client() {
    let fake = Fake::start(|mut stream, _| {
        if read_request(&mut stream) {
            let whole = frame::frame(4, MSG_RESPONSE, &[0u8; 1000]);
            let _ = stream.write_all(&whole[..500]);
        }
    });
    let good = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [\"{}\", \"{}\"]\n",
        fake.addr(),
        good.addr()
    ));
    settle();
    let mut c = Client::connect(bouncer.port);
    c.send("(+ 1 1)");
    // Half an answer is on its way to the client, so it cannot be run again.
    assert!(c.try_recv().is_err());
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
}

#[test]
fn a_replica_that_answers_nonsense_is_not_believed() {
    let fake = Fake::start(|mut stream, _| {
        if read_request(&mut stream) {
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\n\r\nhello");
        }
    });
    let good = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [\"{}\", \"{}\"]\n",
        fake.addr(),
        good.addr()
    ));
    settle();
    // Nothing reached the client, so the request runs on the good replica.
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
}

#[test]
fn a_replica_that_speaks_out_of_turn_gets_a_new_connection() {
    let fake = Fake::start(|mut stream, n| {
        if n == 0 {
            // What a subscription left on the connection would push.
            std::thread::sleep(Duration::from_millis(300));
            let _ = stream.write_all(&frame::frame(4, 0, &int(7)));
            std::thread::sleep(Duration::from_secs(5));
        } else {
            answer_forever(stream, 42);
        }
    });
    let bouncer = Bouncer::start(&format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [\"{}\"]\n",
        fake.addr()
    ));
    eventually("the bouncer opened a second connection", || {
        fake.accepted() >= 2
    });
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(42));
}

#[test]
fn a_subscription_made_through_the_bouncer_does_not_reach_another_client() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    Client::connect(bouncer.port).query("(.mc.sub (quote ticks))");
    let mut publisher = Client::connect(replica.port);
    for i in 0..20 {
        publisher.query(&format!("(.mc.pub (quote ticks) {i})"));
        assert_eq!(
            Client::connect(bouncer.port).query(&format!("(+ {i} 100)")),
            int(i + 100)
        );
    }
}

#[test]
fn a_reload_brings_the_pool_to_the_new_list() {
    let a = Rayforce::start(&[]);
    let b = Rayforce::start(&[]);
    Client::connect(a.port).query("(set who 1)");
    Client::connect(b.port).query("(set who 2)");
    let bouncer = Bouncer::start(&pool(&[&a], OPEN));
    assert_eq!(Client::connect(bouncer.port).query("who"), int(1));
    bouncer.reload(&pool(&[&b], OPEN));
    eventually("queries reach only the new replica", || {
        (0..4).all(|_| Client::connect(bouncer.port).query("who") == int(2))
    });
    // A file that does not parse leaves the running configuration alone.
    bouncer.reload("this is not a configuration");
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(Client::connect(bouncer.port).query("who"), int(2));
}

#[test]
fn a_stop_lets_the_request_in_flight_finish() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut c = Client::connect(bouncer.port);
    c.send(&slow(6000));
    std::thread::sleep(Duration::from_millis(200));
    signal(bouncer.pid(), "TERM");
    assert_eq!(c.recv(), int(6000));
    eventually("the bouncer exited", || bouncer.exited().is_some());
    assert!(bouncer.exited().unwrap().success());
}
