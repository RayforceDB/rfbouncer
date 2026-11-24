//! What makes the bouncer safe to operate: replicas that hang are found
//! while idle, a restart drops nobody, and a stop leaves no replica working
//! for a client that is gone.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, HEADER_LEN, MSG_RESPONSE};
use support::fake::{Fake, answer_forever, read_request};
use support::{Bouncer, Client, Rayforce, eventually, int, pool, signal, slow};

const OPEN: &str = "require_restricted = false";
const OK: &str = "rfbouncer_requests_total{pool=\"main\",outcome=\"ok\"}";
const CHECKS_OK: &str = "rfbouncer_health_checks_total{pool=\"main\",result=\"ok\"}";
const CHECKS_FAILED: &str = "rfbouncer_health_checks_total{pool=\"main\",result=\"failed\"}";

fn over(fake: &Fake, extra: &str) -> String {
    format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [\"{}\"]\n{extra}\n",
        fake.addr()
    )
}

/// The backends of the one pool, as `/api/live` shows them.
fn backends(bouncer: &Bouncer) -> Vec<serde_json::Value> {
    bouncer.live()["pools"][0]["backends"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn a_frozen_replica_is_found_while_idle_and_taken_back_when_it_thaws() {
    // Restricted, as replicas are run: the check must work on those.
    let frozen = Rayforce::start(&["-U", "pw"]);
    let other = Rayforce::start(&["-U", "pw"]);
    let extra = "backend_password = \"pw\"\nhealth_check_interval = \"100ms\"\nhealth_check_timeout = \"300ms\"";
    let bouncer = Bouncer::start(&pool(&[&frozen, &other], extra));
    eventually("both replicas were checked", || {
        bouncer.metric(CHECKS_OK) >= 2.0
    });
    signal(frozen.pid(), "STOP");
    eventually("the frozen replica is down, and says why", || {
        backends(&bouncer).iter().any(|b| {
            b["state"] == "down"
                && b["last_error"]
                    .as_str()
                    .unwrap()
                    .starts_with("health check: no answer within")
        })
    });
    assert!(bouncer.metric(CHECKS_FAILED) >= 1.0);
    // No request finds out the hard way: the default budget of 30 s is
    // what one handed to the frozen replica would have cost.
    let started = Instant::now();
    for i in 0..10 {
        assert_eq!(
            Client::connect(bouncer.port).query(&format!("(+ {i} 0)")),
            int(i)
        );
    }
    assert!(started.elapsed() < Duration::from_secs(5));
    // The checks are no requests: these ten are all that was counted.
    assert_eq!(bouncer.metric(OK), 10.0);
    assert_eq!(
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"error\"}"),
        0.0
    );
    let served: u64 = backends(&bouncer)
        .iter()
        .map(|b| b["served"].as_u64().unwrap())
        .sum();
    assert_eq!(served, 10);
    signal(frozen.pid(), "CONT");
    eventually("the thawed replica is back", || {
        backends(&bouncer).iter().all(|b| b["state"] == "idle")
    });
}

#[test]
fn a_replica_that_stops_answering_gets_a_new_connection() {
    let fake = Fake::start(|mut stream, n| {
        if n == 0 {
            // Takes whatever it is sent and never says a word.
            while read_request(&mut stream) {}
        } else {
            answer_forever(stream, 42);
        }
    });
    let extra = "health_check_interval = \"100ms\"\nhealth_check_timeout = \"200ms\"";
    let bouncer = Bouncer::start(&over(&fake, extra));
    eventually("the bouncer opened a second connection", || {
        fake.accepted() >= 2
    });
    assert_eq!(bouncer.metric(CHECKS_FAILED), 1.0);
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(42));
}

#[test]
fn a_request_that_arrives_during_a_check_waits_no_longer_than_the_check() {
    let (asked, checking) = mpsc::channel();
    let asked = std::sync::Mutex::new(asked);
    let fake = Fake::start(move |mut stream, n| {
        if n == 0 {
            // The first thing to arrive is a check; it is left unanswered.
            if read_request(&mut stream) {
                let _ = asked.lock().unwrap().send(());
            }
            while read_request(&mut stream) {}
        } else {
            answer_forever(stream, 42);
        }
    });
    let extra = "health_check_interval = \"100ms\"\nhealth_check_timeout = \"1s\"";
    let bouncer = Bouncer::start(&over(&fake, extra));
    checking.recv_timeout(Duration::from_secs(10)).unwrap();
    let started = Instant::now();
    // Handed to the connection under check, given back when the check
    // fails, and answered over the connection that replaces it.
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(42));
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(bouncer.metric(OK), 1.0);
}

/// Has a cancel byte arrive among the ordinary bytes of this end of a
/// connection, where reading finds it. Looking for it out of band after the
/// fact does not work everywhere: on Linux a stream read past the byte no
/// longer gives it up.
fn cancels_inline(stream: &std::net::TcpStream) {
    socket2::SockRef::from(stream)
        .set_out_of_band_inline(true)
        .unwrap();
}

/// Reads this end to its close and says whether anything at all arrived.
fn anything_before_the_close(stream: &mut std::net::TcpStream) -> bool {
    let mut any = false;
    while matches!(stream.read(&mut [0u8; 64]), Ok(n) if n > 0) {
        any = true;
    }
    any
}

#[test]
fn a_check_is_never_sent_beside_a_request() {
    // Once with the answer passed on as it comes, once with it gathered
    // whole to be compressed: the connection is as taken in the one as in
    // the other.
    for compress in ["off", "always"] {
        let disturbed = Arc::new(AtomicBool::new(false));
        let seen = disturbed.clone();
        let fake = Fake::start(move |mut stream, _| {
            loop {
                let mut header = [0u8; HEADER_LEN];
                if stream.read_exact(&mut header).is_err() {
                    return;
                }
                let size = u64::from_le_bytes(header[8..].try_into().unwrap()) as usize;
                let mut payload = vec![0u8; size];
                if stream.read_exact(&mut payload).is_err() {
                    return;
                }
                if String::from_utf8_lossy(&payload).contains("RFBOUNCER_PROBE") {
                    let answer = frame::frame(support::wire(), MSG_RESPONSE, &int(0));
                    let _ = stream.write_all(&answer);
                    continue;
                }
                // A request: half of its answer, and then for several
                // intervals nothing more may come.
                let answer = frame::frame(support::wire(), MSG_RESPONSE, &[0u8; 10_000]);
                let _ = stream.write_all(&answer[..5000]);
                stream
                    .set_read_timeout(Some(Duration::from_millis(600)))
                    .unwrap();
                if matches!(stream.read(&mut [0u8; 1]), Ok(n) if n > 0) {
                    seen.store(true, Ordering::SeqCst);
                }
                stream.set_read_timeout(None).unwrap();
                let _ = stream.write_all(&answer[5000..]);
            }
        });
        let extra = format!(
            "health_check_interval = \"50ms\"\nhealth_check_timeout = \"200ms\"\ncompress = \"{compress}\""
        );
        let bouncer = Bouncer::start(&over(&fake, &extra));
        eventually("checks are being made", || bouncer.metric(CHECKS_OK) >= 2.0);
        let answer = Client::connect(bouncer.port).query("(+ 1 1)");
        assert_eq!(answer.len() < 10_000, compress == "always", "{compress}");
        assert!(!disturbed.load(Ordering::SeqCst), "{compress}");
        assert_eq!(bouncer.metric(CHECKS_FAILED), 0.0);
        assert_eq!(fake.accepted(), 1);
        assert_eq!(bouncer.metric(OK), 1.0);
        assert_eq!(backends(&bouncer)[0]["served"], 1);
    }
}

#[test]
fn a_stop_past_its_timeout_sends_no_cancel_to_a_replica_that_is_already_answering() {
    // That replica has done its work; a cancel now would stop whatever it
    // is asked next. With the answer being gathered to be compressed, the
    // client has seen none of it: it is closed, and so is the replica.
    let (done, closed) = mpsc::channel();
    let done = std::sync::Mutex::new(done);
    let fake = Fake::start(move |mut stream, _| {
        cancels_inline(&stream);
        if read_request(&mut stream) {
            let answer = frame::frame(support::wire(), MSG_RESPONSE, &[0u8; 10_000]);
            let _ = stream.write_all(&answer[..5000]);
            let _ = done
                .lock()
                .unwrap()
                .send(anything_before_the_close(&mut stream));
        }
    });
    let extra = "health_check_interval = \"0\"\ncompress = \"always\"";
    let mut bouncer = Bouncer::start(&format!(
        "shutdown_timeout = \"300ms\"\n{}",
        over(&fake, extra)
    ));
    let mut c = Client::connect(bouncer.port);
    c.send("(+ 1 1)");
    bouncer.wait_busy(1);
    signal(bouncer.pid(), "TERM");
    eventually("the bouncer exited", || bouncer.exited().is_some());
    assert!(!closed.recv_timeout(Duration::from_secs(10)).unwrap());
    assert!(c.try_recv().is_err());

    // The same look does see a cancel byte when one was sent.
    let listener = support::listen(support::free_port()).unwrap();
    let sender = support::connect(listener.local_addr().unwrap().port()).unwrap();
    let (mut receiver, _) = listener.accept().unwrap();
    cancels_inline(&receiver);
    socket2::SockRef::from(&sender)
        .send_out_of_band(b"!")
        .unwrap();
    drop(sender);
    assert!(anything_before_the_close(&mut receiver));
}

#[test]
fn a_zero_turns_the_check_off() {
    let got = Arc::new(AtomicUsize::new(0));
    let count = got.clone();
    let fake = Fake::start(move |mut stream, _| {
        while read_request(&mut stream) {
            count.fetch_add(1, Ordering::SeqCst);
        }
    });
    let on = "health_check_interval = \"50ms\"\nhealth_check_timeout = \"10s\"";
    let bouncer = Bouncer::start(&over(&fake, on));
    eventually("a check was sent", || got.load(Ordering::SeqCst) == 1);
    drop(bouncer);
    for off in [
        "health_check_interval = \"0\"\nhealth_check_timeout = \"10s\"",
        "health_check_interval = \"50ms\"\nhealth_check_timeout = \"0\"",
    ] {
        let _bouncer = Bouncer::start(&over(&fake, off));
        // Nothing to wait for but time: eight intervals with no check.
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(got.load(Ordering::SeqCst), 1, "{off}");
    }
}

#[test]
fn a_stop_closes_the_port_at_once() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut c = Client::connect(bouncer.port);
    c.send(&slow(8000));
    bouncer.wait_busy(1);
    signal(bouncer.pid(), "TERM");
    eventually("the port is closed", || {
        support::connect(bouncer.port).is_err()
    });
    // Closed while the request in flight was still running, not at exit.
    assert!(bouncer.exited().is_none());
    assert_eq!(c.recv(), int(8000));
}

#[test]
fn a_client_greeted_before_a_stop_gets_its_one_answer() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    // Greeted, with nothing sent yet: what a client that opens a
    // connection for every request looks like at some instant.
    let mut c = Client::connect(bouncer.port);
    signal(bouncer.pid(), "TERM");
    eventually("the port is closed", || {
        support::connect(bouncer.port).is_err()
    });
    assert_eq!(c.query("(+ 1 1)"), int(2));
    // And then it is let go, and the process with it.
    assert_eq!(c.stream.read(&mut [0u8; 1]).unwrap(), 0);
    eventually("the bouncer exited", || bouncer.exited().is_some());
    assert!(bouncer.exited().unwrap().success());
}

#[test]
fn a_client_that_connected_before_a_stop_is_greeted_and_answered() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    // Connected and nothing more: the bouncer does not yet know it for a
    // client, and has no other reason to stay.
    let stream = support::connect(bouncer.port).unwrap();
    signal(bouncer.pid(), "TERM");
    eventually("the port is closed", || {
        support::connect(bouncer.port).is_err()
    });
    let mut c = Client {
        stream,
        version: support::wire(),
    };
    c.stream.write_all(&[support::wire(), 0]).unwrap();
    let mut hello = [0u8; 2];
    c.stream.read_exact(&mut hello).unwrap();
    assert_eq!(c.query("(+ 2 2)"), int(4));
}

#[test]
fn a_restart_on_a_shared_port_fails_no_request() {
    let a = Rayforce::start(&[]);
    let b = Rayforce::start(&[]);
    let mut old = Bouncer::start(&format!("reuse_port = true\n{}", pool(&[&a, &b], OPEN)));
    let port = old.port;
    let mut persistent = Client::connect(port);
    assert_eq!(persistent.query("(+ 1 1)"), int(2));

    // Clients that open a connection for each request. They pause between
    // two, or a run would use up the machine's ports for outgoing
    // connections, which stay taken for half a minute after each.
    let done = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let clients: Vec<_> = (0..2)
        .map(|_| {
            let (done, stop) = (done.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let n = done.fetch_add(1, Ordering::SeqCst) as i64;
                    assert_eq!(Client::connect(port).query(&format!("(+ {n} 0)")), int(n));
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();
    let more = |n: usize| {
        let target = done.load(Ordering::SeqCst) + n;
        eventually("clients are being served", || {
            done.load(Ordering::SeqCst) >= target || clients.iter().any(|c| c.is_finished())
        });
    };
    more(20);
    let new = old.start_beside();
    more(20);
    // A request in flight on a connection the old process holds. By the
    // time twenty others have come and gone, it has been read.
    persistent.send(&slow(8000));
    more(20);
    signal(old.pid(), "TERM");
    assert_eq!(persistent.recv(), int(8000));
    // Its answer delivered, the old process closes the connection.
    assert_eq!(persistent.stream.read(&mut [0u8; 1]).unwrap(), 0);
    eventually("the old bouncer exited", || old.exited().is_some());
    assert!(old.exited().unwrap().success());
    more(20);
    stop.store(true, Ordering::SeqCst);
    for client in clients {
        client.join().expect("a request failed during the restart");
    }
    // The process left on the port is the new one, and it has been serving.
    assert!(new.metric(OK) > 0.0);
    assert!(new.metric("rfbouncer_uptime_seconds") < 60.0);
}

#[test]
fn a_stop_past_its_timeout_leaves_no_replica_working() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&format!(
        "shutdown_timeout = \"300ms\"\n{}",
        pool(&[&replica], OPEN)
    ));
    let mut c = Client::connect(bouncer.port);
    // About eight seconds of work, were it left to run.
    c.send(&slow(60_000));
    bouncer.wait_busy(1);
    signal(bouncer.pid(), "TERM");
    eventually("the bouncer exited", || bouncer.exited().is_some());
    let started = Instant::now();
    assert_eq!(Client::connect(replica.port).query("(+ 1 1)"), int(2));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the replica went on with a query nobody waits for"
    );
    // The client was told, by the replica's own answer to the cancel.
    assert_eq!(c.query_error_after_cancel(), Some("cancel".to_owned()));
}
