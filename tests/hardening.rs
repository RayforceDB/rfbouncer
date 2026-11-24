//! What a review found broken or untested: each case here failed, or had
//! nothing to fail it, before it was fixed.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::io::{Read, Write};
use std::time::{Duration, Instant};

use rfbouncer::wire::frame::{self, FLAG_COMPRESSED, Header, MSG_RESPONSE};
use rfbouncer::wire::value::error_code;
use support::fake::{Fake, answer_forever, read_request};
use support::{
    Bouncer, Client, Rayforce, Refused, eventually, http_get, http_json, int, pool, signal, slow,
};

const OPEN: &str = "require_restricted = false";

fn fakes(fakes: &[&Fake], extra: &str) -> String {
    let backends: Vec<String> = fakes.iter().map(|f| format!("\"{}\"", f.addr())).collect();
    format!(
        "[[pool]]\nname = \"main\"\n{OPEN}\nbackends = [{}]\n{extra}\n",
        backends.join(", ")
    )
}

#[test]
fn a_compressed_answer_is_not_taken_for_an_error() {
    // A compressed payload opens with its size. One whose size ends in 0x7f
    // opens with the byte that opens an error.
    let payload: Vec<u8> = [
        0x7f, 0x09, 0, 0, 0x05, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
    ]
    .to_vec();
    let sent = payload.clone();
    let fake = Fake::start(move |mut stream, _| {
        while read_request(&mut stream) {
            let header = Header {
                version: support::wire(),
                flags: FLAG_COMPRESSED,
                msgtype: MSG_RESPONSE,
                size: sent.len(),
            };
            let _ = stream.write_all(&header.encode());
            let _ = stream.write_all(&sent);
        }
    });
    let bouncer = Bouncer::start(&fakes(&[&fake], ""));
    let mut c = Client::connect(bouncer.port);
    c.send("(til 300)");
    let (header, got) = c.try_recv().unwrap();
    assert_eq!((header.flags, got), (FLAG_COMPRESSED, payload));
    eventually("the request is counted", || {
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"ok\"}") == 1.0
    });
    assert_eq!(
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"error\"}"),
        0.0
    );
}

#[test]
fn a_reconnect_asked_for_meanwhile_does_not_deafen_a_request_to_its_cancel() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "admin_password = \"root\"\n{}",
        pool(&[&replica], &format!("{OPEN}\npassword = \"pw\""))
    ));
    let mut c = Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap();
    c.send(&slow(60_000));
    bouncer.wait_busy(1);
    Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root")))
        .unwrap()
        .query("reconnect");
    let started = Instant::now();
    c.cancel();
    assert_eq!(c.query_error_after_cancel(), Some("cancel".to_owned()));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the cancel was not passed on"
    );
    assert_eq!(c.query("(+ 1 1)"), int(2));
}

#[test]
fn a_request_given_back_by_a_dead_replica_waits_afresh_for_another() {
    // The first replica takes a request, holds it past the pool's whole
    // wait budget, and dies. The second is busy at that moment.
    let dying = Fake::start(|mut stream, n| {
        if n == 0 {
            read_request(&mut stream);
            std::thread::sleep(Duration::from_millis(2500));
        } else {
            answer_forever(stream, 42);
        }
    });
    let slow_one = Fake::start(|mut stream, _| {
        while read_request(&mut stream) {
            std::thread::sleep(Duration::from_millis(3500));
            let _ = stream.write_all(&frame::frame(support::wire(), MSG_RESPONSE, &int(42)));
        }
    });
    let bouncer = Bouncer::start(&fakes(&[&dying, &slow_one], "query_wait_timeout = \"2s\""));
    let mut first = Client::connect(bouncer.port);
    first.send("(+ 1 1)");
    bouncer.wait_busy(1);
    let mut second = Client::connect(bouncer.port);
    second.send("(+ 2 2)");
    bouncer.wait_busy(2);
    // Before, it was answered `busy` the moment it came back: it had been
    // in the bouncer longer than a request may wait.
    assert_eq!(first.recv(), int(42));
    assert_eq!(second.recv(), int(42));
    assert_eq!(
        bouncer.metric("rfbouncer_retries_total{pool=\"main\"}"),
        1.0
    );
}

#[test]
fn a_client_that_reads_too_slowly_does_not_keep_the_replica() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&replica],
        &format!("{OPEN}\ntransfer_timeout = \"1s\""),
    ));
    let mut slow_reader = Client::connect(bouncer.port);
    slow_reader.send("(til 4000000)");
    // It asks for 30 MiB and takes none of it.
    let started = Instant::now();
    eventually("the replica is given back", || {
        bouncer.live()["pools"][0]["backends"][0]["state"] == "idle"
    });
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(Client::connect(bouncer.port).query("(+ 1 1)"), int(2));
    // What it does get is cut short, not passed off as a whole answer.
    let mut sink = Vec::new();
    let _ = slow_reader.stream.read_to_end(&mut sink);
    assert!(
        sink.len() < 16 + 32_000_010,
        "got a whole answer: {}",
        sink.len()
    );
    assert_eq!(
        bouncer.metric("rfbouncer_requests_total{pool=\"main\",outcome=\"timeout\"}"),
        1.0
    );
}

#[test]
fn a_replica_that_does_not_take_a_large_request_costs_only_its_budget() {
    let frozen = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(
        &[&frozen],
        &format!("{OPEN}\nquery_timeout = \"1s\"\ncancel_grace = \"500ms\""),
    ));
    signal(frozen.pid(), "STOP");
    let mut c = Client::connect(bouncer.port);
    let started = Instant::now();
    // Far more than the socket buffers hold: the write itself stalls.
    c.send(&format!("(count \"{}\")", "x".repeat(12 << 20)));
    assert_eq!(error_code(&c.recv()), Some("timeout".to_owned()));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    signal(frozen.pid(), "CONT");
    eventually("the replica is back", || {
        Client::connect(bouncer.port).query("(+ 1 1)") == int(2)
    });
}

#[test]
fn looking_at_a_pool_does_not_read_whole_requests() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(60_000));
    bouncer.wait_busy(1);
    let mut waiting: Vec<Client> = (0..6).map(|_| Client::connect(bouncer.port)).collect();
    for c in &mut waiting {
        c.send(&format!("(count \"{}\")", "y".repeat(8 << 20)));
    }
    bouncer.wait_queued(6);
    let started = Instant::now();
    let live = bouncer.live();
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    let text = live["pools"][0]["queue"][0]["text"].as_str().unwrap();
    assert!(
        text.starts_with("(count \"yyy") && text.len() <= 16 * 1024,
        "{}",
        text.len()
    );
    hog.cancel();
}

#[test]
fn a_paused_pool_holds_a_request_for_as_long_as_it_is_paused() {
    let replica = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n{}",
        pool(
            &[&replica],
            &format!("{OPEN}\npassword = \"pw\"\nquery_wait_timeout = \"300ms\"")
        )
    );
    let bouncer = Bouncer::start(&body);
    let mut admin =
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root"))).unwrap();
    admin.query("pause");
    let mut c = Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap();
    c.send("(+ 1 2)");
    c.stream
        .set_read_timeout(Some(Duration::from_millis(1200)))
        .unwrap();
    assert!(c.try_recv().is_err(), "a paused pool answered");
    c.stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    admin.query("resume");
    assert_eq!(c.recv(), int(3));
}

#[test]
fn two_bouncers_on_one_store_count_everything_once() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\n{}",
        store.addr(),
        pool(&[&replica], OPEN)
    );
    let (a, b) = (Bouncer::start(&body), Bouncer::start(&body));
    for i in 0..15 {
        assert_eq!(
            Client::connect(a.port).query(&format!("(+ {i} 1)")),
            int(i + 1)
        );
        assert_eq!(
            Client::connect(b.port).query(&format!("(+ {i} 2)")),
            int(i + 2)
        );
    }
    // Each asks the store after writing its own; the other's last seconds
    // may still be on their way.
    for bouncer in [&a, &b] {
        eventually("both bouncers' requests are counted", || {
            http_json(a.port, "/api/overview", "root");
            http_json(b.port, "/api/overview", "root");
            http_json(bouncer.port, "/api/overview", "root")["total"]["count"] == 30
        });
    }
    assert_eq!(
        http_json(a.port, "/api/top", "root")["rows"][0]["count"],
        30
    );
}

#[test]
fn a_request_sent_as_a_value_is_told_from_another_by_its_bytes() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\n{}",
        store.addr(),
        pool(&[&replica], OPEN)
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for n in [7, 7, 8] {
        c.send_frame(1, &int(n));
        assert_eq!(c.recv(), int(n));
    }
    let top = http_json(bouncer.port, "/api/top?sort=count", "root");
    let rows = top["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{top}");
    assert_eq!(
        (rows[0]["text"].as_str(), rows[0]["count"].as_u64()),
        (Some("<binary 10 bytes>"), Some(2))
    );
}

#[test]
fn connections_are_limited_and_given_back() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&format!(
        "max_client_conn = 2\nclient_idle_timeout = \"400ms\"\n{}",
        pool(&[&replica], OPEN)
    ));
    let mut first = Client::connect(bouncer.port);
    let second = Client::connect(bouncer.port);
    assert_eq!(first.query("(+ 1 1)"), int(2));
    assert!(matches!(
        Client::connect_as(bouncer.port, support::wire(), None),
        Err(Refused::Closed)
    ));
    assert!(bouncer.metric("rfbouncer_connections_refused_total") >= 1.0);
    drop(second);
    eventually("the place is given back", || {
        Client::connect_as(bouncer.port, support::wire(), None).is_ok()
    });
    // A client that sits between two requests longer than it may is let go.
    std::thread::sleep(Duration::from_millis(900));
    first.send("(+ 1 1)");
    assert!(first.try_recv().is_err(), "an idle client was kept");
}

#[test]
fn a_replica_with_two_slots_is_given_two_requests() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], &format!("{OPEN}\nslots = 2")));
    let (mut a, mut b) = (Client::connect(bouncer.port), Client::connect(bouncer.port));
    a.send(&slow(60_000));
    b.send(&slow(60_000));
    bouncer.wait_busy(2);
    assert_eq!(
        bouncer.live()["pools"][0]["backends"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    a.cancel();
    b.cancel();
    assert_eq!(a.query_error_after_cancel(), Some("cancel".to_owned()));
    assert_eq!(b.query_error_after_cancel(), Some("cancel".to_owned()));
}

#[test]
fn affinity_keeps_a_client_on_its_replica() {
    let (a, b) = (Rayforce::start(&[]), Rayforce::start(&[]));
    Client::connect(a.port).query("(set who 1)");
    Client::connect(b.port).query("(set who 2)");
    let bouncer = Bouncer::start(&pool(&[&a, &b], &format!("{OPEN}\naffinity = true")));
    let first = Client::connect(bouncer.port).query("who");
    for _ in 0..8 {
        assert_eq!(Client::connect(bouncer.port).query("who"), first);
    }
}

#[test]
fn shapes_past_the_limit_are_counted_together_and_samples_can_be_left_out() {
    let replica = Rayforce::start(&[]);
    let store = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\n[stats]\nrayforce = \"{}\"\nmax_fingerprints_per_minute = 2\nstore_samples = false\n{}",
        store.addr(),
        pool(&[&replica], OPEN)
    );
    let bouncer = Bouncer::start(&body);
    let mut c = Client::connect(bouncer.port);
    for source in ["(+ 1 1)", "(- 1 1)", "(* 1 1)", "(max 1 1)", "(min 1 1)"] {
        c.query(source);
    }
    let top = http_json(bouncer.port, "/api/top", "root");
    let texts: Vec<&str> = top["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["text"].as_str().unwrap())
        .collect();
    assert_eq!(texts.len(), 3, "{texts:?}");
    assert!(texts.contains(&"<other queries>"), "{texts:?}");
    assert_eq!(top["total"]["count"], 5);
    assert!(
        http_json(bouncer.port, "/api/slow", "root")["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        Client::connect(store.port).query("(count rfb_sample)"),
        int(0)
    );
    // A range no one could mean is answered, not fallen over.
    assert_eq!(
        http_get(
            bouncer.port,
            "/api/slow?to=9223372036854775807&from=-5",
            Some("root")
        )
        .0,
        200
    );
}

#[test]
fn a_pool_of_wire_version_three_speaks_it_to_both_sides() {
    let old = Fake::start_speaking(3, |mut stream, _| {
        while read_request(&mut stream) {
            std::thread::sleep(Duration::from_millis(600));
            let _ = stream.write_all(&frame::frame(3, MSG_RESPONSE, &int(3)));
        }
    });
    let bouncer = Bouncer::start(&fakes(&[&old], "wire_version = 3\nqueue_max = 1"));
    assert!(matches!(
        Client::connect_as(bouncer.port, 4, None),
        Err(Refused::Version(3))
    ));
    let mut running = Client::connect_as(bouncer.port, 3, None).unwrap();
    running.send("(+ 1 2)");
    bouncer.wait_busy(1);
    let mut queued = Client::connect_as(bouncer.port, 3, None).unwrap();
    queued.send("(+ 1 2)");
    bouncer.wait_queued(1);
    // The bouncer's own error, in version 3's layout: a type and a code.
    let mut refused = Client::connect_as(bouncer.port, 3, None).unwrap();
    let busy = refused.query("(+ 1 2)");
    assert_eq!(
        (busy.len(), error_code(&busy)),
        (9, Some("busy".to_owned()))
    );
    assert_eq!(running.recv(), int(3));
    assert_eq!(queued.recv(), int(3));
}

#[test]
fn health_needs_no_password_and_the_report_password_opens_only_the_reports() {
    let mut replica = Rayforce::start(&[]);
    let body = format!(
        "admin_password = \"root\"\nreport_password = \"look\"\n{}",
        pool(&[&replica], &format!("{OPEN}\npassword = \"pw\""))
    );
    let bouncer = Bouncer::start(&body);
    assert_eq!(
        http_get(bouncer.port, "/healthz", None),
        (200, "ok\n".to_owned())
    );
    assert_eq!(http_get(bouncer.port, "/api/live", Some("look")).0, 200);
    assert_eq!(http_get(bouncer.port, "/metrics", Some("look")).0, 200);
    assert!(matches!(
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "look"))),
        Err(Refused::Credentials)
    ));
    // A refused login is counted, over either protocol.
    assert_eq!(http_get(bouncer.port, "/api/live", Some("wrong")).0, 401);
    assert_eq!(bouncer.metric("rfbouncer_auth_failures_total"), 2.0);
    replica.kill();
    eventually("health turns bad", || {
        http_get(bouncer.port, "/healthz", None).0 == 503
    });
}

#[test]
fn every_metric_line_is_one_a_scraper_can_read() {
    let replica = Rayforce::start(&[]);
    let bouncer = Bouncer::start(&pool(&[&replica], OPEN));
    for i in 0..5 {
        Client::connect(bouncer.port).query(&format!("(+ {i} 1)"));
    }
    let (_, body) = http_get(bouncer.port, "/metrics", Some("root"));
    for line in body.lines().filter(|l| !l.starts_with('#')) {
        let (name, value) = line
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("no value: {line}"));
        assert!(value.parse::<f64>().is_ok(), "{line}");
        let bare = name.split('{').next().unwrap();
        assert!(
            bare.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
            "{line}"
        );
        assert_eq!(name.contains('{'), name.ends_with('}'), "{line}");
    }
    assert_eq!(
        bouncer.metric("rfbouncer_exec_seconds_count{pool=\"main\"}"),
        5.0
    );
    assert_eq!(
        bouncer.metric("rfbouncer_exec_seconds_bucket{pool=\"main\",le=\"+Inf\"}"),
        5.0
    );
    assert_eq!(
        bouncer.metric(&format!(
            "rfbouncer_replica_up{{pool=\"main\",replica=\"{}\",slot=\"0\"}}",
            replica.addr()
        )),
        1.0
    );
}

#[test]
fn a_reload_applies_passwords_slots_and_the_removal_of_a_pool() {
    let replica = Rayforce::start(&[]);
    let two = |password: &str, slots: usize, second: bool| {
        let extra = if second {
            format!(
                "[[pool]]\nname = \"other\"\nusers = [\"o\"]\n{OPEN}\nbackends = [\"{}\"]\n",
                replica.addr()
            )
        } else {
            String::new()
        };
        format!(
            "admin_password = \"root\"\n[[pool]]\nname = \"main\"\ndefault = true\npassword = \"{password}\"\nslots = {slots}\n{OPEN}\nbackends = [\"{}\"]\n{extra}",
            replica.addr()
        )
    };
    let bouncer = Bouncer::start(&two("old", 1, true));
    let mut stays = Client::connect_as(bouncer.port, support::wire(), Some(("x", "old"))).unwrap();
    let mut removed = Client::connect_as(bouncer.port, support::wire(), Some(("o", ""))).unwrap();
    assert_eq!(removed.query("(+ 1 1)"), int(2));

    bouncer.reload(&two("new", 2, false));
    eventually("the new password is the one that works", || {
        Client::connect_as(bouncer.port, support::wire(), Some(("x", "new"))).is_ok()
    });
    assert!(matches!(
        Client::connect_as(bouncer.port, support::wire(), Some(("x", "old"))),
        Err(Refused::Credentials)
    ));
    // A connection made under the old password goes on.
    assert_eq!(stays.query("(+ 1 1)"), int(2));
    eventually("the replica has two slots", || {
        bouncer.live()["pools"][0]["backends"]
            .as_array()
            .unwrap()
            .len()
            == 2
    });
    assert_eq!(bouncer.live()["pools"].as_array().unwrap().len(), 1);
    // A client of the pool that is gone is let go at its next request.
    removed.send("(+ 1 1)");
    assert!(removed.try_recv().is_err());
}

#[test]
fn a_replica_that_greets_and_hangs_up_is_not_hammered() {
    let flapping = Fake::start(|stream, _| drop(stream));
    let _bouncer = Bouncer::start_as_is(&fakes(&[&flapping], ""));
    std::thread::sleep(Duration::from_secs(2));
    // 0.1 s, 0.2 s, 0.4 s, 0.8 s apart: a handful, not the thousands a
    // retry with no pause would make.
    assert!(
        flapping.accepted() <= 8,
        "{} connections in two seconds",
        flapping.accepted()
    );
}

#[test]
fn a_stop_past_its_timeout_ends_the_process_anyway() {
    let replica = Rayforce::start(&[]);
    let mut bouncer = Bouncer::start(&format!(
        "shutdown_timeout = \"500ms\"\n{}",
        pool(&[&replica], OPEN)
    ));
    let mut c = Client::connect(bouncer.port);
    c.send(&slow(60_000));
    bouncer.wait_busy(1);
    signal(bouncer.pid(), "TERM");
    eventually("the bouncer exited", || bouncer.exited().is_some());
    // The replica was told to stop on the way out, and said so.
    assert_eq!(c.query_error_after_cancel(), Some("cancel".to_owned()));
}
