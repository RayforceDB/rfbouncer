//! The replicas' status streams: what the bouncer learns from them, how it
//! chooses by them, and that a replica without one is none the worse.
//! The server side is a stand-in that speaks the stream as agreed.

#![allow(clippy::unwrap_used, clippy::expect_used)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::Duration;

use rfbouncer::wire::frame::{self, MSG_RESPONSE};
use rfbouncer::wire::status::{MSG_STATUS, State};
use serde_json::Value;
use support::fake::{Fake, Report, Tick, read_frame, replica, replica_stream};
use support::{Bouncer, Client, Rayforce, eventually, int, pool, slow};

const NONE: Duration = Duration::ZERO;
/// How often a stand-in writes a frame.
const EVERY: Duration = Duration::from_millis(40);

/// A stand-in that answers every request with `who` and reports `report`.
fn reporting(who: i64, report: Report) -> Fake {
    Fake::start(move |stream, _| {
        replica(stream, who, NONE, EVERY, || Tick::Frame(report.payload()))
    })
}

/// A pool named `main` over stand-ins, with the status streams as they are
/// unless a test says otherwise: on.
fn fakes(replicas: &[&Fake], extra: &str) -> String {
    let extra = if extra.contains("status_interval") {
        extra.to_owned()
    } else {
        format!("{extra}\nstatus_interval = \"1s\"")
    };
    let backends: Vec<String> = replicas
        .iter()
        .map(|r| format!("\"{}\"", r.addr()))
        .collect();
    format!(
        "[[pool]]\nname = \"main\"\nrequire_restricted = false\nbackends = [{}]\n{extra}\n",
        backends.join(", ")
    )
}

const LEAST: &str = "mode = \"least_loaded\"";

fn backends(bouncer: &Bouncer) -> Vec<Value> {
    bouncer.live()["pools"][0]["backends"]
        .as_array()
        .unwrap()
        .clone()
}

/// Waits until every replica's stream is up and has said enough for a rate.
fn wait_for_streams(bouncer: &Bouncer) {
    eventually("every status stream up", || {
        backends(bouncer)
            .iter()
            .all(|b| b["status"]["util"].is_number())
    });
}

/// Which replica answers each of `n` requests sent one after another.
fn answers(bouncer: &Bouncer, n: usize) -> Vec<i64> {
    let mut client = Client::connect(bouncer.port);
    (0..n)
        .map(|_| {
            let payload = client.query("who");
            i64::from_le_bytes(payload[2..10].try_into().unwrap())
        })
        .collect()
}

#[test]
fn a_replica_busy_with_work_that_is_not_ours_is_passed_over() {
    let a = reporting(1, Report::busy(State::Other, "(refresh)"));
    let b = reporting(2, Report::idle(0.5));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], LEAST));
    wait_for_streams(&bouncer);
    assert_eq!(answers(&bouncer, 6), [2; 6]);
}

#[test]
fn with_every_free_replica_busy_elsewhere_a_request_goes_all_the_same() {
    let a = reporting(1, Report::busy(State::Request, "(their query)"));
    let b = reporting(2, Report::busy(State::Other, ""));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], LEAST));
    wait_for_streams(&bouncer);
    // Which of the two is not said: a replica that has just answered us is
    // known not to be held by anything else, whatever its frame says. That
    // every request is answered, and at once, is the point.
    let begun = std::time::Instant::now();
    assert_eq!(answers(&bouncer, 4).len(), 4);
    assert!(begun.elapsed() < Duration::from_secs(5));
}

#[test]
fn the_least_utilised_replica_is_preferred() {
    let a = reporting(1, Report::idle(0.9));
    let b = reporting(2, Report::idle(0.2));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], LEAST));
    wait_for_streams(&bouncer);
    assert_eq!(answers(&bouncer, 6), [2; 6]);
}

#[test]
fn memory_near_its_limit_counts_against_a_replica() {
    let full = Report {
        rss: 95 << 20,
        limit: 100 << 20,
        ..Report::idle(0.1)
    };
    let roomy = Report {
        rss: 10 << 20,
        limit: 100 << 20,
        ..Report::idle(0.3)
    };
    let (a, b) = (reporting(1, full), reporting(2, roomy));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], LEAST));
    wait_for_streams(&bouncer);
    assert_eq!(answers(&bouncer, 6), [2; 6]);
}

#[test]
fn requests_of_one_interval_do_not_all_go_to_the_replica_that_looked_idlest() {
    // Both streams fall silent once the bouncer has a rate from each, as
    // between two frames of a long interval: what follows is chosen on one
    // frame per replica, the first a little less loaded than the second.
    let hold = Arc::new(AtomicBool::new(false));
    let silent_after = |who: i64, util: f64, hold: Arc<AtomicBool>| {
        Fake::start(move |stream, _| {
            replica(stream, who, Duration::from_millis(30), EVERY, || {
                if hold.load(SeqCst) {
                    Tick::Quiet
                } else {
                    Tick::Frame(Report::idle(util).payload())
                }
            })
        })
    };
    let a = silent_after(1, 0.10, hold.clone());
    let b = silent_after(2, 0.11, hold.clone());
    let bouncer = Bouncer::start(&fakes(
        &[&a, &b],
        &format!("{LEAST}\nstatus_interval = \"10s\""),
    ));
    wait_for_streams(&bouncer);
    hold.store(true, SeqCst);
    // Each answer takes 30 ms, 0.3% of the interval: after four of them the
    // first replica has made up its head start.
    let got = answers(&bouncer, 12);
    assert_eq!(got[0], 1, "{got:?}");
    assert!(got.contains(&2), "{got:?}");
}

#[test]
fn a_replica_that_says_nothing_is_ranked_with_the_average_of_those_that_do() {
    // One replica has no stream and the other reports a heavy load: the
    // silent one is taken to be as loaded, and the two go in turn. Neither
    // gets everything, as the silent one would if it counted as idle and the
    // other would if what is known came first.
    let a = Fake::start(|stream, _| replica(stream, 1, NONE, EVERY, || Tick::Close));
    let b = reporting(2, Report::idle(0.9));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], LEAST));
    eventually("the second replica's stream up", || {
        backends(&bouncer)[1]["status"]["util"].is_number()
    });
    let mut got = answers(&bouncer, 6);
    got.sort_unstable();
    assert_eq!(got, [1, 1, 1, 2, 2, 2]);
}

#[test]
fn an_answer_from_the_cache_is_no_load_on_the_replica_chosen_for_it() {
    // Frames stop once each replica has a rate, so the choice is made on
    // one frame per replica and the time the bouncer's own requests ran.
    let hold = Arc::new(AtomicBool::new(false));
    let silent_after = |who: i64, util: f64, hold: Arc<AtomicBool>| {
        Fake::start(move |stream, _| {
            replica(stream, who, Duration::from_millis(300), EVERY, || {
                if hold.load(SeqCst) {
                    Tick::Quiet
                } else {
                    Tick::Frame(Report::idle(util).payload())
                }
            })
        })
    };
    let a = silent_after(1, 0.10, hold.clone());
    let b = silent_after(2, 0.30, hold.clone());
    let settings = format!("{LEAST}\nstatus_interval = \"10s\"\ncache_ttl = \"60s\"");
    let bouncer = Bouncer::start(&fakes(&[&a, &b], &settings));
    wait_for_streams(&bouncer);
    hold.store(true, SeqCst);
    // Three of the same request: one on each replica, and the third waits.
    // When the first is answered the third is given that replica, and is
    // answered from the cache on its way there.
    // shortcut: this guards the path, not the arithmetic. With one
    // connection per replica a start time wrongly noted for the third would
    // be overwritten by the next request there; telling the two apart needs
    // `slots` above 1 and a longer script.
    let mut clients: Vec<Client> = (0..3).map(|_| Client::connect(bouncer.port)).collect();
    for (n, client) in clients.iter_mut().enumerate() {
        client.send("who");
        if n < 2 {
            bouncer.wait_busy(n + 1);
        }
    }
    assert_eq!(clients[0].recv(), int(1));
    assert_eq!(clients[1].recv(), int(2));
    let third = clients[2].recv();
    assert!(third == int(1) || third == int(2));
    let served: Vec<Value> = backends(&bouncer)
        .iter()
        .map(|b| b["served"].clone())
        .collect();
    assert_eq!(served, [1, 1], "the third ran on a replica");
    // The first replica has run one request, 3% of the interval, and stays
    // the less loaded by far: what was not run there is not counted.
    std::thread::sleep(Duration::from_secs(1));
    let mut client = Client::connect(bouncer.port);
    assert_eq!(client.query("x"), int(1));
    assert_eq!(client.query("y"), int(1));
}

#[test]
fn a_replica_the_health_check_found_hung_is_not_chosen_whatever_its_stream_says() {
    // The first replica's stream calls it idle and unloaded throughout. Its
    // other connection stops answering, and nothing new is greeted.
    let hung = Arc::new(AtomicBool::new(false));
    let a = {
        let hung = hung.clone();
        Fake::start(move |mut stream, _| {
            let Some((msgtype, _)) = read_frame(&mut stream) else {
                return;
            };
            if msgtype == MSG_STATUS {
                return replica_stream(stream, EVERY, || Tick::Frame(Report::idle(0.0).payload()));
            }
            while !hung.load(SeqCst) {
                let answer = frame::frame(support::wire(), MSG_RESPONSE, &int(1));
                if std::io::Write::write_all(&mut stream, &answer).is_err()
                    || read_frame(&mut stream).is_none()
                {
                    return;
                }
            }
            while read_frame(&mut stream).is_some() {}
        })
    };
    let b = reporting(2, Report::idle(0.9));
    let settings = format!(
        "{LEAST}\nstatus_interval = \"100ms\"\nhealth_check_interval = \"100ms\"\nhealth_check_timeout = \"200ms\""
    );
    let bouncer = Bouncer::start(&fakes(&[&a, &b], &settings));
    wait_for_streams(&bouncer);
    assert_eq!(answers(&bouncer, 4), [1; 4]);
    // Its status connection is not a request: nothing was served by it or
    // counted for it, and the checks on the idle connection are no load of
    // ours either.
    assert_eq!(backends(&bouncer)[0]["served"], 4);
    assert_eq!(backends(&bouncer)[0]["status"]["not_ours"], false);

    a.hung.store(true, SeqCst);
    hung.store(true, SeqCst);
    eventually("the first replica down", || {
        backends(&bouncer)[0]["state"] == "down"
    });
    assert_eq!(answers(&bouncer, 6), [2; 6]);
    let down = &backends(&bouncer)[0];
    assert_eq!(
        (
            &down["state"],
            &down["status"]["state"],
            &down["status_stream"]
        ),
        (
            &Value::from("down"),
            &Value::from("idle"),
            &Value::from("up")
        )
    );
}

/// Two replicas that answer `who` with 1 and 2.
fn pair() -> [Rayforce; 2] {
    let pair = [Rayforce::start(&[]), Rayforce::start(&[])];
    Client::connect(pair[0].port).query("(set who 1)");
    Client::connect(pair[1].port).query("(set who 2)");
    pair
}

fn count(text: &str, what: &str) -> usize {
    text.matches(what).count()
}

#[test]
fn replicas_without_a_status_stream_are_chosen_as_before() {
    // Real servers, which do not know the status request.
    let [a, b] = pair();
    let settings = format!("require_restricted = false\n{LEAST}\nstatus_interval = \"100ms\"");
    let bouncer = Bouncer::start(&pool(&[&a, &b], &settings));
    eventually("both replicas known to have no stream", || {
        count(&bouncer.log(), "has no status stream") == 2
    });
    // In turn while both are free, and past one that is busy.
    assert_eq!(answers(&bouncer, 4), [1, 2, 1, 2]);
    let mut hog = Client::connect(bouncer.port);
    hog.send(&slow(60_000));
    bouncer.wait_busy(1);
    assert_eq!(answers(&bouncer, 4), [2; 4]);
    hog.cancel();
    for b in backends(&bouncer) {
        assert_eq!(b["status"], Value::Null);
        assert_eq!(b["status_stream"], "none");
    }
    let up = format!(
        "rfbouncer_replica_status_up{{pool=\"main\",replica=\"{}\"}}",
        a.addr()
    );
    assert_eq!(bouncer.metric(&up), 0.0);
    // The replicas themselves took no harm from being asked.
    assert_eq!(Client::connect(a.port).query("who"), int(1));
    assert_eq!(
        count(&bouncer.log(), "status stream"),
        2,
        "{}",
        bouncer.log()
    );
}

/// A stream that says "busy with work that is not ours" and then stops, in
/// the way `after` says: the replica is passed over while it speaks, and
/// taken in turn like any replica without a stream once it does not.
fn a_stream_stops(after: fn() -> Tick) {
    let stop = Arc::new(AtomicBool::new(false));
    // The first replica has no stream at all.
    let a = Fake::start(|stream, _| replica(stream, 1, NONE, EVERY, || Tick::Close));
    let b = {
        let stop = stop.clone();
        Fake::start(move |stream, _| {
            replica(stream, 2, NONE, EVERY, || {
                if stop.load(SeqCst) {
                    after()
                } else {
                    Tick::Frame(Report::busy(State::Other, "(refresh)").payload())
                }
            })
        })
    };
    let settings = format!("{LEAST}\nstatus_interval = \"200ms\"");
    let bouncer = Bouncer::start(&fakes(&[&a, &b], &settings));
    eventually("the second replica's stream up", || {
        backends(&bouncer)[1]["status"]["not_ours"] == true
    });
    assert_eq!(answers(&bouncer, 4), [1; 4]);
    stop.store(true, SeqCst);
    eventually("the second replica's stream gone", || {
        let b = &backends(&bouncer)[1];
        b["status"].is_null() && b["status_stream"] == "lost"
    });
    let got = answers(&bouncer, 4);
    assert!(got.contains(&1) && got.contains(&2), "{got:?}");
    assert_eq!(backends(&bouncer)[1]["state"], "idle");
}

#[test]
fn a_stream_that_goes_quiet_is_not_believed_past_a_few_intervals() {
    a_stream_stops(|| Tick::Quiet);
}

#[test]
fn a_stream_that_dies_leaves_a_replica_that_is_used_as_before() {
    a_stream_stops(|| Tick::Close);
}

#[test]
fn a_replica_without_the_stream_is_asked_once_and_named_once() {
    // Closes on a status request, as a server that does not know it may.
    let asked = Arc::new(AtomicUsize::new(0));
    let replica = {
        let asked = asked.clone();
        Fake::start(move |mut stream, _| {
            while let Some((msgtype, _)) = read_frame(&mut stream) {
                if msgtype == MSG_STATUS {
                    asked.fetch_add(1, SeqCst);
                    return;
                }
                let answer = frame::frame(support::wire(), MSG_RESPONSE, &int(1));
                std::io::Write::write_all(&mut stream, &answer).unwrap();
            }
        })
    };
    let settings = "password = \"pw\"\nstatus_interval = \"100ms\"";
    let bouncer = Bouncer::start(&fakes(&[&replica], settings));
    eventually("the replica asked for its status", || {
        asked.load(SeqCst) == 1
    });
    let mut client =
        Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap();
    for _ in 0..50 {
        assert_eq!(client.query("who"), int(1));
    }
    assert_eq!(asked.load(SeqCst), 1);
    // A replica that reconnected may be a newer one: it is asked again.
    let mut admin =
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root"))).unwrap();
    admin.query("reconnect");
    eventually("asked again after the reconnect", || {
        asked.load(SeqCst) == 2
    });
    assert_eq!(client.query("who"), int(1));
    eventually("the replica up again", || {
        backends(&bouncer)[0]["state"] == "idle"
    });
    assert_eq!(asked.load(SeqCst), 2);
    assert_eq!(
        count(&bouncer.log(), "status stream"),
        1,
        "{}",
        bouncer.log()
    );
}

#[test]
fn a_stream_that_talks_nonsense_is_no_status_and_its_replica_serves() {
    fn nonsense(kind: usize) -> Tick {
        let good = Report::idle(0.1).payload();
        let mut other_layout = good.clone();
        other_layout[0] = 2;
        let mut huge = frame::frame(support::wire(), MSG_STATUS, &good).to_vec();
        huge[8..16].copy_from_slice(&(1u64 << 30).to_le_bytes());
        match kind {
            0 => Tick::Raw(frame::frame(support::wire(), MSG_RESPONSE, &int(7)).to_vec()),
            1 => Tick::Frame(other_layout),
            2 => Tick::Frame(good[..40].to_vec()),
            3 => Tick::Raw(huge),
            _ => Tick::Raw(b"HTTP/1.1 400 Bad Request\r\n\r\n".to_vec()),
        }
    }
    let replicas: Vec<Fake> = (0..5)
        .map(|kind| {
            Fake::start(move |stream, _| {
                replica(stream, kind as i64, NONE, EVERY, || nonsense(kind))
            })
        })
        .collect();
    let all: Vec<&Fake> = replicas.iter().collect();
    let settings = format!("{LEAST}\nstatus_interval = \"100ms\"");
    let bouncer = Bouncer::start(&fakes(&all, &settings));
    eventually("every stream given up", || {
        count(&bouncer.log(), "has no status stream") == 5
    });
    let mut got = answers(&bouncer, 10);
    got.sort_unstable();
    assert_eq!(got, [0, 0, 1, 1, 2, 2, 3, 3, 4, 4]);
    for b in backends(&bouncer) {
        assert_eq!(
            (&b["status"], &b["state"]),
            (&Value::Null, &Value::from("idle"))
        );
    }
}

#[test]
fn what_a_replica_says_is_shown_whatever_the_mode() {
    let theirs = Report {
        rss: 50 << 20,
        limit: 200 << 20,
        ..Report::busy(State::Request, "(select {from: big})")
    };
    let a = reporting(1, theirs);
    let b = reporting(2, Report::idle(0.25));
    let bouncer = Bouncer::start(&fakes(&[&a, &b], "password = \"pw\""));
    wait_for_streams(&bouncer);

    let live = backends(&bouncer);
    let (busy, idle) = (&live[0]["status"], &live[1]["status"]);
    assert_eq!(live[0]["status_stream"], "up");
    assert_eq!(busy["state"], "request");
    assert_eq!(busy["not_ours"], true);
    assert_eq!(busy["text"], "(select {from: big})");
    assert_eq!(busy["running_ms"], 5000);
    assert_eq!(busy["rss_bytes"], 50 << 20);
    assert_eq!(busy["mem_share"], 0.25);
    assert_eq!(busy["cores"], 8);
    assert_eq!(idle["state"], "idle");
    assert_eq!(idle["not_ours"], false);
    assert_eq!(idle["mem_share"], Value::Null);
    let near = |value: &Value, want: f64| (value.as_f64().unwrap() - want).abs() < 0.01;
    assert!(
        near(&idle["util"], 0.25) && near(&idle["cpu"], 0.25),
        "{idle}"
    );
    // The bouncer's own view of the replica is unchanged: it has nothing there.
    assert_eq!(live[0]["state"], "idle");
    assert_eq!(live[0]["running"], Value::Null);

    let of = |metric: &str, r: &Fake| {
        bouncer.metric(&format!(
            "rfbouncer_replica_{metric}{{pool=\"main\",replica=\"{}\"}}",
            r.addr()
        ))
    };
    assert_eq!(of("status_up", &a), 1.0);
    assert_eq!(of("not_ours", &a), 1.0);
    assert_eq!(of("not_ours", &b), 0.0);
    assert_eq!(of("rss_bytes", &a), f64::from(50 << 20));
    assert_eq!(of("mem_limit_bytes", &a), f64::from(200 << 20));
    assert!((of("utilisation", &b) - 0.25).abs() < 0.01);
    assert!((of("cpu_cores", &b) - 0.25).abs() < 0.01);

    let mut admin =
        Client::connect_as(bouncer.port, support::wire(), Some(("rfbouncer", "root"))).unwrap();
    let servers = String::from_utf8_lossy(&admin.query("show servers")).into_owned();
    for expected in [
        "util",
        "cpu",
        "rss",
        "mem_share",
        "not_ours",
        "(select {from: big})",
    ] {
        assert!(servers.contains(expected), "no {expected:?} in {servers:?}");
    }
    // Without a mode that reads the streams, requests go in turn as ever.
    let mut client =
        Client::connect_as(bouncer.port, support::wire(), Some(("app", "pw"))).unwrap();
    assert_eq!(client.query("who"), int(1));
    assert_eq!(client.query("who"), int(2));
}

#[test]
fn a_reload_turns_the_streams_on_and_off() {
    let a = reporting(1, Report::idle(0.1));
    let off = fakes(&[&a], "status_interval = \"0\"");
    let bouncer = Bouncer::start(&off);
    assert_eq!(backends(&bouncer)[0]["status_stream"], "off");
    assert_eq!(answers(&bouncer, 1), [1]);
    bouncer.reload(&format!("admin_password = \"root\"\n{}", fakes(&[&a], "")));
    wait_for_streams(&bouncer);
    bouncer.reload(&format!("admin_password = \"root\"\n{off}"));
    eventually("the stream off again", || {
        backends(&bouncer)[0]["status_stream"] == "off"
    });
    assert_eq!(answers(&bouncer, 1), [1]);
}

#[test]
fn the_page_marks_what_is_not_ours() {
    // The page is not run here; this holds it to the fields the API gives.
    let app = include_str!("../web/app.js");
    for field in [
        "status.util",
        "status.cpu",
        "rss_bytes",
        "mem_share",
        "not_ours",
        "running_ms",
    ] {
        assert!(app.contains(field), "the page does not read {field}");
    }
}
